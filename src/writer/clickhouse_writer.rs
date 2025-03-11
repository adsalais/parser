use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use crate::{
    Error,
    output::{OutputWriter, Tuple},
};
use chrono::{DateTime, Utc};
use clickhouse::{Client, inserter::Inserter};
use clickhouse_derive::Row;
use log::error;
use serde::{Deserialize, Serialize};

use tokio::{
    runtime::Runtime,
    sync::{mpsc, oneshot},
};

pub enum Message {
    Flush(oneshot::Sender<Result<(), Error>>),
    Row(Row),
}

pub struct ClickhouseWriter {
    _runtime: Runtime,
    sender: mpsc::Sender<Message>,
    has_error: Arc<AtomicBool>,
    table: String,
}
impl ClickhouseWriter {
    pub fn new(
        server: &str,
        login: &Option<String>,
        password: &Option<String>,
        database: &str,
        table: &str,
    ) -> Result<Self, Error> {
        let runtime = Runtime::new()?;
        let max_rows: u64 = 10_000;
        let (sender, mut receiver) = mpsc::channel::<Message>(max_rows as usize);
        let default_login = "default".to_owned();
        let login = login.as_ref().unwrap_or(&default_login);
        let default_password = "".to_owned();
        let password = password.as_ref().unwrap_or(&default_password);

        let client = Client::default()
            .with_url(format!("http://{}", server))
            .with_user(login)
            .with_password(password)
            .with_database(database)
            .with_option("enable_json_type", "1")
            .with_option("input_format_binary_read_json_as_string", "1")
            .with_option("output_format_binary_write_json_as_string", "1")
            .with_option("allow_experimental_json_type", "1")
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0");

        let mut inserter: Inserter<Row> = client
            .inserter::<Row>(&table)?
            .with_max_rows(max_rows)
            .with_max_bytes(1024 * 1024 * 10);

        let has_error = Arc::new(AtomicBool::new(false));

        let report_error = has_error.clone();
        runtime.spawn(async move {
            let mut flush = None;
            while let Some(msg) = receiver.recv().await {
                match msg {
                    Message::Flush(reply) => {
                        flush = Some(reply);
                        break;
                    }
                    Message::Row(row) => {
                        if let Err(e) = Self::write(row, &mut inserter).await {
                            error!("Clickhouse error: {e}");
                            report_error.store(true, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }
            let end = inserter.end().await;
            if let Some(flush) = flush {
                if let Err(e) = end {
                    let _ = flush.send(Err(e.into()));
                } else {
                    let _ = flush.send(Ok(()));
                }
            } else {
                if let Err(e) = end {
                    error!("{e}")
                }
            }
        });

        Ok(Self {
            _runtime: runtime,
            sender,
            has_error,
            table: table.to_owned(),
        })
    }

    async fn write(row: Row, inserter: &mut Inserter<Row>) -> Result<(), Error> {
        inserter.write(&row)?;
        inserter.commit().await?;
        Ok(())
    }
}

impl OutputWriter for ClickhouseWriter {
    fn write(&mut self, data: Tuple) -> Result<(), Error> {
        if !self.has_error.load(Ordering::Relaxed) {
            let row = Row::from_tuple(data)?;
            self.sender
                .blocking_send(Message::Row(row))
                .map_err(|e| Error::Generic(e.to_string()))?;
        } else {
            return Err(Error::Generic(format!(
                "An error occured while inserting data in table: {}",
                self.table
            )));
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), crate::Error> {
        let (tx, rx) = oneshot::channel::<Result<(), Error>>();
        self.sender.blocking_send(Message::Flush(tx)).map_err(|e| {
            Error::Generic(format!(
                "Error while flushing data from table: '{}', {e}",
                self.table
            ))
        })?;
        rx.blocking_recv()?
    }
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct Row {
    id: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    import_date: DateTime<Utc>,
    computer: String,
    original_file: String,
    archive_name: String,
    data: String,
}
impl Row {
    pub fn from_tuple(tuple: Tuple) -> Result<Self, Error> {
        let data = if let Some(v) = tuple.data {
            serde_json::to_string(&v)?
        } else {
            "{}".to_owned()
        };
        let row = Self {
            id: tuple.id,
            import_date: tuple.import_date,
            computer: tuple.computer,
            original_file: tuple.original_file,
            archive_name: tuple.archive_name,
            data,
        };
        Ok(row)
    }
}
#[cfg(test)]
mod tests {

    use clickhouse::sql::Identifier;
    use serde_json::Value;
    use serde_json::json;

    use super::*;
    use crate::init_log;
    use crate::output::*;

    #[test]
    fn parser() {
        init_log();
        let database = "test_clickhouse_writer_parser";
        let table = "test";
        let server = "localhost:8123";

        let client = Client::default()
            .with_url(format!("http://{}", server))
            .with_option("enable_json_type", "1");

        let database_query = format!("CREATE DATABASE IF NOT EXISTS {database} ;");
        let runtime = Runtime::new().unwrap();
        runtime
            .block_on(client.query(&database_query).execute())
            .unwrap();

        let table_query = format!(
            "CREATE TABLE IF NOT EXISTS {database}.{table} 
                (
                    {FIELD_ID} String,
                    {FIELD_IMPORT_DATE} DateTime64(3,'UTC') CODEC(Delta, ZSTD),
                    {FIELD_COMPUTER} LowCardinality(String),
                    {FIELD_ORIGINAL} LowCardinality(String),
                    {FIELD_ARCHIVE} LowCardinality(String),
                    {FIELD_DATA} json,
                ) 
                ENGINE = ReplacingMergeTree({FIELD_IMPORT_DATE})
                ORDER BY ({FIELD_COMPUTER},{FIELD_ID})
                ;",
        );

        runtime
            .block_on(client.query(&table_query).execute())
            .unwrap();

        let fields = Fields::new(
            "machine_id",
            "original_file",
            "archive_name",
            "archive_file",
        );

        let mut tuple = Tuple::new(&fields);

        let mut writer = ClickhouseWriter::new(server, &None, &None, database, table).unwrap();
        let mut data = serde_json::Map::new();
        data.insert("i".to_string(), json!(0));
        data.insert("rd".to_string(), json!("Some String"));

        tuple.set_data(Value::Object(data), None).unwrap();

        writer.write(tuple).unwrap();
        writer.flush().unwrap();

        std::thread::sleep(std::time::Duration::from_millis(100));

        let client = Client::default()
            .with_url(format!("http://{}", server))
            .with_option("enable_json_type", "1")
            .with_option("allow_experimental_json_type", "1")
            // Enable inserting JSON columns as a string
            .with_option("input_format_binary_read_json_as_string", "1")
            // Enable selecting JSON columns as a string
            .with_option("output_format_binary_write_json_as_string", "1")
            .with_database(database);
        let db_row = runtime
            .block_on(
                client
                    .query("SELECT ?fields FROM ? LIMIT 1")
                    .bind(Identifier(table))
                    .fetch_one::<Row>(),
            )
            .unwrap();

        // You can then use any JSON library to parse the JSON string, e.g., serde_json.
        let json_value: serde_json::Value =
            serde_json::from_str(&db_row.data).expect("Invalid JSON");

        assert_eq!("Some String", json_value["rd"]);
    }
}
