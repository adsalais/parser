use clickhouse::Client;
use log::info;

use crate::{
    Error,
    configuration::{DataTopic, DataType},
    output::*,
};

///
/// Create database and required tables for every topics..
/// re-entrant: can be run several times without any risk of data deletion
///
pub async fn create_database(
    client: &Client,
    db: &str,
    topics: &Vec<DataTopic>,
    cluster: &Option<String>,
    enable_kafka: bool,
    kafka_brokers: &str,
    consumers: usize,
) -> Result<(), Error> {
    let database_query = &database_query(db, cluster);
    client.query(database_query).execute().await?;

    //create timeline table
    let timeline_short = timeline_short_query(db, cluster);
    client.query(&timeline_short).execute().await?;

    for topic in topics {
        info!("creating tables for '{}'", topic.table_name);
        //create main table
        if topic
            .partial_field_def
            .iter()
            .find_map(|field| {
                if field.0.eq(&topic.sort_field) {
                    Some(field)
                } else {
                    None
                }
            })
            .is_none()
        {
            return Err(Error::ClickhouseSortField(
                topic.table_name.to_owned(),
                topic.sort_field.to_owned(),
            ));
        }

        let table_query = table_query(
            db,
            &topic.table_name,
            &topic.partial_field_def,
            &topic.sort_field,
            cluster,
        );
        client.query(&table_query).execute().await?;

        if enable_kafka {
            //create kafka table
            let kafka_table_query = kafka_table_query(
                db,
                &topic.table_name,
                kafka_brokers,
                &topic.topic_name,
                consumers,
                cluster,
            );
            client.query(&kafka_table_query).execute().await?;

            //create materialized view
            let materialized_view = materialized_view_query(db, &topic.table_name, cluster);
            client.query(&materialized_view).execute().await?;
        }

        for (field, data_type) in &topic.partial_field_def {
            if let DataType::Date = data_type {
                //create materialized views that feeds the timeline table
                let timeline_short_mat =
                    timeline_short_materialized_view_query(db, &topic.table_name, field, cluster);
                client.query(&timeline_short_mat).execute().await?;
            }
        }
    }

    //create timeline views
    let timeline_data_view = timeline_data_view_query(db, topics, cluster);
    client.query(&timeline_data_view).execute().await?;

    let timeline_view = timeline_view_query(db, cluster);
    client.query(&timeline_view).execute().await?;

    Ok(())
}

/// The database creation query
fn database_query(db: &str, cluster: &Option<String>) -> String {
    let cluster = cluster_def(cluster);
    format!("CREATE DATABASE IF NOT EXISTS {db} {cluster};")
}

/// The kafka table creation query
fn kafka_table_query(
    db: &str,
    table_name: &str,
    broker: &str,
    topic: &str,
    consumers: usize,
    cluster: &Option<String>,
) -> String {
    let cluster = cluster_def(cluster);
    format!(
        "CREATE TABLE IF NOT EXISTS {db}._{table_name}_kafka {cluster} 
        (
            {FIELD_ID} String,
            {FIELD_IMPORT_DATE} DateTime64(3,'UTC'),
            {FIELD_COMPUTER} String,
            {FIELD_ORIGINAL} String,
            {FIELD_ARCHIVE} String,
            {FIELD_DATA} String,

        ) 
        ENGINE = Kafka()
        SETTINGS
            kafka_broker_list = '{broker}',
            kafka_topic_list = '{topic}',
            kafka_group_name = 'clickhouse',
            kafka_format = 'JSONEachRow',
            kafka_num_consumers = {consumers}
        ;"
    )
}

/// The final table creation
fn table_query(
    db: &str,
    table_name: &str,
    partial_field_def: &Vec<(String, DataType)>,
    sort_field: &str,
    cluster: &Option<String>,
) -> String {
    let cluster = cluster_def(cluster);
    let json_type = build_json_type(partial_field_def);
    let sort_field = if sort_field.is_empty() {
        sort_field.to_owned()
    } else {
        format!(", data.{sort_field}")
    };

    format!(
        "CREATE TABLE IF NOT EXISTS {db}.{table_name} {cluster} 
        (
            {FIELD_ID} String,
            {FIELD_IMPORT_DATE} DateTime64(3,'UTC') CODEC(Delta, ZSTD),
            {FIELD_COMPUTER} LowCardinality(String),
            {FIELD_ORIGINAL} LowCardinality(String),
            {FIELD_ARCHIVE} LowCardinality(String),
            {FIELD_DATA} {json_type},
        ) 
        ENGINE = ReplacingMergeTree({FIELD_IMPORT_DATE})
        ORDER BY ({FIELD_COMPUTER},{FIELD_ID}{sort_field})
        ;",
    )
}

fn build_json_type(partial_field_def: &Vec<(String, DataType)>) -> String {
    let mut json = "JSON".to_owned();
    let mut first = true;
    if !partial_field_def.is_empty() {
        json.push('(');
        for (name, ftype) in partial_field_def {
            if first {
                json.push_str("\n\t");
            } else {
                json.push_str(",\n\t");
            }
            first = false;
            json.push_str(name);
            json.push_str(" ");

            match ftype {
                DataType::Boolean => json.push_str("Bool"),
                DataType::String => json.push_str("String"),
                DataType::Date => json.push_str("DateTime64(3,'UTC')"),
                DataType::Float => json.push_str("Float64"),
                DataType::Int32 => json.push_str("Int32"),
                DataType::Int64 => json.push_str("Int64"),
                DataType::Uint8 => json.push_str("UInt8"),
                DataType::Uint16 => json.push_str("UInt16"),
            };
        }

        json.push_str("\n\t)");
    }
    json
}

///
///  The materialized view creation query that feed the final table with kafka data
///
fn materialized_view_query(db: &str, table_name: &str, cluster: &Option<String>) -> String {
    let cluster = cluster_def(cluster);

    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}._{table_name}_consumer TO {db}.{table_name} {cluster} 
        AS 
        SELECT
            {FIELD_ID},
            {FIELD_IMPORT_DATE},
            {FIELD_COMPUTER},
            {FIELD_ORIGINAL},
            {FIELD_ARCHIVE},
            {FIELD_DATA}
        FROM {db}._{table_name}_kafka
        ;"
    )
}

///
/// The cluster definition
/// if run on a cluster, the create_database function and the drop_kafka_tables will be run on every machines in the cluster
///
fn cluster_def(cluster: &Option<String>) -> String {
    match cluster {
        Some(cluster) => format!("ON CLUSTER {cluster}"),
        None => String::new(),
    }
}

///
/// The timeline table that will be feeded by a materialized view
/// It contains a reference to every entry of every tables that have at least one date field
/// Tables with several date fields will have one entry per date
///
fn timeline_short_query(db: &str, cluster: &Option<String>) -> String {
    let cluster = cluster_def(cluster);
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.timeline_short {cluster} 
        (
            {FIELD_ID} String,
            {FIELD_COMPUTER} LowCardinality(String),
            {FIELD_ORIGINAL} LowCardinality(String),
            {FIELD_ARCHIVE} LowCardinality(String),
            event_date DateTime64(3,'UTC'),
            source LowCardinality(String)
        ) 
        ENGINE = ReplacingMergeTree()
        ORDER BY ({FIELD_COMPUTER},{FIELD_ID},event_date)
        PARTITION BY toYYYYMM(event_date)
        ;"
    )
}

/// Materialized view that feeds the timeline_short table
fn timeline_short_materialized_view_query(
    db: &str,
    table_name: &str,
    date_field: &str,
    cluster: &Option<String>,
) -> String {
    let cluster = cluster_def(cluster);
    let field_name = date_field.replace(".", "_");
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}._{table_name}_tml_mat_{field_name} TO {db}.timeline_short {cluster} AS SELECT 
                    {FIELD_ID},
                    {FIELD_COMPUTER},
                    {FIELD_ORIGINAL},
                    {FIELD_ARCHIVE},
                    data.{date_field} as event_date,
                    '{table_name}' as source
                FROM {db}.{table_name};"
    )
}

//
// View that is used to retrieve the data field from the timeline
//
fn timeline_data_view_query(db: &str, topics: &Vec<DataTopic>, cluster: &Option<String>) -> String {
    let mut query = String::new();
    for (topic_pos, data_topic) in topics.iter().enumerate() {
        let partial_field_def = &data_topic.partial_field_def;
        let table_name = &data_topic.table_name;
        for (_, data_type) in partial_field_def {
            if let DataType::Date = data_type {
                if topic_pos != 0 {
                    query.push_str("\nUNION ALL");
                    query.push(' ');
                }

                let subquery = format!(
                    r"SELECT 
                        {FIELD_ID},
                        {FIELD_COMPUTER},
                        {FIELD_ORIGINAL},
                        '{table_name}' as source,
                        toJSONString({FIELD_DATA}) as data
                    FROM {db}.{table_name}"
                );
                query.push_str(&subquery);

                //we need only one select per table that have a date field
                break;
            }
        }
    }
    let cluster = cluster_def(cluster);
    format!("CREATE OR REPLACE VIEW {db}._timeline_data {cluster} AS \n {query}")
}

fn timeline_view_query(db: &str, cluster: &Option<String>) -> String {
    let cluster = cluster_def(cluster);
    format!(
        "CREATE OR REPLACE VIEW {db}.timeline {cluster} AS 
        SELECT 
            t.event_date,
            t.source,
            t.{FIELD_COMPUTER},
            t.{FIELD_ORIGINAL},
            t.{FIELD_ARCHIVE},
            t.{FIELD_ID},
            d.data
        FROM 
            {db}.timeline_short t INNER JOIN {db}._timeline_data d
        ON 
            t.{FIELD_COMPUTER} = d.{FIELD_COMPUTER} AND
            t.{FIELD_ORIGINAL} = d.{FIELD_ORIGINAL} AND
            t.{FIELD_ID} = d.{FIELD_ID} AND
            t.source = d.source
        ;"
    )
}

#[cfg(test)]
mod tests {

    use crate::configuration::DataTopic;

    use super::*;

    #[tokio::test]
    async fn create_tables() {
        //let cluster = Some("test_cluster".to_string());
        let cluster = None;

        let db = "test_database";
        let db_query = database_query(db, &cluster);
        println!("{db_query}");

        let table_name = "test_table";
        let topic = format!("{db}_{table_name}");

        let fields = vec![
            ("TestString".to_owned(), DataType::String),
            ("TestInteger".to_owned(), DataType::Int64),
            ("TestDate".to_owned(), DataType::Date),
            ("TestDate2".to_owned(), DataType::Date),
            ("TestDate3".to_owned(), DataType::Date),
            ("TestFloat".to_owned(), DataType::Float),
            ("TestBool".to_owned(), DataType::Boolean),
        ];
        let table = table_query(db, table_name, &fields, "TestDate", &cluster);
        println!("{table}");

        let timeline_short = timeline_short_query(db, &cluster);
        println!("{timeline_short}");

        for (field, data_type) in &fields {
            if let DataType::Date = data_type {
                let time_line_mat =
                    timeline_short_materialized_view_query(db, table_name, field, &cluster);
                println!("{time_line_mat}");
            }
        }

        let data_topic = DataTopic::new(
            topic.clone(),
            table_name.to_owned(),
            fields,
            "TestDate".to_owned(),
        );

        let timeline_data = timeline_data_view_query(db, &vec![data_topic], &cluster);
        println!("{timeline_data}");

        let timeline = timeline_view_query(db, &cluster);
        println!("{timeline}");
    }

    #[tokio::test]
    async fn test_connection() {
        let client = Client::default()
            .with_user("default")
            .with_url("http://localhost:8123")
            .with_option("enable_json_type", "1");

        let mut res = client.query("SHOW DATABASES").fetch::<String>().unwrap();

        while let Some(e) = res.next().await.unwrap() {
            println!("{e}")
        }
    }

    #[tokio::test]
    async fn invalid_sort_date() {
        let client = Client::default()
            .with_user("default")
            .with_url("http://localhost:8123")
            .with_option("enable_json_type", "1");

        let db_name = "test_clickhouse_config_invalid";
        let table_name = "test_table";
        let topic_name = format!("{db_name}_{table_name}");
        let fields = vec![("TestString".to_owned(), DataType::String)];
        let data_topic = DataTopic::new(
            topic_name.clone(),
            table_name.to_owned(),
            fields,
            "Invalid_sort_field".to_owned(),
        );

        //
        //  Create all tables
        //
        create_database(
            &client,
            db_name,
            &vec![data_topic.clone()],
            &None,
            false,
            "",
            0,
        )
        .await
        .expect_err("");
    }

    #[tokio::test]
    async fn create_database_test() {
        let client = Client::default()
            .with_user("default")
            .with_url("http://localhost:8123")
            .with_option("enable_json_type", "1");

        let db_name = "test_clickhouse_config_create";
        let table_name = "test_table";
        let topic_name = format!("{db_name}_{table_name}");
        let fields = vec![
            ("TestString".to_owned(), DataType::String),
            ("TestInteger".to_owned(), DataType::Int64),
            ("TestDate".to_owned(), DataType::Date),
            ("TestDateAgain".to_owned(), DataType::Date),
            ("TestDateOneMore".to_owned(), DataType::Date),
            ("TestFloat".to_owned(), DataType::Float),
            ("TestBool".to_owned(), DataType::Boolean),
        ];
        let data_topic = DataTopic::new(
            topic_name.clone(),
            table_name.to_owned(),
            fields,
            "TestDate".to_owned(),
        );

        //
        //  Create all tables
        //
        create_database(
            &client,
            db_name,
            &vec![data_topic.clone()],
            &None,
            false,
            "",
            0,
        )
        .await
        .unwrap();

        let mut cursor = client
            .query(&format!("SHOW TABLES FROM {db_name}"))
            .fetch::<String>()
            .unwrap();
        let mut res = vec![];
        while let Some(e) = cursor.next().await.unwrap() {
            res.push(e);
        }

        let expected = format!("{table_name}");
        assert!(res.iter().find(|e| e.eq(&&expected)).is_some());
    }
}
