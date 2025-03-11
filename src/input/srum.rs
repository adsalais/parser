use crate::{
    Error,
    output::{Fields, OUTPUT_DATE_FORMAT_UTC, Output, OutputConfig, Tuple},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as enc64};
use chrono::{DateTime, Utc};
use libesedb::{EseDb, systemtime_from_filetime, systemtime_from_oletime};
use log::warn;
use serde_json::json;
use std::{collections::HashMap, path::Path};

use super::srum_model::{ID_MAP_TABLE, SRUM_SORT_FIELD, srum_tables};

///
/// Parse a Srum file.
/// SRUM is used to monitor desktop application programs, services, Windows applications and network connections.
/// https://github.com/libyal/esedb-kb/blob/main/documentation/System%20Resource%20Usage%20Monitor%20(SRUM).asciidoc
///
pub struct SrumParser {
    db: EseDb,
    index: HashMap<i32, String>,
}
impl SrumParser {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let db = EseDb::open(path)?;
        let index = SrumParser::init_index(&db)?;

        Ok(SrumParser { db, index })
    }

    ///
    /// Srum tables stores some data in an index table
    /// The index is fully read and stored in memory
    ///
    fn init_index(db: &EseDb) -> Result<HashMap<i32, String>, Error> {
        let table = db.table_by_name(ID_MAP_TABLE)?;

        let num_records = table.count_records()? as usize;
        let mut index: HashMap<i32, String> = HashMap::with_capacity(num_records);

        for rec in table.iter_records()? {
            let rec = rec?;

            let id_type = match rec.value(0)?.to_u32() {
                Some(v) => v,
                None => continue,
            };

            let id_index = match rec.value(1)?.to_i32() {
                Some(v) => v,
                None => continue,
            };

            let blob = rec.value(2)?;
            let id_blob = match blob.as_bytes() {
                Some(v) => v,
                None => continue,
            };

            let value = if id_type == 3 {
                convert_sid(id_blob)
            } else {
                String::from_utf8_lossy(id_blob).replace("\0", "")
            };

            index.insert(id_index, value);
        }
        Ok(index)
    }

    ///
    /// Parse all SRUM tables
    /// it does it in a best effort mode: upon failure, it does not stop and only log the error.
    ///
    pub fn parse_all_tables(
        &self,
        client_context: &str,
        fields: &Fields,
        output_config: &[OutputConfig],
    ) -> Result<usize, Error> {
        let mut num_rows = 0;

        for srum_table in srum_tables() {
            let mut output = Output::new(
                output_config,
                &fields.archive_name,
                client_context,
                &srum_table.topic,
            )?;

            if let Err(e) = self.parse_table(&srum_table.name, &mut output, fields) {
                warn!(
                    "Srum Table:'{}', Input:'{}/{}', Error: {e}",
                    &srum_table.topic, &fields.archive_name, &fields.archive_file
                );
            }
            num_rows += output.num_rows();
        }
        Ok(num_rows)
    }

    ///
    /// parse a table, convert to jsonl and write result to the output
    ///
    fn parse_table(
        &self,
        table_name: &str,
        output: &mut Output,
        fields: &Fields,
    ) -> Result<(), Error> {
        let table = self.db.table_by_name(table_name)?;

        let mut columns = Vec::new();
        for col in table.iter_columns()? {
            let col = col?;
            let col_name = col.name()?;
            let col_type = column_type(&col_name);
            columns.push((col_name, col_type));
        }

        for row in table.iter_records()? {
            let row = row?;
            let mut data = serde_json::Map::new();
            let mut tuple = Tuple::new(fields);
            let mut sort_data = None;
            for (pos, column) in row.iter_values()?.enumerate() {
                let (column_name, data_type) = &columns[pos];
                let column = column?;

                let value = match data_type {
                    SrumDataType::String => self.get_string_from_index(column),
                    SrumDataType::Date => {
                        let date = self.get_date(column);
                        if let Some(date) = date {
                            if column_name.eq(SRUM_SORT_FIELD) {
                                sort_data = Some(date.timestamp());
                            }
                            json!(date.format(OUTPUT_DATE_FORMAT_UTC).to_string())
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    _ => self.get_data(column),
                };
                data.insert(column_name.to_owned(), value);
            }
            tuple.set_data(serde_json::Value::Object(data), sort_data)?;
            output.write(tuple)?;
        }

        Ok(())
    }

    ///
    /// Retrieve a string from the cache  
    ///
    fn get_string_from_index(&self, column: libesedb::Value) -> serde_json::Value {
        let index = column.to_i32();
        match index {
            Some(e) => match self.index.get(&e) {
                Some(s) => json!(s),
                None => serde_json::Value::Null,
            },
            None => serde_json::Value::Null,
        }
    }

    ///
    /// convert the internal date format to UTC and convert it in a json String
    ///
    fn get_date(&self, column: libesedb::Value) -> Option<DateTime<Utc>> {
        match column {
            libesedb::Value::F64(e) => {
                let systime = systemtime_from_oletime(e);
                Some(systime.into())
            }
            libesedb::Value::DateTime(_) => match column.to_oletime() {
                Some(systime) => Some(systime.into()),
                None => None,
            },
            libesedb::Value::I64(e) => {
                let systime = systemtime_from_filetime(e as u64);
                Some(systime.into())
            }

            _ => None,
        }
    }

    ///
    /// convert data to json
    ///
    fn get_data(&self, column: libesedb::Value) -> serde_json::Value {
        match column {
            libesedb::Value::Bool(v) => json!(v.to_string()),
            libesedb::Value::U8(v) => json!(v),
            libesedb::Value::I16(v) => json!(v),
            libesedb::Value::I32(v) => json!(v),
            libesedb::Value::F32(v) => json!(v),
            libesedb::Value::F64(v) => json!(v),
            libesedb::Value::DateTime(v) => json!(v),
            libesedb::Value::U32(v) => json!(v),
            libesedb::Value::U16(v) => json!(v),
            libesedb::Value::Text(v) | libesedb::Value::LargeText(v) => json!(v),
            libesedb::Value::I64(v) | libesedb::Value::Currency(v) => json!(v),
            libesedb::Value::Binary(items)
            | libesedb::Value::LargeBinary(items)
            | libesedb::Value::Guid(items)
            | libesedb::Value::SuperLarge(items) => json!(enc64.encode(&items)),
            _ => serde_json::Value::Null,
        }
    }
}

///
/// Data types used in the field definition
///
enum SrumDataType {
    String,
    Date,
    Data,
}
///
/// retrieve the field type for a column
///
fn column_type(column_name: &str) -> SrumDataType {
    match column_name {
        "TimeStamp" => SrumDataType::Date,
        "AppId" => SrumDataType::String,
        "UserId" => SrumDataType::String,
        "ConnectStartTime" => SrumDataType::Date,
        _ => SrumDataType::Data,
    }
}

///
/// Convert a binary Microsoft Security Identifier into its standard string representation
///
pub fn convert_sid(b_sid: &[u8]) -> String {
    // sid[0] is the Revision, we allow only version 1, because it's the
    // only version that exists right now.
    let mut sid = "S-1-".to_string();

    // The next byte specifies the numbers of sub authorities
    // (number of dashes minus two), should be 5 or less, but not enforcing that
    let sub_authority_count = b_sid[1];

    // identifier authority (6 bytes starting from the second) (big endian)
    let mut identifier_authority: u64 = 0;
    let offset = 2;
    let size = 6;
    for i in 0..size {
        identifier_authority |= (b_sid[offset + i] as u64) << (8 * (size - 1 - i));
    }
    sid.push_str(&identifier_authority.to_string());

    // Iterate all the sub authorities (little-endian)
    let mut offset = 8;
    let size = 4; // 32-bits (4 bytes) for sub authorities

    for _ in 0..sub_authority_count {
        let arr: [u8; 4] = b_sid[offset..offset + size].try_into().unwrap();
        let sub_authority = u32::from_le_bytes(arr);
        sid.push('-');
        sid.push_str(&sub_authority.to_string());
        offset += size;
    }
    sid
}

// ///
// /// Convert a Microsoft ole date time format to the standard unix format.
// /// it assumes that the provided date is in UTC (wich is the case for all microsoft system dates)
// ///
// fn date_from_ole_timestamp(timestamp: f64) -> DateTime<Utc> {
//     let ticks = ((timestamp - MS_DAY_OFFSET) * DAY_MILLISECONDS) as i64;
//     Utc.timestamp_millis_opt(ticks).unwrap()
// }

// ///
// /// Convert a Microsoft filetime date format the standard unix format.
// /// it assumes that the provided date is in UTC (which is the case for all microsoft system dates)
// ///
// fn date_from_filetime(timestamp: u64) -> DateTime<Utc> {
//     let ts_micro = (timestamp / 10) as i64;
//     let delta = TimeDelta::microseconds(ts_micro);
//     let origin = Utc.with_ymd_and_hms(1601, 1, 1, 0, 0, 0).unwrap();
//     origin + delta
// }

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::{
        input::srum_model::{APPLICATION_RESOURCES, NETWORK_CONNECTIVITY_USAGE},
        writer::file_writer::MemoryWriter,
    };

    use super::*;

    pub const SRUM_PATH: &str = "data/parser/SRUDB.dat";
    #[test]
    fn sid_converter() {
        let bytes = vec![
            1, 5, 0, 0, 0, 0, 0, 5, 21, 0, 0, 0, 2, 49, 177, 214, 81, 110, 183, 63, 11, 252, 167,
            173, 147, 48, 0, 0,
        ];
        let expected = "S-1-5-21-3601936642-1068985937-2913467403-12435";
        let result = convert_sid(&bytes);
        assert_eq!(result, expected);
        //println!("{result}");
    }

    #[test]
    fn table_columns() {
        let db = EseDb::open(SRUM_PATH).unwrap();

        let table = db
            .table_by_name("{5C8CF1C7-7257-4F13-B223-970EF5939312}")
            .unwrap();

        let col_count = table.count_columns().unwrap();
        println!("col count {col_count}");
        for column in table.iter_columns().unwrap() {
            let column = column.unwrap();
            println!("{} {:?}", column.name().unwrap(), column.variant().unwrap());
        }

        // let s = table.count_records().unwrap();
    }

    #[test]
    fn parse_table() {
        let now = Instant::now();
        let p = SrumParser::new(SRUM_PATH).unwrap();
        println!("cache completed in : {:.2?}", now.elapsed());

        assert_eq!(712, p.index.len());

        let fields = Fields::new(
            "mymachine",
            "c:\\system32\\SRUDB.dat",
            "mymachine_ORC.7z",
            "SRUDB.dat",
        );

        //
        // test network connectivity
        //
        let output = MemoryWriter::new(1);
        let buffer = output.get_buffer();
        let mut output = Output {
            list: vec![Box::new(output)],
            num_rows: 0,
        };

        //let t = NETWORK_CONNECTIVITY_USAGE_TOPIC;

        // let table_name = tables.get("srum_network_connectivity_usage").unwrap();
        let now = Instant::now();
        p.parse_table(NETWORK_CONNECTIVITY_USAGE, &mut output, &fields)
            .unwrap();

        {
            let json: serde_json::Value = serde_json::from_str(&buffer.borrow()[0]).unwrap();
            //println!("{}", serde_json::to_string_pretty(&json).unwrap());
            let object = json.as_object().unwrap();
            let data = object.get("data").unwrap();
            let res: String = serde_json::to_string(data).unwrap();
            assert_eq!(
                res,
                r#"{"AutoIncId":1,"TimeStamp":"2022-03-10 16:34:59.999","AppId":null,"UserId":null,"InterfaceLuid":1689399632855040,"L2ProfileId":0,"ConnectedTime":66,"ConnectStartTime":"2022-03-10 16:33:53.000","L2ProfileFlags":0}"#
            );
            // println!("{}", res);
        }
        println!(
            "parsed 'srum_network_connectivity_usage' in : {:.2?}",
            now.elapsed()
        );

        //
        // test network data usage
        //
        let output = MemoryWriter::new(1);
        let buffer = output.get_buffer();
        let mut output = Output {
            list: vec![Box::new(output)],
            num_rows: 0,
        };

        let now = Instant::now();
        p.parse_table(APPLICATION_RESOURCES, &mut output, &fields)
            .unwrap();

        {
            let json: serde_json::Value = serde_json::from_str(&buffer.borrow()[0]).unwrap();
            let object = json.as_object().unwrap();
            let data = object.get("data").unwrap();
            let res: String = serde_json::to_string(data).unwrap();
            assert_eq!(
                res,
                r#"{"AutoIncId":82,"TimeStamp":"2022-03-10 16:34:59.999","AppId":"svc.ownproc.s0.uc0.host2000000000000000_1.0.0.0_neutral__1234567890abc","UserId":"S-1-5-5-0-130205","ForegroundCycleTime":8046669904,"BackgroundCycleTime":0,"FaceTime":670188134,"ForegroundContextSwitches":55997,"BackgroundContextSwitches":0,"ForegroundBytesRead":2168832,"ForegroundBytesWritten":811008,"ForegroundNumReadOperations":108,"ForegroundNumWriteOperations":198,"ForegroundNumberOfFlushes":36,"BackgroundBytesRead":0,"BackgroundBytesWritten":0,"BackgroundNumReadOperations":0,"BackgroundNumWriteOperations":0,"BackgroundNumberOfFlushes":0}"#
            );
            // println!("{}", res);
        }
        println!(
            "parsed 'srum_application_resources' in : {:.2?}",
            now.elapsed()
        );
    }

    #[test]
    fn parse_all() {
        assert_eq!(10, srum_tables().len());
        let now = Instant::now();
        let p = SrumParser::new(SRUM_PATH).unwrap();
        println!("string cache read in : {:.2?}", now.elapsed());

        let fields = Fields::new(
            "mymachine",
            "c:\\system32\\SRUDB.dat",
            "mymachine_ORC.7z",
            "SRUDB.dat",
        );

        let now = Instant::now();
        p.parse_all_tables("test_client", &fields, &vec![]).unwrap();
        println!("Parse all : {:.2?}", now.elapsed());
    }
}
