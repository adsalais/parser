use crate::{
    Error,
    configuration::DataType,
    output::{Fields, OUTPUT_DATE_FORMAT_UTC, Output, OutputConfig, Tuple},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as enc64};
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use nt_hive::{Hive, KeyNode, KeyValue, KeyValueData, KeyValueDataType, Result};
use serde_json::json;
use std::{fs::File, io::Read, path::Path};
use zerocopy::SplitByteSlice;

pub const HIVE_TABLE_NAME: &str = "hive";
pub const HIVE_SORT_FIELD: &str = HIVE_DATE;

const HIVE_PATH: &str = "Path";
const HIVE_CLASS: &str = "Class";
const HIVE_DATE: &str = "TimeStamp";
const HIVE_VALUE: &str = "Value";
pub fn hive_fields() -> Vec<(String, DataType)> {
    vec![
        (HIVE_PATH.to_owned(), DataType::String),
        (HIVE_CLASS.to_owned(), DataType::String),
        (HIVE_DATE.to_owned(), DataType::Date),
    ]
}
///
/// Parse a windows hive file
///
pub fn parse_hive<P: AsRef<Path>>(
    path: P,
    client_context: &str,
    fields: &Fields,
    root_name: &str,
    output_config: &[OutputConfig],
) -> Result<usize, Error> {
    let mut output = Output::new(
        output_config,
        &fields.archive_name,
        client_context,
        HIVE_TABLE_NAME,
    )?;

    parse(path, root_name, fields, &mut output)?;
    Ok(output.num_rows())
}

fn parse<P: AsRef<Path>>(
    path: P,
    root_name: &str,
    fields: &Fields,
    output: &mut Output,
) -> Result<(), Error> {
    let mut hive_file = File::open(path.as_ref()).unwrap();
    let mut data = Vec::with_capacity(hive_file.metadata().unwrap().len() as usize);
    hive_file.read_to_end(&mut data).unwrap();

    let hive = Hive::without_validation(data.as_ref())
        .map_err(|e| {
            format!(
                "Error parsing hive file: '{}' - {e}",
                path.as_ref().to_string_lossy()
            )
        })
        .unwrap();

    let root_key = hive
        .root_key_node()
        .map_err(|e| {
            format!(
                "Error getting root key in file: '{}' - {e}",
                path.as_ref().to_string_lossy()
            )
        })
        .unwrap();

    parse_subkey(root_key, &root_name, fields, output)?;
    Ok(())
}

fn parse_subkey<B>(
    key_node: KeyNode<B>,
    path: &str,
    fields: &Fields,
    output: &mut Output,
) -> Result<(), Error>
where
    B: SplitByteSlice,
{
    if let Some(subkeys) = key_node.subkeys() {
        let subkeys = subkeys?;

        for key_node in subkeys {
            let key_node = key_node?;

            let key_name = key_node.name()?.to_string_lossy();

            let path = format!("{path}\\{key_name}");

            let filetime = from_filetime(key_node.timestamp()?);
            let timestamp = filetime.timestamp();
            let date = filetime.format(OUTPUT_DATE_FORMAT_UTC).to_string();

            let class_name = if let Some(class_name) = key_node.class_name() {
                Some(class_name?.to_string_lossy())
            } else {
                None
            };

            let mut data = serde_json::Map::new();
            data.insert(HIVE_PATH.to_owned(), json!(path));
            data.insert(HIVE_DATE.to_owned(), json!(date));
            if let Some(class) = &class_name {
                data.insert(HIVE_CLASS.to_owned(), json!(class));
            }
            let mut tuple = Tuple::new(fields);
            tuple.set_data(serde_json::Value::Object(data), Some(timestamp))?;
            output.write(tuple)?;

            if let Some(value_iter) = key_node.values() {
                let value_iter = value_iter?;

                for value in value_iter {
                    let key_value: KeyValue<'_, B> = value?;

                    let mut value_name = key_value.name()?.to_string_lossy();
                    if value_name.is_empty() {
                        value_name.push_str("Default");
                    }

                    let value = match parse_key_value(key_value) {
                        Ok(v) => v,
                        Err(e) => {
                            // println!("{e:?}");
                            serde_json::Value::String(format!("{e}"))
                        }
                    };

                    let path = format!("{path}\\{value_name}");
                    let mut data = serde_json::Map::new();
                    data.insert(HIVE_PATH.to_owned(), json!(path));
                    data.insert(HIVE_DATE.to_owned(), json!(date));
                    if let Some(class) = &class_name {
                        data.insert(HIVE_CLASS.to_owned(), json!(class));
                    }
                    data.insert(HIVE_VALUE.to_owned(), value);

                    let mut tuple = Tuple::new(fields);
                    tuple.set_data(serde_json::Value::Object(data), Some(timestamp))?;
                    output.write(tuple)?;
                }
            }

            parse_subkey(key_node, &path, fields, output)?;
        }
    }

    Ok(())
}

///
/// translate hive values to serde_json::Value
///
fn parse_key_value<B>(value: KeyValue<B>) -> Result<serde_json::Value, Error>
where
    B: SplitByteSlice,
{
    let json_value = match value.data_type() {
        Ok(value_type) => match value_type {
            KeyValueDataType::RegSZ | KeyValueDataType::RegExpandSZ => {
                let string_data = value.string_data()?;
                json!(string_data)
            }
            KeyValueDataType::RegBinary => {
                let binary_data = value.data()?;
                match binary_data {
                    KeyValueData::Small(data) => {
                        let binary = enc64.encode(data);
                        json!(binary)
                    }
                    KeyValueData::Big(iter) => {
                        let data = iter.collect::<Result<Vec<_>>>()?;

                        let data_size = value.data_size();
                        let mut value: Vec<u8> = Vec::with_capacity(data_size as usize);
                        for slice in data {
                            value.extend_from_slice(slice);
                        }

                        json!(enc64.encode(&value))
                    }
                }
            }
            KeyValueDataType::RegDWord | KeyValueDataType::RegDWordBigEndian => {
                let dword_data = value.dword_data()?;
                json!(dword_data)
            }
            KeyValueDataType::RegMultiSZ => {
                let multi_string_data = value.multi_string_data()?.collect::<Result<Vec<_>>>()?;

                let string_value = multi_string_data.join("");
                json!(string_value)
            }
            KeyValueDataType::RegQWord => {
                let qword_data = value.qword_data()?;
                json!(qword_data)
            }
            _ => serde_json::Value::Null,
        },
        Err(e) => {
            let bytes = value.data().unwrap().into_vec().unwrap();
            let as_string = String::from_utf8_lossy(&bytes)
                .to_string()
                .replace("\0", "");

            let value = format!(
                "{{\"DataString\":\"{as_string}\", \"DataBase64\":\"{}\", \"Error\":\"{e}\"}}",
                enc64.encode(&bytes)
            );
            json!(value)
        }
    };

    Ok(json_value)
}

//
// parse TimeStamp in the FILETIME format: the number of 100-nanosecond intervals since January 1, 1601 (UTC).
//
fn from_filetime(timestamp: u64) -> DateTime<Utc> {
    let naive = NaiveDate::from_ymd_opt(1601, 1, 1)
        .and_then(|x| x.and_hms_nano_opt(0, 0, 0, 0))
        .expect("to_datetime() should work")
        + Duration::microseconds((timestamp / 10) as i64);

    Utc.from_local_datetime(&naive).unwrap()
}
#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::{init_log, writer::file_writer::MemoryWriter};

    use super::*;
    #[test]
    fn test_parse() {
        init_log();
        let output = MemoryWriter::new(20);
        let buffer = output.get_buffer();
        let mut output = Output {
            list: vec![Box::new(output)],
            num_rows: 0,
        };

        let fields = Fields::new(
            "mymachine",
            "c:\\system32\\log\\kernel_pnp.evtx",
            "mymachine_ORC.7z",
            "kernel_pnp.evtx",
        );
        let now = Instant::now();
        parse("data/parser/testhive", "kernel", &fields, &mut output).unwrap();
        println!("Parse {} rows in {:.2?}", output.num_rows(), now.elapsed());

        let s = &buffer.borrow()[10];

        let json: serde_json::Value = serde_json::from_str(s).unwrap();

        let data = json.get("data").unwrap();

        let path = data.get("Path").unwrap().as_str().unwrap();
        assert_eq!(path, "kernel\\data-test\\reg-sz");

        let value = data.get("Value").unwrap().as_str().unwrap();
        assert_eq!(value, "sz-test");
    }

    #[test]
    fn test_parse_error() {
        init_log();

        let output = MemoryWriter::new(22);
        let buffer = output.get_buffer();
        let mut output = Output {
            list: vec![Box::new(output)],
            num_rows: 0,
        };

        let fields = Fields::new(
            "mymachine",
            "c:\\system32\\log\\kernel_pnp.evtx",
            "mymachine_ORC.7z",
            "kernel_pnp.evtx",
        );
        let now = Instant::now();
        parse("data/parser/SAM.hive", "", &fields, &mut output).unwrap();
        println!("Parse {} rows in {:.2?}", output.num_rows(), now.elapsed());

        let s = &buffer.borrow()[21];

        let json: serde_json::Value = serde_json::from_str(s).unwrap();

        let data = json.get("data").unwrap();

        let path = data.get("Path").unwrap().as_str().unwrap();
        assert_eq!(
            path,
            "\\SAM\\Domains\\Account\\Groups\\Names\\None\\Default"
        );

        let value = data.get("Value").unwrap().as_str().unwrap();
        assert_eq!(
            value,
            "{\"DataString\":\"\", \"DataBase64\":\"\", \"Error\":\"The key value data type at offset 0x00001800 is 0x00000201, which is not supported\"}"
        );
    }
}
