use std::{
    fs::File,
    hash::{DefaultHasher, Hash, Hasher},
    path::Path,
    usize,
};

use crate::{
    errors::Error,
    output::{Fields, OUTPUT_DATE_FORMAT_UTC, Output, OutputConfig, Tuple},
};
use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone, Utc};
use csv::StringRecord;
use log::error;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::csv_mapping::CsvMapping;
///
/// Alloue 10 Mo au buffer de lecture
///
const BUFFER_CAPACITY: usize = 1024 * 1024 * 10;

///
/// Parse a windows evtx log file
///
pub fn parse_csv<P, M>(
    path: P,
    client_context: &str,
    fields: &Fields,
    output_config: &[OutputConfig],
    mapping_path: M,
    best_effort: bool,
    skip_lines: usize,
) -> Result<usize, Error>
where
    P: AsRef<Path>,
    M: AsRef<Path>,
{
    let mapping = CsvMapping::load(mapping_path)?;

    let mut reader: csv::Reader<_> = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(mapping.csv_delimiter as u8)
        .buffer_capacity(BUFFER_CAPACITY)
        .from_path(path)?;

    let headers: &csv::StringRecord = reader.headers()?;
    let (converters, sort_col) = create_converters(headers, &mapping);

    let mut output = Output::new(
        output_config,
        &fields.archive_name,
        client_context,
        &mapping.topic,
    )?;

    parse(
        reader,
        &converters,
        sort_col,
        fields,
        &mut output,
        best_effort,
        skip_lines,
    )?;

    Ok(output.num_rows())
}

type SortColumn = usize;

fn parse(
    mut csv_reader: csv::Reader<File>,
    converters: &[Converter],
    sort_column: SortColumn,
    fields: &Fields,
    output: &mut Output,
    best_effort: bool,
    skip_lines: usize,
) -> Result<(), Error> {
    for (line_nb, line) in csv_reader.records().enumerate() {
        if line_nb < skip_lines {
            continue;
        }

        let json_line = convert_line(&converters, line, sort_column, line_nb);
        match json_line {
            Ok((data, sort_data)) => {
                let mut tuple = Tuple::new(fields);
                tuple.set_data(data, sort_data)?;
                output.write(tuple)?;
            }
            Err(err) => {
                if best_effort {
                    error!("Skipping Line {line_nb} caused by error: {err}");
                } else {
                    return Err(err);
                }
            }
        }
    }

    Ok(())
}

///
/// the first line of the csv is parsed to associate field names with the ones defined in the mapping
///
fn create_converters(
    headers: &csv::StringRecord,
    mapping: &CsvMapping,
) -> (Vec<Converter>, SortColumn) {
    let mut converters = Vec::with_capacity(headers.len());
    let mut sort_pos: usize = usize::MAX;
    for (pos, field_name) in headers.iter().enumerate() {
        let field = mapping.fields.get(field_name);

        if let Some(sort_field) = &mapping.sort_field {
            if field_name.eq(sort_field) {
                sort_pos = pos;
            }
        }
        let converter = match field {
            Some(field_type) => match field_type {
                FieldType::String { mandatory }
                | FieldType::Integer { mandatory }
                | FieldType::Float { mandatory } => Converter {
                    field_name: field_name.to_owned(),
                    mandatory: *mandatory,
                    field_type: field_type.clone(),
                    ..Default::default()
                },
                FieldType::Date {
                    mandatory,
                    input_date_format: _,
                } => Converter {
                    field_name: field_name.to_owned(),
                    mandatory: *mandatory,
                    field_type: field_type.clone(),
                    default_date_format: mapping.default_date_format.clone(),
                },
            },
            None => Converter {
                field_name: field_name.to_owned(),
                ..Default::default()
            },
        };

        converters.push(converter);
    }
    (converters, sort_pos)
}

///
/// Convert a csv line to a json object
///
fn convert_line(
    converters: &[Converter],
    record: Result<StringRecord, csv::Error>,
    sort_column: usize,
    line_num: usize,
) -> Result<(Value, Option<i64>), Error> {
    let record = record?;
    let mut map: Map<String, Value> = Map::with_capacity(record.len());
    let mut sort_data = None;
    for (column, data) in record.iter().enumerate() {
        let conv = &converters[column];
        if data.is_empty() {
            if conv.mandatory {
                return Err(Error::CsvMandatoryField(
                    line_num,
                    column,
                    conv.field_name.clone(),
                ));
            }
            //do not parse or insert empty fields
            continue;
        }
        if sort_column == column {
            sort_data = Some(conv.to_sort_data(data)?);
        }
        match conv.to_json_value(data) {
            Ok(value) => {
                map.insert(conv.field_name.clone(), value);
            }
            Err(e) => {
                return Err(Error::CsvParsing(
                    line_num,
                    column,
                    conv.field_name.clone(),
                    e.to_string(),
                ));
            }
        }
    }
    let obj = serde_json::Value::Object(map);
    Ok((obj, sort_data))
}

///
/// Liste les formats de date possible
///
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub enum DateFormat {
    #[default]
    Rfc3339,
    Rfc2822,
    Pattern(String, bool),
}

///
/// liste les formats de sortie possibles
///
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum FieldType {
    String {
        #[serde(default)]
        mandatory: bool,
    },
    Integer {
        #[serde(default)]
        mandatory: bool,
    },
    Float {
        #[serde(default)]
        mandatory: bool,
    },
    Date {
        #[serde(default)]
        mandatory: bool,
        input_date_format: Option<DateFormat>,
    },
}
impl Default for FieldType {
    fn default() -> Self {
        Self::String { mandatory: false }
    }
}
#[derive(Default)]
struct Converter {
    pub field_name: String,
    pub mandatory: bool,
    field_type: FieldType,
    default_date_format: DateFormat,
}
impl Converter {
    pub fn to_json_value(&self, value: &str) -> Result<serde_json::Value, Error> {
        match &self.field_type {
            FieldType::String { .. } => Ok(json!(value)),
            FieldType::Integer { .. } => {
                let value: i64 = value.parse()?;
                Ok(json!(value))
            }
            FieldType::Float { .. } => {
                let value: f64 = value.parse()?;
                Ok(json!(value))
            }
            FieldType::Date {
                mandatory: _,
                input_date_format,
            } => {
                let input_date_format = input_date_format
                    .as_ref()
                    .unwrap_or(&self.default_date_format);
                let value = Self::convert_date(value, input_date_format)?;
                Ok(json!(value))
            }
        }
    }

    pub fn to_sort_data(&self, value: &str) -> Result<i64, Error> {
        match &self.field_type {
            FieldType::String { .. } => {
                let mut hasher = DefaultHasher::new();
                value.hash(&mut hasher);
                let hash = hasher.finish();
                Ok(hash as i64)
            }
            FieldType::Integer { .. } => {
                let value: i64 = value.parse()?;
                Ok(value)
            }
            FieldType::Float { .. } => {
                let value: f64 = value.parse()?;
                Ok(value as i64)
            }
            FieldType::Date {
                mandatory: _,
                input_date_format,
            } => {
                let input_date_format = input_date_format
                    .as_ref()
                    .unwrap_or(&self.default_date_format);
                let value = Self::convert_to_timestamp(value, input_date_format)?;
                Ok(value)
            }
        }
    }

    fn convert_date(value: &str, input_date_format: &DateFormat) -> Result<String, Error> {
        let date = match input_date_format {
            DateFormat::Rfc2822 => {
                let date = DateTime::parse_from_rfc2822(value)?;
                date.to_utc().format(OUTPUT_DATE_FORMAT_UTC)
            }
            DateFormat::Rfc3339 => {
                let date = DateTime::parse_from_rfc3339(value)?;
                date.to_utc().format(OUTPUT_DATE_FORMAT_UTC)
            }
            DateFormat::Pattern(pattern, has_timezone) => {
                if *has_timezone {
                    let date: DateTime<FixedOffset> = DateTime::parse_from_str(value, &pattern)?;
                    date.to_utc().format(OUTPUT_DATE_FORMAT_UTC)
                } else {
                    let date = NaiveDateTime::parse_from_str(value, &pattern)?;
                    let date_time: DateTime<Utc> = Utc.from_local_datetime(&date).unwrap();
                    date_time.format(OUTPUT_DATE_FORMAT_UTC)
                }
            }
        };
        Ok(date.to_string())
    }

    fn convert_to_timestamp(value: &str, input_date_format: &DateFormat) -> Result<i64, Error> {
        let date = match input_date_format {
            DateFormat::Rfc2822 => {
                let date = DateTime::parse_from_rfc2822(value)?;
                date.to_utc().timestamp_millis()
            }
            DateFormat::Rfc3339 => {
                let date = DateTime::parse_from_rfc3339(value)?;
                date.to_utc().timestamp_millis()
            }
            DateFormat::Pattern(pattern, has_timezone) => {
                if *has_timezone {
                    let date: DateTime<FixedOffset> = DateTime::parse_from_str(value, &pattern)?;
                    date.to_utc().timestamp_millis()
                } else {
                    let date = NaiveDateTime::parse_from_str(value, &pattern)?;
                    let date_time: DateTime<Utc> = Utc.from_local_datetime(&date).unwrap();
                    date_time.timestamp_millis()
                }
            }
        };
        Ok(date)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::{init_log, writer::file_writer::MemoryWriter};

    use super::*;

    #[test]
    fn string() {
        let converter = Converter {
            field_type: FieldType::String { mandatory: false },
            ..Default::default()
        };

        let value = "hello world";

        let res = converter.to_json_value(value).unwrap();

        assert_eq!(res.as_str().unwrap(), value);
    }

    #[test]
    fn integer() {
        let converter = Converter {
            field_type: FieldType::Integer { mandatory: false },
            ..Default::default()
        };
        let value = "1000";

        let res = converter.to_json_value(value).unwrap();

        assert_eq!(res.as_i64().unwrap().to_string(), value);

        let value = "1000.32";
        converter.to_json_value(value).expect_err("Invalid Integer");
    }

    #[test]
    fn float() {
        let converter = Converter {
            field_type: FieldType::Float { mandatory: false },
            ..Default::default()
        };
        let value = "1000";

        let res = converter.to_json_value(value).unwrap();

        assert_eq!(res.as_f64().unwrap().to_string(), value);

        let value = "1000.43242";
        let res = converter.to_json_value(value).unwrap();

        assert_eq!(res.as_f64().unwrap().to_string(), value);

        let value = "ABCD";
        converter.to_json_value(value).expect_err("Invalid Float");
    }

    #[test]
    fn date_rfc_2822() {
        let converter = Converter {
            field_type: FieldType::Date {
                mandatory: false,
                input_date_format: Some(DateFormat::Rfc2822),
            },
            ..Default::default()
        };

        let value = "Tue, 1 Jul 2003 10:52:37 +0200";

        let res = converter.to_json_value(value).unwrap();

        assert_eq!(res.as_str().unwrap(), "2003-07-01 08:52:37.000");

        let value = "ABCD";
        converter.to_json_value(value).expect_err("Invalid Date");
    }

    #[test]
    fn date_rfc_3339() {
        let converter = Converter {
            field_type: FieldType::Date {
                mandatory: false,
                input_date_format: Some(DateFormat::Rfc3339),
            },
            ..Default::default()
        };

        let value = "1996-12-19T16:39:57-08:00";

        let res = converter.to_json_value(value).unwrap();
        assert_eq!(res.as_str().unwrap(), "1996-12-20 00:39:57.000");

        let value = "ABCD";
        converter.to_json_value(value).expect_err("Invalid Date");
    }

    #[test]
    fn date_pattern_timezone() {
        let converter = Converter {
            field_type: FieldType::Date {
                mandatory: false,
                input_date_format: Some(DateFormat::Pattern(
                    "%Y-%m-%d %H:%M:%S %z".to_owned(),
                    true,
                )),
            },
            ..Default::default()
        };

        let value = "2016-01-22 03:08:51 -08:00";

        let res = converter.to_json_value(value).unwrap();
        assert_eq!(res.as_str().unwrap(), "2016-01-22 11:08:51.000");

        let value = "ABCD";
        converter.to_json_value(value).expect_err("Invalid Date");
    }

    #[test]
    fn date_pattern_naive() {
        let converter = Converter {
            field_type: FieldType::Date {
                mandatory: false,
                input_date_format: Some(DateFormat::Pattern("%Y-%m-%d %H:%M:%S".to_owned(), false)),
            },
            ..Default::default()
        };

        let value = "2016-01-22 03:08:51";

        let res = converter.to_json_value(value).unwrap();
        assert_eq!(res.as_str().unwrap(), "2016-01-22 03:08:51.000");

        let value = "ABCD";
        converter.to_json_value(value).expect_err("Invalid Date");
    }

    pub const DATA_PATH: &str = "data/parser/NTFSInfo.csv";
    pub const DATA_ERRORS_PATH: &str = "data/parser/NTFSError.csv";
    pub const MAPPING_PATH: &str = "data/ntfs_info.map.yaml";

    #[test]
    fn parse_simple() {
        init_log();
        let fields = Fields::new("mymachine", "NTFSINFO", "mymachine_ORC.7z", "NTFSINFO.csv");

        let output = MemoryWriter::new(1);
        let buffer = output.get_buffer();
        let mut output = Output {
            list: vec![Box::new(output)],
            num_rows: 0,
        };

        let mapping = CsvMapping::load(MAPPING_PATH).unwrap();

        let mut reader: csv::Reader<_> = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(mapping.csv_delimiter as u8)
            .buffer_capacity(BUFFER_CAPACITY)
            .from_path(DATA_PATH)
            .unwrap();

        let headers: &csv::StringRecord = reader.headers().unwrap();
        let (converters, sort_col) = create_converters(headers, &mapping);

        let now = Instant::now();
        parse(
            reader,
            &converters,
            sort_col,
            &fields,
            &mut output,
            false,
            0,
        )
        .unwrap();
        println!("Parse {} rows in {:.2?}", output.num_rows(), now.elapsed());

        //   println!("{}", &buffer.borrow()[0]);

        let json: serde_json::Value = serde_json::from_str(&buffer.borrow()[0]).unwrap();
        let object = json.as_object().unwrap();
        let data = object.get("data").unwrap();
        let res = serde_json::to_string(data).unwrap();
        //   println!("{}", res);
        assert_eq!(
            res,
            r#"{"ComputerName":"VM-WIN10-DEV","VolumeID":"0xC2E23C53E23C4E43","File":".","ParentName":"\\","Extension":".","SizeInBytes":0,"Attributes":"..D.H.....S..","CreationDate":"2015-10-30 06:28:30.642","LastModificationDate":"2016-02-03 11:00:25.927","LastAccessDate":"2016-02-03 11:00:25.927","LastAttrChangeDate":"2016-02-03 11:00:25.927","FileNameCreationDate":"2016-01-22 03:08:51.337","FileNameLastModificationDate":"2016-01-22 03:08:51.337","FileNameLastAccessDate":"2016-01-22 03:08:51.337","FileNameLastAttrModificationDate":"2016-01-22 03:08:51.337","USN":"0x0000000004571C88","FRN":"0x0005000000000005","ParentFRN":"0x0005000000000005","FilenameID":1,"RecordInUse":"Y","OwnerId":0,"FilenameFlags":3,"SecDescrID":265,"FilenameIndex":0,"SnapshotID":"{00000000-0000-0000-0000-000000000000}"}"#
        );
    }

    #[test]
    fn parse_skip() {
        init_log();
        let fields = Fields::new("mymachine", "NTFSINFO", "mymachine_ORC.7z", "NTFSINFO.csv");

        let output = MemoryWriter::new(1);
        let buffer = output.get_buffer();
        let mut output = Output {
            list: vec![Box::new(output)],
            num_rows: 0,
        };

        let mapping = CsvMapping::load(MAPPING_PATH).unwrap();

        let mut reader: csv::Reader<_> = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(mapping.csv_delimiter as u8)
            .buffer_capacity(BUFFER_CAPACITY)
            .from_path(DATA_PATH)
            .unwrap();

        let headers: &csv::StringRecord = reader.headers().unwrap();
        let (converters, sort_col) = create_converters(headers, &mapping);

        let now = Instant::now();
        parse(
            reader,
            &converters,
            sort_col,
            &fields,
            &mut output,
            false,
            1,
        )
        .unwrap();
        println!("Parse {} rows in {:.2?}", output.num_rows(), now.elapsed());

        let json: serde_json::Value = serde_json::from_str(&buffer.borrow()[0]).unwrap();
        let object = json.as_object().unwrap();
        let data = object.get("data").unwrap();
        let res = serde_json::to_string(data).unwrap();
        //  println!("{}", res);
        assert_eq!(
            res,
            r#"{"ComputerName":"VM-WIN10-DEV","VolumeID":"0xC2E23C53E23C4E43","File":"$Bitmap","ParentName":"\\","SizeInBytes":1950016,"Attributes":"....HN....S..","CreationDate":"2016-01-22 03:08:51.337","LastModificationDate":"2016-01-22 03:08:51.337","LastAccessDate":"2016-01-22 03:08:51.337","LastAttrChangeDate":"2016-01-22 03:08:51.337","FileNameCreationDate":"2016-01-22 03:08:51.337","FileNameLastModificationDate":"2016-01-22 03:08:51.337","FileNameLastAccessDate":"2016-01-22 03:08:51.337","FileNameLastAttrModificationDate":"2016-01-22 03:08:51.337","USN":"0x0000000000000000","FRN":"0x0006000000000006","ParentFRN":"0x0005000000000005","FilenameID":2,"DataID":4,"RecordInUse":"Y","OwnerId":0,"FilenameFlags":3,"SecDescrID":256,"FilenameIndex":0,"DataIndex":0,"SnapshotID":"{00000000-0000-0000-0000-000000000000}"}"#
        );
    }

    #[test]
    fn parse_error() {
        init_log();
        let fields = Fields::new("mymachine", "NTFSINFO", "mymachine_ORC.7z", "NTFSINFO.csv");

        let output = MemoryWriter::new(1);
        let mut output = Output {
            list: vec![Box::new(output)],
            num_rows: 0,
        };

        let mapping = CsvMapping::load(MAPPING_PATH).unwrap();

        let mut reader: csv::Reader<_> = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(mapping.csv_delimiter as u8)
            .buffer_capacity(BUFFER_CAPACITY)
            .from_path(DATA_ERRORS_PATH)
            .unwrap();

        let headers: &csv::StringRecord = reader.headers().unwrap();
        let (converters, sort_col) = create_converters(headers, &mapping);

        parse(
            reader,
            &converters,
            sort_col,
            &fields,
            &mut output,
            false,
            0,
        )
        .expect_err("msg");
    }

    #[test]
    fn parse_best_effort() {
        init_log();
        let fields = Fields::new("mymachine", "NTFSINFO", "mymachine_ORC.7z", "NTFSINFO.csv");

        let output = MemoryWriter::new(1);
        let buffer = output.get_buffer();
        let mut output = Output {
            list: vec![Box::new(output)],
            num_rows: 0,
        };

        let mapping = CsvMapping::load(MAPPING_PATH).unwrap();

        let mut reader: csv::Reader<_> = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(mapping.csv_delimiter as u8)
            .buffer_capacity(BUFFER_CAPACITY)
            .from_path(DATA_ERRORS_PATH)
            .unwrap();

        let headers: &csv::StringRecord = reader.headers().unwrap();
        let (converters, sort_col) = create_converters(headers, &mapping);

        let now = Instant::now();
        parse(reader, &converters, sort_col, &fields, &mut output, true, 0).unwrap();
        println!("Parse {} rows in {:.2?}", output.num_rows(), now.elapsed());

        let json: serde_json::Value = serde_json::from_str(&buffer.borrow()[0]).unwrap();
        let object = json.as_object().unwrap();
        let data = object.get("data").unwrap();
        let res = serde_json::to_string(data).unwrap();
        //println!("{}", res);
        assert_eq!(
            res,
            r#"{"ComputerName":"VM-WIN10-DEV","VolumeID":"0xC2E23C53E23C4E43","File":"$Bitmap","ParentName":"\\","SizeInBytes":1950016,"Attributes":"....HN....S..","CreationDate":"2016-01-22 03:08:51.337","LastModificationDate":"2016-01-22 03:08:51.337","LastAccessDate":"2016-01-22 03:08:51.337","LastAttrChangeDate":"2016-01-22 03:08:51.337","FileNameCreationDate":"2016-01-22 03:08:51.337","FileNameLastModificationDate":"2016-01-22 03:08:51.337","FileNameLastAccessDate":"2016-01-22 03:08:51.337","FileNameLastAttrModificationDate":"2016-01-22 03:08:51.337","USN":"0x0000000000000000","FRN":"0x0006000000000006","ParentFRN":"0x0005000000000005","FilenameID":2,"DataID":4,"RecordInUse":"Y","OwnerId":0,"FilenameFlags":3,"SecDescrID":256,"FilenameIndex":0,"DataIndex":0,"SnapshotID":"{00000000-0000-0000-0000-000000000000}"}"#
        );
    }
}
