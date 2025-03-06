use std::path::Path;

use crate::{
    Error,
    configuration::DataType,
    output::{Fields, OUTPUT_DATE_FORMAT_UTC, Output, OutputConfig, Tuple},
};
use chrono::{DateTime, FixedOffset, Utc};
use evtx::ParserSettings;
use serde_json::{Value, json};

use evtx::EvtxParser;

pub const EVTX_TABLE_NAME: &str = "evtx";
pub const EVTX_SORT_FIELD: &str = "System.TimeCreated";
///
/// Parse a windows evtx log file
///
pub fn parse_evtx<P: AsRef<Path>>(
    path: P,
    client_context: &str,
    fields: &Fields,
    output_config: &[OutputConfig],
) -> Result<usize, Error> {
    let mut output = Output::new(
        output_config,
        &fields.archive_name,
        client_context,
        EVTX_TABLE_NAME,
    )?;

    let num_rows = parse(path, fields, &mut output)?;
    Ok(num_rows)
}

fn parse<P: AsRef<Path>>(path: P, fields: &Fields, output: &mut Output) -> Result<usize, Error> {
    let settings = ParserSettings::new().separate_json_attributes(true);
    let parser = EvtxParser::from_path(path).unwrap();
    let mut parser = parser.with_configuration(settings);

    let mut num_rows = 0;
    for record in parser.records_json_value() {
        let record = record?;
        let (event, sort_data) = format_event(record.data).unwrap();
        let mut tuple = Tuple::new(fields);
        tuple.set_data(event, Some(sort_data))?;
        output.write(tuple)?;
        num_rows += 1;
    }
    Ok(num_rows)
}

///
/// Simplify the output and format the TimeCreated date properly
///
fn format_event(mut event: Value) -> Result<(Value, i64), Error> {
    let mut event = event
        .as_object_mut()
        .ok_or(Error::JsonNotAndObject("event".to_owned()))?
        .remove("Event")
        .ok_or(Error::Evtx("Event data not found".to_string()))?;

    let system = event
        .as_object_mut()
        .ok_or(Error::JsonNotAndObject("Event".to_owned()))?
        .get_mut("System")
        .ok_or(Error::Evtx("System data not found".to_string()))?
        .as_object_mut()
        .ok_or(Error::JsonNotAndObject("Event.System".to_owned()))?;

    let val = system.remove("Provider_attributes");
    if let Some(val) = val {
        system.insert("Provider".to_owned(), val);
    }

    let val = system.remove("Execution_attributes");
    if let Some(val) = val {
        system.insert("Execution".to_owned(), val);
    }

    let val = system.remove("Security_attributes");
    if let Some(mut val) = val {
        let map = val.as_object_mut().ok_or(Error::JsonNotAndObject(
            "Event.System.Security_attributes".to_owned(),
        ))?;
        let uid = map.remove("UserID").ok_or(Error::JsonNotAndObject(
            "Event.System.Security_attributes.UserID".to_owned(),
        ))?;
        system.insert("UserID".to_owned(), uid);
    }

    let mut time_created =
        system
            .remove("TimeCreated_attributes")
            .ok_or(Error::JsonNotAndObject(
                "Event.System.TimeCreated_attributes".to_owned(),
            ))?;

    let time_created = time_created.as_object_mut().ok_or(Error::JsonNotAndObject(
        "Event.System.TimeCreated_attributes".to_owned(),
    ))?;

    let time_created = time_created
        .remove("SystemTime")
        .ok_or(Error::JsonNotAndObject(
            "Event.System.TimeCreated_attributes.SystemTime".to_owned(),
        ))?;

    let time_created = time_created.as_str().ok_or(Error::JsonNotaString(
        "Event.System.TimeCreated_attributes.SystemTime".to_owned(),
    ))?;

    let datetime: DateTime<FixedOffset> = DateTime::parse_from_rfc3339(time_created)?;
    let datetime: DateTime<Utc> = datetime.into();
    let sort_data = datetime.timestamp();
    let value = json!(datetime.format(OUTPUT_DATE_FORMAT_UTC).to_string());
    system.insert("TimeCreated".to_owned(), value);
    Ok((event, sort_data))
}

pub fn evtx_fields() -> Vec<(String, DataType)> {
    vec![
        ("System.Execution.ProcessID".to_owned(), DataType::Int64),
        ("System.Execution.ThreadID".to_owned(), DataType::Int64),
        ("System.EventID".to_owned(), DataType::Uint16),
        ("System.Version".to_owned(), DataType::Int64),
        ("System.Level".to_owned(), DataType::Uint8),
        ("System.Task".to_owned(), DataType::Int64),
        ("System.Opcode".to_owned(), DataType::Int64),
        ("System.Keywords".to_owned(), DataType::String),
        ("System.UserID".to_owned(), DataType::String),
        ("System.EventRecordID".to_owned(), DataType::Int64),
        ("System.Provider.Name".to_owned(), DataType::String),
        ("System.Provider.Guid".to_owned(), DataType::String),
        ("System.Channel".to_owned(), DataType::String),
        ("System.Computer".to_owned(), DataType::String),
        ("System.TimeCreated".to_owned(), DataType::Date),
    ]
}

#[cfg(test)]
mod tests {

    use std::time::Instant;

    use crate::{init_log, writer::file_writer::TestWriter};

    use super::*;
    pub const EVTX_PATH: &str = "data/parser/kernel_pnp.evtx";
    #[test]
    fn parse_test() {
        init_log();
        let fields = Fields::new(
            "mymachine",
            "c:\\system32\\log\\kernel_pnp.evtx",
            "mymachine_ORC.7z",
            "kernel_pnp.evtx",
        );

        let output = TestWriter::new(1);
        let buffer = output.get_buffer();
        let mut output = Output {
            list: vec![Box::new(output)],
        };

        let now = Instant::now();
        let num = parse(EVTX_PATH, &fields, &mut output).unwrap();
        println!("Parse {} rows in {:.2?}", num, now.elapsed());

        let json: serde_json::Value = serde_json::from_str(&buffer.borrow()[0]).unwrap();
        let object = json.as_object().unwrap();
        let data = object.get("data").unwrap();
        let res = serde_json::to_string(data).unwrap();
        assert_eq!(
            res,
            r#"{"System":{"Execution":{"ProcessID":4,"ThreadID":8},"EventID":403,"Version":0,"Level":3,"Task":0,"Opcode":0,"Keywords":"0x4000000000000000","UserID":"S-1-5-18","EventRecordID":1,"Correlation":null,"Provider":{"Name":"Microsoft-Windows-Kernel-PnP","Guid":"9C205A39-1250-487D-ABD7-E831C6290539"},"Channel":"Microsoft-Windows-Kernel-PnP/Configuration","Computer":"MINWINPC","TimeCreated":"2016-04-26 21:30:13.520"},"EventData":{"DeviceInstanceId":"ROOT\\ACPI_HAL\\0000","DriverName":"hal.inf","ClassGuid":"4D36E966-E325-11CE-BFC1-08002BE10318","DriverDate":"06/21/2006","DriverVersion":"10.0.10586.0","DriverProvider":"Microsoft","DriverInbox":true,"DriverSection":"ACPI_AMD64_HAL","DriverRank":"0xff0000","MatchingDeviceId":"acpiapic","OutrankedDrivers":"","DeviceUpdated":false,"Status":"0x0","ParentDeviceInstanceId":"HTREE\\ROOT\\0"}}"#
        );
        //println!("{}", res);
    }
}

// 0x00000001 WINEVENT_LEVEL_CRITICAL
// 0x00000002 WINEVENT_LEVEL_ERROR
// 0x00000003 WINEVENT_LEVEL_WARNING
// 0x00000004 WINEVENT_LEVEL_INFO
// 0x00000005 WINEVENT_LEVEL_VERBOSE
