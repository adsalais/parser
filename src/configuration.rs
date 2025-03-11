use std::{collections::HashSet, fs, path::Path};

use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as SerdeError};

use crate::{
    Error,
    input::{
        csv_mapping::CsvMapping,
        evtx::{EVTX_SORT_FIELD, EVTX_TABLE_NAME, evtx_fields},
        hive::{HIVE_SORT_FIELD, HIVE_TABLE_NAME, hive_fields},
        srum_model::{SRUM_SORT_FIELD, srum_tables},
    },
    output::{OutputConfig, full_topic_name},
};

///
/// defines the available parser
///
#[derive(Serialize, Deserialize, Clone, Debug)]
//#[serde(tag = "type")]
#[allow(non_camel_case_types)]
pub enum ParserType {
    csv {
        mapping_file: String,
        best_effort: Option<bool>,
        skip_lines: Option<usize>,
    },
    evtx,
    hive {
        root_name: String,
    },
    srum,
}

///
/// Default configuration file that will be created if no file is provided
///
const DEFAULT_FILE_NAME: &str = "configuration.yaml";

///
/// The configuration file for the application
///
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Configuration {
    pub client_context: String,
    pub input_folder: String,
    #[serde(default)]
    pub input_is_decompressed: bool,
    #[serde(default)]
    pub temp_folder: String,
    #[serde(default)]
    pub archive_threads: usize,
    #[serde(default)]
    pub parsing_threads: usize,
    #[serde(default)]
    pub decompression_threads: usize,
    #[serde(default)]
    pub parsers: Vec<ParserConfig>,
    #[serde(default)]
    pub output: Vec<OutputConfig>,
}
impl Configuration {
    ///
    /// Load config file, creates a default one if no file is provided
    ///
    pub fn load(config: &Option<String>) -> Result<Configuration, Error> {
        let toml_file_name = match config {
            Some(file) => file.as_str(),
            None => {
                let ntf_challenge_path = Path::new(DEFAULT_FILE_NAME);
                if !ntf_challenge_path.exists() {
                    fs::write(ntf_challenge_path, SAMPLE_CONFIGURATION)?;
                }
                DEFAULT_FILE_NAME
            }
        };
        let config_path = Path::new(toml_file_name);
        if !config_path.exists() {
            return Err(Error::Configuration(toml_file_name.to_string()));
        }

        let toml_file: String = fs::read_to_string(config_path)?;
        let conf = serde_yml::from_str::<Configuration>(&toml_file).unwrap();

        Ok(conf)
    }

    ///
    /// list topics
    ///
    pub fn list_topics(&self) -> Result<Vec<DataTopic>, Error> {
        let mut list = Vec::new();
        let mut is_parsed = HashSet::new();
        for conf in &self.parsers {
            match &conf.parser {
                ParserType::csv {
                    mapping_file: config_file,
                    best_effort: _,
                    skip_lines: _,
                } => {
                    let mapping = CsvMapping::load(config_file)?;
                    let name = &mapping.topic;
                    if is_parsed.contains(name) {
                        continue;
                    }
                    is_parsed.insert(name.to_owned());
                    let topic_name = full_topic_name(&self.client_context, name);
                    let partial_field_def = mapping.partial_fields();
                    list.push(DataTopic::new(
                        topic_name,
                        name.to_owned(),
                        partial_field_def,
                        mapping.sort_field.unwrap_or("".to_owned()),
                    ));
                }
                ParserType::evtx => {
                    if is_parsed.contains(EVTX_TABLE_NAME) {
                        continue;
                    }
                    is_parsed.insert(EVTX_TABLE_NAME.to_owned());
                    let topic_name = full_topic_name(&self.client_context, EVTX_TABLE_NAME);
                    let partial_field_def = evtx_fields();
                    list.push(DataTopic::new(
                        topic_name,
                        EVTX_TABLE_NAME.to_owned(),
                        partial_field_def,
                        EVTX_SORT_FIELD.to_owned(),
                    ));
                }
                ParserType::srum => {
                    if is_parsed.contains("SRUM") {
                        continue;
                    }
                    is_parsed.insert("SRUM".to_owned());
                    for srum_table in srum_tables() {
                        let topic_name = full_topic_name(&self.client_context, &srum_table.topic);
                        let partial_field_def: Vec<(String, DataType)> = srum_table
                            .fields
                            .iter()
                            .map(|(name, dtype)| (name.to_string(), dtype.clone()))
                            .collect();
                        list.push(DataTopic::new(
                            topic_name,
                            srum_table.topic.to_owned(),
                            partial_field_def,
                            SRUM_SORT_FIELD.to_owned(),
                        ));
                    }
                }
                ParserType::hive { root_name: _ } => {
                    if is_parsed.contains(HIVE_TABLE_NAME) {
                        continue;
                    }
                    is_parsed.insert(HIVE_TABLE_NAME.to_owned());
                    let topic_name = full_topic_name(&self.client_context, HIVE_TABLE_NAME);
                    let partial_field_def = hive_fields();
                    list.push(DataTopic::new(
                        topic_name,
                        HIVE_TABLE_NAME.to_owned(),
                        partial_field_def,
                        HIVE_SORT_FIELD.to_owned(),
                    ));
                }
            }
        }
        Ok(list)
    }
}

///
/// Describe a topic
/// used to create kaka topics and clickhouse tables
///   
#[derive(Clone, Debug)]
pub struct DataTopic {
    pub topic_name: String,
    pub table_name: String,
    pub partial_field_def: Vec<(String, DataType)>,
    pub sort_field: String,
}
impl DataTopic {
    pub fn new(
        topic_name: String,
        table_name: String,
        partial_field_def: Vec<(String, DataType)>,
        sort_field: String,
    ) -> Self {
        Self {
            topic_name,
            table_name,
            partial_field_def,
            sort_field,
        }
    }
}

///
/// Data types used in the field definition
///
#[derive(Clone, Debug)]
pub enum DataType {
    String,
    Date,
    Int32,
    Int64,
    Uint8,
    Uint16,
    Float,
    Boolean,
}

///
/// Associate a parser to a regex that will filter input file names
///
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ParserConfig {
    #[serde(deserialize_with = "regex_deserializer")]
    #[serde(serialize_with = "regex_serializer")]
    pub file_filter: Regex,
    pub parser: ParserType,
}

///
/// a deserialiser for the Regex struct
///
fn regex_deserializer<'de, D>(deserializer: D) -> Result<Regex, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    Regex::new(s).map_err(D::Error::custom)
}

///
/// a serialiser for the Regex struct
///
fn regex_serializer<S>(regex: &Regex, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(regex.as_str())
}

const SAMPLE_CONFIGURATION: &str = r#"# Used as
# - a prefix for kafka topics 
# - a prefix for file names
# - the clickhouse database name
client_context: test

# Where the archives can be found
input_folder: input

# Indicate if we expect 7zip archives or decompressed folders
input_is_decompressed: false

# The .7zip achives will be decompressed in this folder
temp_folder: tmp

# Number of threads that will decompress the .7zip archives
# if set to 0, it will be defaulted to half the number of CPU 
decompression_threads: 0

# Number of threads that manages archives
# if set to 0, it will be defaulted to half the number of CPU 
archive_threads: 0

# Number of threads that parse the files
# if set to 0, it will be defaulted to half the number of CPU 
parsing_threads: 0

# Configure of the parsers
# the files name in the archive will be parsed with the file_filter regular expression to select the parser 
# available parser:
# - srum
# - csv
parsers:
- file_filter: SRUDB.*\.dat$
  parser: srum
- file_filter: test.*\.csv$
  parser: !csv
    # csv column mapping requires an additional configuration file (see data/ntfs_info.map.yaml)
    mapping_file: conf/test.yaml
    best_effort: true
    skip_lines: 0
# configure the output
output:
- type: file
  folder: output
- type: kafka
  params:
    bootstrap.servers: localhost:9092
    queue.buffering.max.ms: '100'
    queue.buffering.max.messages: '100000'
    compression.codec: lz4
- type: clickhouse
  server: localhost:8123
  login: default
# password:

"#;

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::*;

    #[test]
    fn regex() {
        let file_filter = Regex::new("SRUDB.*\\.dat$").unwrap();

        let pattern = "qsdqs_SRUDB_qsdqs.dat";
        assert!(file_filter.is_match(pattern));

        let pattern = "SRUDB.dat";
        assert!(file_filter.is_match(pattern));

        let pattern = "qsdqs_qsdqs.dat";
        assert!(!file_filter.is_match(pattern));

        let pattern = "qsdqs_SRUDB_qsdqs.dat.html";
        assert!(!file_filter.is_match(pattern));
    }

    #[test]
    fn conf() {
        let srum_parser = ParserConfig {
            file_filter: Regex::new("SRUDB.*\\.dat$").unwrap(),
            parser: ParserType::srum,
        };

        let csv_parser = ParserConfig {
            file_filter: Regex::new("test.*\\.dat$").unwrap(),
            parser: ParserType::csv {
                mapping_file: "conf/test.yaml".to_string(),
                best_effort: Some(true),
                skip_lines: Some(0),
            },
        };

        let hive_parser = ParserConfig {
            file_filter: Regex::new("hive.*\\.hive$").unwrap(),
            parser: ParserType::hive {
                root_name: "\\HKLM\\SAM".to_owned(),
            },
        };

        let evtx_parser = ParserConfig {
            file_filter: Regex::new(".evtx$").unwrap(),
            parser: ParserType::evtx,
        };

        // "\\HKLM\\SAM"
        let file_output = OutputConfig::file {
            folder: "output".to_string(),
        };

        let mut params = HashMap::new();
        params.insert("bootstrap.servers".to_owned(), "localhost:9092".to_owned());
        params.insert("queue.buffering.max.ms".to_owned(), "100".to_owned());
        params.insert(
            "queue.buffering.max.messages".to_owned(),
            "100000".to_owned(),
        );
        params.insert("compression.codec".to_owned(), "lz4".to_owned());
        let kafka_outptut = OutputConfig::kafka { params };

        let clickhouse_output = OutputConfig::clickhouse {
            server: "localhost:8123".to_owned(),
            login: Some("default".to_owned()),
            password: None,
        };

        let config = Configuration {
            client_context: "test".to_string(),
            input_folder: "input".to_string(),
            temp_folder: "tmp".to_string(),
            archive_threads: 0,
            parsing_threads: 0,
            decompression_threads: 0,
            input_is_decompressed: false,
            parsers: vec![srum_parser, csv_parser, hive_parser, evtx_parser],
            output: vec![file_output, kafka_outptut, clickhouse_output],
        };

        let serialized = serde_yml::to_string(&config).unwrap();
        println!("{serialized}");

        let conf = serde_yml::from_str::<Configuration>(&serialized).unwrap();
        assert_eq!("test", conf.client_context);
    }
}
