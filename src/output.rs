use std::{
    collections::HashMap,
    fs::{self},
    hash::{DefaultHasher, Hash, Hasher},
    path::PathBuf,
};

use chrono::{DateTime, Utc};
use log::error;
use zerocopy::IntoBytes;

use crate::{
    Error,
    writer::{clickhouse_writer::ClickhouseWriter, file_writer::FileWriter, kafka::KafkaWriter},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as enc64};

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value, json};

pub const OUTPUT_DATE_FORMAT_UTC: &str = "%Y-%m-%d %H:%M:%S.%3f";
///
/// Trait implemented by all writers
///
pub trait OutputWriter {
    ///
    /// write a single line to the ouput
    ///
    fn write(&mut self, data: Tuple) -> Result<(), Error>;

    ///
    /// called at the end of a parsing to ensure that the last data is writen or sent
    ///
    fn flush(&mut self) -> Result<(), Error>;
}

///
/// Create output writers from a list of output config
///
pub struct Output {
    pub list: Vec<Box<dyn OutputWriter>>,
    pub num_rows: usize,
}
impl Output {
    pub fn new(
        output_config: &[OutputConfig],
        archive_name: &str,
        context: &str,
        topic: &str,
    ) -> Result<Self, Error> {
        let mut list = vec![];
        for o in output_config {
            list.push(o.build(archive_name, context, topic)?);
        }
        Ok(Self { list, num_rows: 0 })
    }

    ///
    /// Write provided data to all writers.
    /// Returns an error if any writer fail
    ///
    pub fn write(&mut self, data: Tuple) -> Result<(), Error> {
        //avoid a clone if there is only one output
        if self.list.len() == 1 {
            self.list[0].write(data)?;
        } else {
            for writer in &mut self.list {
                writer.write(data.clone())?;
            }
        }
        self.num_rows += 1;
        Ok(())
    }

    ///
    /// Flush data
    /// Ensure that it is always called for every writer
    ///
    pub fn flush(&mut self) -> Result<(), Error> {
        let mut in_error = false;
        for writer in &mut self.list {
            if let Err(e) = writer.flush() {
                error!("Error while flushing output: {e}");
                in_error = true;
            }
        }
        if in_error {
            Err(Error::OutputFlush())
        } else {
            Ok(())
        }
    }

    ///
    /// returns the number of rows written to one writer
    ///
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}
impl Drop for Output {
    fn drop(&mut self) {
        //flush data before dropping the object
        if let Err(e) = self.flush() {
            error!("{e}")
        }
    }
}

///
/// Configuration for the supported output
///
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
#[allow(non_camel_case_types)]
pub enum OutputConfig {
    file {
        folder: String,
    },
    clickhouse {
        server: String,
        login: Option<String>,
        password: Option<String>,
    },
    kafa {
        params: HashMap<String, String>,
    },
}
impl OutputConfig {
    pub fn build(
        &self,
        archive_name: &str,
        context: &str,
        topic: &str,
    ) -> Result<Box<dyn OutputWriter>, Error> {
        match self {
            OutputConfig::file { folder } => {
                let mut path: PathBuf = folder.into();
                path.push(archive_name);
                fs::create_dir_all(&path)?;
                let file_name = &full_topic_name(context, topic);
                path.push(format!("{file_name}.jsonl"));
                let writer = FileWriter::new(&path)?;
                Ok(Box::new(writer))
            }
            OutputConfig::clickhouse {
                server,
                login,
                password,
            } => {
                let writer = ClickhouseWriter::new(server, &login, &password, context, topic)?;
                Ok(Box::new(writer))
            }
            OutputConfig::kafa { params } => {
                let topic_name = &full_topic_name(context, topic);
                let writer = KafkaWriter::new(topic_name, params)?;
                Ok(Box::new(writer))
            }
        }
    }
}

///
/// topic name used for file name and kafka topics
///
pub fn full_topic_name(client_context: &str, name: &str) -> String {
    format!("{client_context}_{name}")
}

///
/// The metadata fields common to every tuples
///
pub const FIELD_ID: &str = "id";
pub const FIELD_IMPORT_DATE: &str = "import_date";
pub const FIELD_COMPUTER: &str = "computer";
pub const FIELD_ORIGINAL: &str = "original_file";
pub const FIELD_ARCHIVE: &str = "archive_name";
pub const FIELD_DATA: &str = "data";

///
/// Fields common to every tuples
/// The key is used by kafa to ensure that everything tuple of a file are sent to the same partition
///
pub struct Fields {
    pub key: [u8; 2],
    pub machine_id: String,
    pub original_file: String,
    pub archive_name: String,
    pub archive_file: String,
}
impl Fields {
    pub fn new(
        machine_id: &str,
        original_file: &str,
        archive_name: &str,
        archive_file: &str,
    ) -> Self {
        let mut hasher = DefaultHasher::new();
        machine_id.hash(&mut hasher);
        original_file.hash(&mut hasher);
        let hash = hasher.finish();
        let key = hash.as_bytes()[0..2].try_into().unwrap();
        Self {
            key,
            machine_id: machine_id.to_owned(),
            original_file: original_file.to_owned(),
            archive_name: archive_name.to_owned(),
            archive_file: archive_file.to_owned(),
        }
    }
}

///
/// A data entry built with shared metadata fields
///
#[derive(Clone)]
pub struct Tuple {
    pub key: [u8; 2],
    pub id: String,
    pub import_date: DateTime<Utc>,
    pub computer: String,
    pub original_file: String,
    pub archive_name: String,
    pub archive_file: String,
    pub data: Option<Value>,
    pub sort_data: Option<i64>,
}
impl Tuple {
    pub fn new(fields: &Fields) -> Self {
        let import_date = Utc::now();

        Self {
            key: fields.key,
            id: "".to_owned(),
            import_date,
            computer: fields.machine_id.to_owned(),
            original_file: fields.original_file.to_owned(),
            archive_name: fields.archive_name.to_owned(),
            archive_file: fields.archive_file.to_owned(),
            data: None,
            sort_data: None,
        }
    }

    ///
    /// Add data to the tuple, this can only performed once
    /// sort_field is used as the most signicant bytes of the id
    ///
    pub fn set_data(&mut self, data: Value, sort_data: Option<i64>) -> Result<(), Error> {
        if self.data.is_some() {
            return Err(Error::DataField());
        }

        if !data.is_object() {
            return Err(Error::JsonNotAndObject("data".to_owned()));
        }

        self.data = Some(data);
        self.sort_data = sort_data;
        self.id = self.compute_id();

        Ok(())
    }

    ///
    /// Compute a rentrant 128bits by aggregating two arrays
    ///     - most significant bytes: cryptographic hash of the original file name.  Ensures that data is sorted by file in the clickhouse tables
    ///     - Least significant bytes: cryptographic hash of the rest of the data to ensure unicity
    ///
    fn compute_id(&self) -> String {
        let mut hasher = blake3::Hasher::new();

        if let Some(data) = &self.data {
            //the content is allready checked in the set_data()
            let map = data.as_object().unwrap();
            Self::hash_json_map(&mut hasher, map);
        }
        hasher.update(self.computer.as_bytes());
        hasher.update(self.original_file.as_bytes());
        let hash = hasher.finalize();

        let mut uid = [0; 16];
        match self.sort_data {
            Some(field) => {
                let (one, two) = uid.split_at_mut(8);
                let key = &hash.as_bytes()[0..8];

                one.copy_from_slice(&field.as_bytes());
                two.copy_from_slice(&key);
            }
            None => {
                uid = hash.as_bytes()[0..16].try_into().unwrap();
            }
        }
        enc64.encode(&uid)
    }

    ///
    /// consume the object to prevent future reuse
    ///
    pub fn to_json_string(self) -> Result<String, Error> {
        let mut tuple = serde_json::Map::new();

        tuple.insert(
            FIELD_IMPORT_DATE.to_string(),
            Value::String(self.import_date.format(OUTPUT_DATE_FORMAT_UTC).to_string()),
        );

        tuple.insert(FIELD_COMPUTER.to_string(), Value::String(self.computer));
        tuple.insert(
            FIELD_ORIGINAL.to_string(),
            Value::String(self.original_file.to_owned()),
        );
        tuple.insert(
            FIELD_ARCHIVE.to_string(),
            Value::String(self.archive_name.to_owned()),
        );

        tuple.insert(FIELD_ID.to_string(), json!(self.id.to_string()));

        if let Some(val) = self.data {
            tuple.insert(FIELD_DATA.to_string(), val);
        } else {
            tuple.insert(
                FIELD_DATA.to_string(),
                Value::Object(serde_json::Map::new()),
            );
        }

        let s = serde_json::to_string(&Value::Object(tuple))?;
        Ok(s)
    }

    pub fn hash_json_map(hasher: &mut blake3::Hasher, map: &serde_json::Map<String, Value>) {
        for (key, value) in map {
            hasher.update(key.as_bytes());
            Self::hash_json_value(hasher, value);
        }
    }

    pub fn hash_number(hasher: &mut blake3::Hasher, num: &Number) {
        if num.is_f64() {
            let num = num.as_f64().unwrap();
            hasher.update(&num.to_le_bytes());
        } else if num.is_i64() {
            let num = num.as_i64().unwrap();
            hasher.update(&num.to_le_bytes());
        } else if num.is_u64() {
            let num = num.as_u64().unwrap();
            hasher.update(&num.to_le_bytes());
        }
    }

    pub fn hash_json_value(hasher: &mut blake3::Hasher, value: &Value) {
        match value {
            Value::Bool(v) => {
                hasher.update(v.as_bytes());
            }
            Value::Number(number) => Self::hash_number(hasher, number),
            Value::String(v) => {
                hasher.update(v.as_bytes());
            }
            Value::Array(values) => Self::hash_json_array(hasher, values),
            Value::Object(map) => Self::hash_json_map(hasher, map),
            Value::Null => {}
        }
    }

    fn hash_json_array(hasher: &mut blake3::Hasher, js_array: &Vec<Value>) {
        for value in js_array {
            Self::hash_json_value(hasher, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rand::{TryRngCore, rngs::OsRng};
    use serde_json::{Value, json};

    use crate::output::{
        FIELD_ARCHIVE, FIELD_COMPUTER, FIELD_DATA, FIELD_ID, FIELD_IMPORT_DATE, FIELD_ORIGINAL,
    };

    use super::{Fields, Tuple};

    #[test]
    fn unique_id() {
        let fields = Fields::new(
            "machine_id",
            "original_file",
            &format!("archive_name_{}", OsRng.try_next_u32().unwrap()),
            "archive_file",
        );

        let mut tuple = Tuple::new(&fields);
        let sort_data = OsRng.try_next_u64().unwrap() as i64;
        let mut hashset = HashSet::new();

        tuple
            .set_data(Value::Object(serde_json::Map::new()), Some(sort_data))
            .unwrap();

        hashset.insert(tuple.id.to_string());

        let expected_header = &tuple.id[0..8];
        for i in 0..100 {
            let s = OsRng.try_next_u32().unwrap();
            let mut data = serde_json::Map::new();
            data.insert("i".to_string(), json!(i));
            data.insert("rd".to_string(), json!(s));
            let mut tuple = Tuple::new(&fields);
            tuple
                .set_data(Value::Object(data), Some(sort_data))
                .unwrap();
            let val = tuple.id;

            assert!(!hashset.contains(&val));
            hashset.insert(val.clone());

            //  println!("{val}");
            assert_eq!(expected_header, &val[0..8]);
        }
    }

    #[test]
    fn unique_id_stabilility() {
        let fields = Fields::new(
            "machine",
            "original",
            &format!("archive_name_{}", OsRng.try_next_u32().unwrap()),
            &format!("archive_file_{}", OsRng.try_next_u32().unwrap()),
        );

        let mut tuple = Tuple::new(&fields);
        let sort_data = 122324;
        let mut data = serde_json::Map::new();
        data.insert("i".to_string(), json!(1.87));
        data.insert("rd".to_string(), json!("test"));
        tuple
            .set_data(Value::Object(data), Some(sort_data))
            .unwrap();

        assert_eq!("1N0BAAAAAAAb3tT1XT2sSw", tuple.id);
    }

    #[test]
    fn to_json_string() {
        let fields = Fields::new(
            "machine",
            "original",
            &format!("archive_name_{}", OsRng.try_next_u32().unwrap()),
            &format!("archive_file_{}", OsRng.try_next_u32().unwrap()),
        );

        let mut tuple = Tuple::new(&fields);
        let sort_data = 122324;
        let mut data = serde_json::Map::new();
        data.insert("i".to_string(), json!(1.87));
        data.insert("rd".to_string(), json!("test"));
        tuple
            .set_data(Value::Object(data), Some(sort_data))
            .unwrap();
        let json = tuple.to_json_string().unwrap();

        let v: Value = serde_json::from_str(&json).unwrap();

        let map = v.as_object().unwrap();
        assert!(map.contains_key(FIELD_ARCHIVE));
        assert!(map.contains_key(FIELD_COMPUTER));
        assert!(map.contains_key(FIELD_DATA));
        assert!(map.contains_key(FIELD_ID));
        assert!(map.contains_key(FIELD_ORIGINAL));
        assert!(map.contains_key(FIELD_IMPORT_DATE));

        let data = map.get(FIELD_DATA).unwrap().as_object().unwrap();
        assert_eq!(1.87, data.get("i").unwrap().as_f64().unwrap());
        assert_eq!("test", data.get("rd").unwrap().as_str().unwrap());
    }
}
