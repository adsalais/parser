use std::{collections::HashMap, fs, path::Path};

use serde::{Deserialize, Serialize};

use crate::{configuration::DataType, errors::Error};

use super::csv::{DateFormat, FieldType};

const DEFAULT_DELIMITER: char = ',';

///
/// Defines convertion for each field
/// If a field is not configured, the default value will be used
///
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CsvMapping {
    #[serde(default)]
    pub topic: String,

    #[serde(default)]
    pub sort_field: Option<String>,

    // Csv delimiter
    #[serde(default)]
    pub csv_delimiter: char,

    //Default date format
    #[serde(default)]
    pub default_date_format: DateFormat,

    //mapping for each field
    #[serde(default)]
    pub fields: HashMap<String, FieldType>,
}
impl Default for CsvMapping {
    fn default() -> Self {
        Self {
            topic: "".to_owned(),
            sort_field: None,
            csv_delimiter: DEFAULT_DELIMITER,
            default_date_format: DateFormat::Rfc3339,
            fields: Default::default(),
        }
    }
}
impl CsvMapping {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        if !path.as_ref().exists() {
            return Err(Error::CsvConfiguration(
                path.as_ref().to_string_lossy().to_string(),
            ));
        }
        let toml_file: String = fs::read_to_string(path)?;

        let conf = serde_yml::from_str::<CsvMapping>(&toml_file).unwrap();
        Ok(conf)
    }

    pub fn partial_fields(&self) -> Vec<(String, DataType)> {
        let mut res = Vec::with_capacity(self.fields.len());
        for (name, field_type) in &self.fields {
            match field_type {
                FieldType::String { .. } => res.push((name.to_owned(), DataType::String)),
                FieldType::Integer { .. } => res.push((name.to_owned(), DataType::Int64)),
                FieldType::Float { .. } => res.push((name.to_owned(), DataType::Float)),
                FieldType::Date { .. } => res.push((name.to_owned(), DataType::String)),
            }
        }
        res
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::collections::HashMap;

    #[test]
    fn mapping() {
        let mut field = HashMap::new();

        field.insert(
            "integer".to_string(),
            FieldType::Integer { mandatory: true },
        );

        field.insert(
            "Some float".to_string(),
            FieldType::Float { mandatory: true },
        );

        field.insert(
            "aString".to_string(),
            FieldType::String { mandatory: false },
        );

        field.insert(
            "date".to_string(),
            FieldType::Date {
                mandatory: false,
                input_date_format: Some(DateFormat::Rfc3339),
            },
        );

        let conf = CsvMapping {
            topic: "mapping_test".to_owned(),
            fields: field,
            default_date_format: DateFormat::Pattern("%Y-%m-%d %H:%M:%S.%3f".to_owned(), false),

            ..Default::default()
        };

        let yaml = serde_yml::to_string(&conf).unwrap();
        println!("{yaml}");
        let deser: CsvMapping = serde_yml::from_str(&yaml).unwrap();

        assert!(deser.fields.contains_key("date"));
        assert!(deser.fields.contains_key("aString"));
        assert!(deser.fields.contains_key("Some float"));
        assert!(deser.fields.contains_key("integer"));
    }

    ///
    /// le mapping pour le fichier du challenge
    ///
    pub const TEST_MAPPING: &str = r#"

# csv delimiter
csv_delimiter: ','

#
# default date format 
#
default_date_format: !Pattern
- '%Y-%m-%d %H:%M:%S.%3f'
- false

#
# Définis les types de champs
#
fields:
  ComputerName:
    type: String
    mandatory: true

  VolumeID:
    type: String
    mandatory: true

  SizeInBytes:
    type: Integer

  CreationDate:
    type: Date
    mandatory: true

  LastModificationDate:
    type: Date

  LastAccessDate:
    type: Date

  LastAttrChangeDate:
    type: Date

  FileNameCreationDate:
    type: Date

  FileNameLastModificationDate:
    type: Date

  FileNameLastAccessDate:
    type: Date

  FileNameLastAttrModificationDate:
    type: Date

"#;

    #[test]
    fn test_default_config_yaml() {
        let ser = serde_yml::from_str::<CsvMapping>(TEST_MAPPING).unwrap();
        assert!(ser.fields.contains_key("ComputerName"));
        assert!(ser.fields.contains_key("FileNameLastAccessDate"));
    }
}
