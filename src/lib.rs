pub mod archive_parser;
pub mod configuration;
pub mod errors;
pub mod input;
pub mod output;
pub use errors::Error;
pub mod writer;
#[cfg(test)]
struct SimpleLogger;

#[cfg(test)]
use log::{Level, Metadata, Record};

#[cfg(test)]
static LOGGER: SimpleLogger = SimpleLogger;
#[cfg(test)]
impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info && !metadata.target().eq("evtx::evtx_chunk")
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}
#[cfg(test)]
use log::LevelFilter;
#[cfg(test)]
pub fn init_log() {
    let _ = log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Info));
}

#[cfg(test)]
mod tests {

    use regex::Regex;

    use crate::{
        archive_parser,
        configuration::{Configuration, ParserConfig, ParserType},
        init_log,
        output::OutputConfig,
        writer::clickhouse_config,
    };

    #[tokio::test]
    async fn end_to_end() {
        init_log();

        let srum_filter = Regex::new("SRUDB.*\\.dat$").unwrap();
        let srum_config = ParserConfig {
            file_filter: srum_filter,
            parser: ParserType::srum,
        };

        let evtx_filter = Regex::new("evtx$").unwrap();
        let evtx_config = ParserConfig {
            file_filter: evtx_filter,
            parser: ParserType::evtx,
        };

        let hive_sam = Regex::new("SAM$").unwrap();
        let hive_sam_config = ParserConfig {
            file_filter: hive_sam,
            parser: ParserType::hive {
                root_name: "\\HKLM\\SAM".to_owned(),
            },
        };

        let clickhouse_config = OutputConfig::clickhouse {
            server: "localhost:8123".to_owned(),
            login: None,
            password: None,
        };

        let context = "test_lib_end_to_end";
        let configuration = Configuration {
            client_context: context.to_owned(),
            input_folder: "data/archive/".to_string(),
            input_is_decompressed: false,
            temp_folder: "data/temp/".to_string(),
            archive_threads: 0,
            parsing_threads: 0,
            decompression_threads: 0,
            parsers: vec![srum_config, evtx_config, hive_sam_config],
            output: vec![clickhouse_config],
        };

        //
        //  Create clickhouse tables
        //
        let clickhouse_client = clickhouse::Client::default()
            .with_user("default")
            .with_url("http://localhost:8123")
            .with_option("enable_json_type", "1");

        let data_topics = configuration.list_topics().unwrap();

        clickhouse_config::create_database(
            &clickhouse_client,
            &configuration.client_context,
            &data_topics,
            &None,
            false,
            "",
            0,
        )
        .await
        .unwrap();
        archive_parser::parse(configuration).unwrap();
    }
}
