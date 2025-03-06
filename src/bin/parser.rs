use std::{fs, path::Path, process::exit};

use clap::Parser;
use log::error;
use parser::{archive_parser::parse, configuration::Configuration, errors::Error};

// Use the jemallocator if possible
#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

///
/// Entry point of the parser
///
/// Read archives and convert files in JSONL that can be written to disk or send directly to kafka
///
#[allow(dead_code)]
fn main() {
    initialize_logs();
    let args: CliArgs = CliArgs::parse();
    const EXIT_FAILURE: i32 = 1;
    if let Err(e) = run(args) {
        error!("{e}");
        exit(EXIT_FAILURE);
    }
}

fn run(args: CliArgs) -> Result<(), Error> {
    let conf = Configuration::load(&args.configuration)?;
    parse(conf)?;
    Ok(())
}

///
/// Read archives and convert files in JSONL that can be written to disk or send directly to kafka
///
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct CliArgs {
    /// The configuration file
    #[arg(short, long, verbatim_doc_comment)]
    configuration: Option<String>,
}

///
/// Initialize logs, creating a default configuration file if none present.
///
fn initialize_logs() {
    fs::create_dir_all("logs").unwrap();

    let log_config = "parser_log4rs.yml";
    let log_path = Path::new(log_config);

    if !log_path.exists() {
        let default_config = "refresh_rate: 30 seconds
appenders:
  stdout:
    kind: console
  file:
    kind: rolling_file
    path: logs/parser.log
    policy:
      trigger:
        kind: size
        limit: 10 mb
      roller:
        kind: fixed_window
        pattern: logs/parser_{}.gz
        count: 5
        base: 1
    encoder:
      pattern: \"{d} {l} {t} - {m}{n}\"
root:
  level: info
  appenders:
    - file
    - stdout
loggers:
  evtx::evtx_chunk:
    level: off
";

        fs::write(log_path, default_config).unwrap();
    }
    log4rs::init_file(log_config, Default::default()).unwrap();
}
