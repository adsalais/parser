use clap::Parser;

use clickhouse::Client;
use log::error;

use parser::{
    configuration::Configuration, errors::Error, writer::clickhouse_config::create_database,
};
use std::{fs, io, path::Path, process::exit};

const EXIT_FAILURE: i32 = 1;

///
/// Initialize Clickhouse by createing the database and the required tables
/// It is re-entrant, you can re-run any number of time
///
#[tokio::main]
async fn main() {
    initialize_logs();
    let args: CliArgs = CliArgs::parse();

    println!("Clickhouse Password:");
    let mut buffer = String::new();
    let stdin = io::stdin(); // We get `Stdin` here.

    stdin.read_line(&mut buffer).unwrap();
    let password = buffer.trim();

    if let Err(e) = run(args, password).await {
        error!("{e}");
        exit(EXIT_FAILURE);
    }
}

async fn run(args: CliArgs, password: &str) -> Result<(), Error> {
    let client = Client::default()
        .with_url(format!("http://{}", args.server))
        .with_user(args.login.unwrap_or("default".to_string()))
        .with_password(password)
        .with_option("enable_json_type", "1");

    let conf = Configuration::load(&args.configuration)?;
    let topics = conf.list_topics().unwrap();
    create_database(
        &client,
        &conf.client_context,
        &topics,
        &args.cluster,
        args.with_kafka,
        &args.kafka_server.unwrap_or("set_me".to_owned()),
        args.kafka_consumers.unwrap_or(1),
    )
    .await?;

    Ok(())
}

///
/// Initialize logs, creating a default configuration file if none present.
///
fn initialize_logs() {
    fs::create_dir_all("logs").unwrap();

    let log_config = "clickhouse_log4rs.yml";
    let log_path = Path::new(log_config);

    if !log_path.exists() {
        let default_config = "refresh_rate: 30 seconds
appenders:
  stdout:
    kind: console
  file:
    kind: rolling_file
    path: logs/clickhouse.log
    policy:
      trigger:
        kind: size
        limit: 10 mb
      roller:
        kind: fixed_window
        pattern: logs/clickhouse_{}.gz
        count: 5
        base: 1
    encoder:
      pattern: \"{d} {l} {t} - {m}{n}\"
root:
  level: info
  appenders:
    - file
    - stdout
";

        fs::write(log_path, default_config).unwrap();
    }
    log4rs::init_file(log_config, Default::default()).unwrap();
}

///
/// Initialize Clickhouse by createing the database and the required tables
/// It is re-entrant, you can re-run any number of time
///
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct CliArgs {
    /// The configuration file to parse
    #[arg(short, long, verbatim_doc_comment)]
    configuration: Option<String>,

    /// The clickhouse server. Example:localhost:8123
    #[arg(short, long, verbatim_doc_comment)]
    server: String,

    /// Login
    #[arg(short, long, verbatim_doc_comment)]
    login: Option<String>,

    /// The clickhouse cluster
    #[arg(short, long, verbatim_doc_comment)]
    cluster: Option<String>,

    /// Create the kafka related tables
    #[arg(long, verbatim_doc_comment)]
    with_kafka: bool,

    /// Kafka server. Example:localhost:9092
    /// requires --with-kafka
    #[arg(short, long, verbatim_doc_comment)]
    kafka_server: Option<String>,

    /// Number of kafka consumers
    /// requires --with-kafka
    #[arg(short, long, verbatim_doc_comment)]
    kafka_consumers: Option<usize>,
}
