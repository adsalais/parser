use std::{fs, io, path::Path, process::exit};

use ::parser::{configuration::Configuration, errors::Error, writer::kafka};
use clap::Parser;
use log::error;
use rdkafka::{ClientConfig, admin::AdminClient, config::FromClientConfig};
mod parser;

const EXIT_FAILURE: i32 = 1;
///
/// Kafka manager  
///  - create required topic based on the parser configuration file
///  - delete topics based on the parser configuration file
///  - list all topics
///
#[tokio::main]
async fn main() {
    initialize_logs();
    let args: CliArgs = CliArgs::parse();

    if let Err(e) = run(args).await {
        error!("{e}");
        exit(EXIT_FAILURE);
    }
}

async fn run(args: CliArgs) -> Result<(), Error> {
    let conf = Configuration::load(&args.configuration)?;

    let data_topics = conf.list_topics().unwrap();
    let topics: Vec<String> = data_topics
        .iter()
        .map(|topic| topic.topic_name.clone())
        .collect();

    match args.action {
        Action::delete => {
            println!(
                "//!!\\\\ You are about to delete every topics from client context '{}'",
                conf.client_context
            );
            println!("//!!\\\\ Enter the client context to confirm deletion");
            let mut buffer = String::new();
            let stdin = io::stdin(); // We get `Stdin` here.

            stdin.read_line(&mut buffer)?;
            let buffer = buffer.trim();
            if !buffer.eq(&conf.client_context) {
                println!(
                    "input '{}' does not match context '{}', exiting",
                    buffer, conf.client_context
                );
                exit(EXIT_FAILURE);
            }

            delete(topics, &args.kafka_brokers).await?;
        }
        Action::create => {
            create(
                topics,
                &args.partitions,
                &args.replication,
                &args.kafka_brokers,
            )
            .await?
        }
        Action::list_conf_topics => {
            for topic in topics {
                println!("{topic}");
            }
        }
        Action::list_server_topics => list_topics_on_server(&args.kafka_brokers).await?,
    }

    Ok(())
}

///
/// delete a list of topics, errors are written to logs
///
async fn delete(topics: Vec<String>, servers: &str) -> Result<(), Error> {
    let adminclient = AdminClient::from_config(
        ClientConfig::new().set("bootstrap.servers", servers.to_string()),
    )?;
    kafka::delete_topics(topics, &adminclient).await;

    Ok(())
}

///
/// Create a list of topics, errors are written to logs
///
async fn create(
    topics: Vec<String>,
    partitions: &Option<i32>,
    replication: &Option<i32>,
    servers: &str,
) -> Result<(), Error> {
    let adminclient = AdminClient::from_config(
        ClientConfig::new().set("bootstrap.servers", servers.to_string()),
    )?;
    let partitions = partitions.unwrap_or(30);
    let replication = replication.unwrap_or(1);
    kafka::create_topics(topics, partitions, replication, &adminclient).await;
    Ok(())
}

///
/// List every topics on the server
///
async fn list_topics_on_server(servers: &str) -> Result<(), Error> {
    let adminclient = AdminClient::from_config(
        ClientConfig::new().set("bootstrap.servers", servers.to_string()),
    )?;

    let topics = kafka::list_topics(&adminclient).await?;
    for (topic, partition) in topics {
        println!("topic:'{topic}' partitions:{partition}")
    }
    Ok(())
}

///
/// Kafka manager
///  - create required topic based on the parser configuration file
///  - delete topics based on the parser configuration file
///  - list all topics extracted from the configuration file
///
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct CliArgs {
    /// The configuration file to parse
    #[arg(short, long, verbatim_doc_comment)]
    configuration: Option<String>,

    /// The kafka bootstrap servers. Example:localhost:9092
    #[arg(short, long, verbatim_doc_comment)]
    kafka_brokers: String,

    /// Action to perform
    #[arg(short, long, verbatim_doc_comment)]
    action: Action,

    /// Number of partitions to create per kafka topics
    #[arg(short, long, verbatim_doc_comment)]
    partitions: Option<i32>,

    /// Replication factor for each topics
    #[arg(short, long, verbatim_doc_comment)]
    replication: Option<i32>,
}

#[derive(Clone, Debug, clap::ValueEnum)]
#[allow(non_camel_case_types)]
pub enum Action {
    delete,
    create,
    list_conf_topics,
    list_server_topics,
}

///
/// Initialize logs, creating a default configuration file if none present.
///
fn initialize_logs() {
    fs::create_dir_all("logs").unwrap();

    let log_config = "kafka_manager_log4rs.yml";
    let log_path = Path::new(log_config);

    if !log_path.exists() {
        let default_config = "refresh_rate: 30 seconds
appenders:
  stdout:
    kind: console
  file:
    kind: rolling_file
    path: logs/kafka_manager.log
    policy:
      trigger:
        kind: size
        limit: 10 mb
      roller:
        kind: fixed_window
        pattern: logs/kafka_manager_{}.gz
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
