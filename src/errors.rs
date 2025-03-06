use thiserror::Error;
///
/// Les différentes erreurs qui peuvent être retournées par l'application
///
#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Clickhouse(#[from] clickhouse::error::Error),

    #[error(transparent)]
    EvtxError(#[from] evtx::err::EvtxError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),

    #[error(transparent)]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error(transparent)]
    NtHive(#[from] nt_hive::NtHiveError),

    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),

    #[error(transparent)]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error(transparent)]
    ParseDate(#[from] chrono::ParseError),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error(transparent)]
    TokioOneShotReceive(#[from] tokio::sync::oneshot::error::RecvError),

    #[error("Configuration file does not exist {0}")]
    Configuration(String),

    #[error("data field already set")]
    DataField(),

    #[error("{0}")]
    Generic(String),

    #[error("An error occured while writing data to Kafka")]
    KafkaProducer(),

    #[error("Kafka topic '{0}' does not exists")]
    KafkaUnknownTopic(String),

    #[error("Kafka ressource error '{0}'")]
    KafkaRessource(String),

    #[error("Some error occured while flushing the output")]
    OutputFlush(),

    #[error("Ordering field '{0}' for table '{1}' is not defined in the partial_field_definition")]
    OrderingFieldNotDefined(String, String),

    #[error("JSON value is not an object: '{0}'")]
    JsonNotAndObject(String),

    #[error("JSON value is not a String: '{0}'")]
    JsonNotaString(String),

    #[error("Invalid event: {0}")]
    Evtx(String),

    #[error(
        "for table:'{0}', sort field: {1}, is not defined the partial_field_def field definition"
    )]
    ClickhouseSortField(String, String),
}
