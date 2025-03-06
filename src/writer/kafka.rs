use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use log::{error, info};
use rdkafka::{
    ClientConfig, ClientContext,
    admin::{
        AdminClient, AdminOptions, NewTopic, OwnedResourceSpecifier, ResourceSpecifier,
        TopicReplication,
    },
    client::DefaultClientContext,
    error::KafkaError,
    message::DeliveryResult,
    producer::{BaseRecord, Producer, ProducerContext, ThreadedProducer},
    types::RDKafkaErrorCode,
};

use crate::{
    Error,
    output::{OutputWriter, Tuple},
};

///
/// Write output to a kafka topic
///
pub struct KafkaWriter {
    producer: ThreadedProducer<InputDeliveryCallback>,
    topic: String,
    has_error: Arc<AtomicBool>,
    num_rows: usize,
}
impl KafkaWriter {
    pub fn new(topic: &str, params: &HashMap<String, String>) -> Result<KafkaWriter, Error> {
        let mut config = params
            .iter()
            .fold(ClientConfig::new(), |mut config, (key, value)| {
                config.set(key, value);
                config
            });
        //forbid the creation of a topic if it does not exists
        config.set("allow.auto.create.topics".to_string(), "false".to_string());

        let input_callback = InputDeliveryCallback::new();
        let has_error = input_callback.has_error.clone();
        let producer: ThreadedProducer<InputDeliveryCallback> =
            config.create_with_context(input_callback)?;

        //verify that the topic exisst
        let metadata = producer
            .client()
            .fetch_metadata(Some(topic), Duration::from_secs(2))?;
        if metadata.topics().is_empty() || metadata.topics()[0].partitions().is_empty() {
            return Err(Error::KafkaUnknownTopic(topic.to_owned()));
        }

        Ok(Self {
            producer,
            topic: topic.to_owned(),
            has_error,
            num_rows: 0,
        })
    }
}
impl OutputWriter for KafkaWriter {
    fn write(&mut self, data: Tuple) -> Result<(), Error> {
        if self.has_error.load(Ordering::Relaxed) {
            return Err(Error::KafkaProducer());
        }
        let key = data.key;
        let line = data.to_json_string()?;
        loop {
            let record = BaseRecord::to(&self.topic).payload(&line).key(&key);
            match self.producer.send(record) {
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                    self.producer.poll(Duration::from_millis(10));
                    continue;
                }
                Err((e, _)) => {
                    return Err(e.into());
                }
                Ok(_) => break,
            }
        }
        self.num_rows += 1;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        const FLUSH_TIMEOUT: u64 = 30;
        self.producer.flush(Duration::from_secs(FLUSH_TIMEOUT))?;
        Ok(())
    }

    fn num_rows(&self) -> usize {
        self.num_rows
    }
}

///
/// Callback to properly log errors and stop the output writer as soon as an error occurs
///
struct InputDeliveryCallback {
    has_error: Arc<AtomicBool>,
}
impl InputDeliveryCallback {
    fn new() -> Self {
        Self {
            has_error: Arc::new(AtomicBool::new(false)),
        }
    }
}
impl ClientContext for InputDeliveryCallback {}
impl ProducerContext for InputDeliveryCallback {
    type DeliveryOpaque = ();

    fn delivery(&self, r: &DeliveryResult, _: Self::DeliveryOpaque) {
        if let Err((e, _)) = r {
            // Only log the first error to avoid flooding the log file
            // It is ok to do so because the writer will stop after one error
            if !self.has_error.load(Ordering::Relaxed) {
                self.has_error.store(true, Ordering::Relaxed);
                error!("Delivery error {e}");
            }
        }
    }
}

///
/// Create a list of topic
///
pub async fn create_topics(
    topic_names: Vec<String>,
    partitions: i32,
    replication: i32,
    client: &AdminClient<DefaultClientContext>,
) {
    let mut topics = Vec::with_capacity(topic_names.capacity());

    for name in &topic_names {
        topics.push(NewTopic::new(
            name,
            partitions,
            TopicReplication::Fixed(replication),
        ));
    }

    let admin_options = AdminOptions::new();
    let result = client.create_topics(topics.iter(), &admin_options).await;

    match result {
        Ok(results) => {
            for res in results {
                match res {
                    Ok(topic) => info!("topic '{topic}' created"),
                    Err((topic, e)) => error!("topic '{topic}' creation failed: {e}"),
                }
            }
        }
        Err(e) => error!("Topic creation failed {e}"),
    }
}

///
/// Delete a list of topic
///
pub async fn delete_topics(topic_names: Vec<String>, client: &AdminClient<DefaultClientContext>) {
    let mut topics = Vec::with_capacity(topic_names.capacity());

    for name in &topic_names {
        topics.push(name.as_str());
    }

    let admin_options = AdminOptions::new();
    let result = client.delete_topics(&topics, &admin_options).await;

    match result {
        Ok(results) => {
            for res in results {
                match res {
                    Ok(topic) => info!("topic '{topic}' deleted"),
                    Err((topic, e)) => error!("topic '{topic}' delete failed: {e}"),
                }
            }
        }
        Err(e) => error!("Topic deletetion failed {e}"),
    }
}

pub struct TopicMetadata {
    pub name: String,
    pub meta: Vec<(String, String)>,
}
///
/// Describe a list of topic
///
pub async fn describe_topics(
    topic_names: Vec<String>,
    client: &AdminClient<DefaultClientContext>,
) -> Result<Vec<TopicMetadata>, Error> {
    let mut ressources = Vec::with_capacity(topic_names.capacity());

    for name in &topic_names {
        ressources.push(ResourceSpecifier::Topic(name));
    }

    let admin_options = AdminOptions::new();
    let ressources = client
        .describe_configs(ressources.iter(), &admin_options)
        .await?;

    let mut result = Vec::new();
    for res in ressources {
        match res {
            Ok(config) => {
                if let OwnedResourceSpecifier::Topic(name) = config.specifier {
                    let mut meta = Vec::new();
                    for entry in config.entries {
                        meta.push((entry.name, entry.value.unwrap_or("".to_string())));
                    }
                    result.push(TopicMetadata { name, meta });
                }
            }
            Err(e) => return Err(Error::KafkaRessource(e.to_string())),
        }
    }
    Ok(result)
}

///
/// List all topics
///
pub async fn list_topics(
    client: &AdminClient<DefaultClientContext>,
) -> Result<Vec<(String, usize)>, Error> {
    let metadata = client
        .inner()
        .fetch_metadata(None, Duration::from_secs(2))?;

    let mut res = Vec::new();
    for topic in metadata.topics() {
        res.push((topic.name().to_string(), topic.partitions().len()));
    }
    Ok(res)
}

#[cfg(test)]
mod tests {

    use rdkafka::config::FromClientConfig;

    use crate::{init_log, output::Fields};

    use super::*;

    #[tokio::test]
    async fn send_data() {
        init_log();

        let mut params = HashMap::new();
        params.insert("queue.buffering.max.ms".to_string(), "100".to_string());
        params.insert(
            "queue.buffering.max.messages".to_string(),
            "100000".to_string(),
        );
        params.insert(
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        params.insert("compression.codec".to_string(), "lz4".to_string());
        params.insert("allow.auto.create.topics".to_string(), "false".to_string());

        let adminclient = AdminClient::from_config(
            ClientConfig::new().set("bootstrap.servers", "localhost:9092".to_string()),
        )
        .unwrap();
        let topic_name = "testtopic";
        delete_topics(vec![topic_name.to_string()], &adminclient).await;

        // topic does not exists yet
        let writer = KafkaWriter::new("testtopic", &params.clone());
        assert!(writer.is_err());

        let partitions = 3;
        create_topics(vec![topic_name.to_string()], partitions, 1, &adminclient).await;
        let mut writer = KafkaWriter::new("testtopic", &params.clone()).unwrap();

        let fields = Fields::new(
            "mymachine",
            "c:\\system32\\SRUDB.dat",
            "mymachine_ORC.7z",
            "SRUDB.dat",
        );
        let tuple = Tuple::new(&fields);

        writer.write(tuple).unwrap();
        writer.flush().unwrap();

        let topics = list_topics(&adminclient).await.unwrap();
        let mut found = false;
        for (topic, len) in topics {
            if topic.eq(topic_name) {
                assert!(len > 0);
                found = true;
            }
        }
        assert!(found);

        let topic = describe_topics(vec!["testtopic".to_string()], &adminclient)
            .await
            .unwrap();
        assert_eq!(1, topic.len());

        delete_topics(vec![topic_name.to_string()], &adminclient).await;

        let topics = list_topics(&adminclient).await.unwrap();
        let mut found = false;
        for (topic, len) in topics {
            if topic.eq(topic_name) {
                assert!(len == 0);
                found = true;
            }
        }
        assert!(!found);
    }
}
