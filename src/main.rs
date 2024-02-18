use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode, BaseConsumer};
use rdkafka::config::ClientConfig;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;

use serde_json::Value;
use chrono::Utc;
use log::{info, warn, error};
use tokio::signal;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicBool};
use std::time::Duration;
use reqwest;
use env_logger::Env;
use rumqttc::{AsyncClient, QoS, MqttOptions};
use tokio::task::JoinSet;
use tokio::task::{self, JoinHandle}; // For spawning blocking tasks
fn setup_logging() {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();
    //env_logger::init();
}

fn process_message(payload: &[u8]) {
    if let Ok(text) = std::str::from_utf8(payload) {
        if let Ok(json) = serde_json::from_str::<Value>(text) {
            let start_time = json["ts"].as_i64().unwrap_or(0);
            let end_time = Utc::now().timestamp_millis();
            let duration = end_time - start_time;

            // Use duration...
        }
    }
}

async fn start_kafka_consumer(should_stop: Arc<AtomicBool>,brokers: String, group_id: String, topic: String, partitions:Vec<i32>, split_id: i32) -> Result<(), Box<dyn std::error::Error>> {
    info!("Start Kafka Consumer {:?} on partitions {:?}", split_id, partitions);

    let consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        //.set("group.id", "")
        .set("bootstrap.servers", &brokers)
        .set("enable.auto.commit", "false")        
        .set("max.in.flight.requests.per.connection", 1.to_string())
        //.set("log_level", 0.to_string())
        //.set("debug", "consumer,cgrp,topic,fetch")
        .create()
        .expect("Consumer creation failed");
    // Convert Vec<String> to Vec<&str>
    //let topic_refs: Vec<&str> = topics.iter().map(AsRef::as_ref).collect();
    let mut topic_refs: Vec<&str> = Vec::new();
    topic_refs.push(&topic);
    //let topic_refs: Vec<&str> = topics.iter().map(AsRef::as_ref).collect();
    //consumer.subscribe(&topic_refs).expect("Can't subscribe to specified topics");
    if partitions.len() > 0 {
        let mut topic_map: HashMap<(String, i32), rdkafka::Offset> = HashMap::new();
        for partition in partitions {
            topic_map.insert((topic.clone(), partition), rdkafka::Offset::End);
        }
        let r = consumer.assign(&TopicPartitionList::from_topic_map(&topic_map).unwrap());
        info!("{:?}", r);
    } else {
        
        info!("Consuming {:?} - {:?}", &topic_refs, consumer.subscribe(&topic_refs));
    }

    info!("Assignment: {:?}", consumer.assignment());
    //let mut message_stream = consumer.stream();
    loop {
        if should_stop.load(Ordering::SeqCst) {
            warn!("Should stop");
            break
        }
        //let r = consumer.poll(Duration::from_secs(1)).expect("Poll failed");
        for msg_result in consumer.iter() {
            match msg_result {
                Err(e) => {
                    warn!("Kafka error: {}", e);
                    return Err(Box::new(e))
                },
                Ok(m) => {
                    let payload = match m.payload_view::<str>() {
                        None => "",
                        Some(Ok(s)) => s,
                        Some(Err(e)) => {
                            warn!("Error while deserializing message payload: {:?}", e);
                            ""
                        }
                    };                
                    info!("[{:?}] key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                        split_id, m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());                
                    //consumer.commit_message(&m, CommitMode::Async).unwrap();
                } 
            }
        }
    }
    Ok(())
}

async fn send_http_request(data: &str) {
    let client = reqwest::Client::new();
    let res = client.post("http://logger:8080/results")
                    .body(data.to_string())
                    .send()
                    .await;

    match res {
        Ok(response) => println!("Response: {}", response.status()),
        Err(e) => println!("HTTP Request failed: {}", e),
    }
}


async fn start_mqtt_client(broker: &str, port: u16, topic: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mqttoptions = MqttOptions::new("rust-mqtt-client", broker, port);
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    client.subscribe(topic, QoS::AtMostOnce).await?;

    loop {
        match eventloop.poll().await {
            Ok(notification) => {
                // Handle different types of notifications
                // For example, if you receive a publish, process the message
                if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish)) = notification {
                    process_message(&publish.payload);
                }
            }
            Err(e) => {
                // Handle error (e.g., connection loss, etc.)
                println!("Error: {:?}", e);
            }
        }
    }
}
fn java_string_hashcode(s: &str) -> i32 {
    let mut hash = 0i32;
    for c in s.chars() {
        hash = 31i32.wrapping_mul(hash) + (c as i32);
    }
    hash
} 
fn get_split_owner(topic: &str, partition: i32, num_readers: i32) -> i32 {
    let hash_code = java_string_hashcode(topic);
    let start_index = (hash_code.wrapping_mul(31) & 0x7FFFFFFF) as i32 % num_readers;
    (start_index + partition) % num_readers
}
pub fn get_partition_splitid(num_sources: i32, num_partitions: i32, virtual_partition: i32) -> i32 {
    //let mut p = 0;
    let mut s = 0;
    let mut map_partitions: HashMap<i32, i32> = HashMap::new();
    for vp in 0..(num_sources * num_partitions)  {


        let p = match map_partitions.get(&s) {
            Some(p) => {
                (p.clone() + 1) % num_partitions
            },
            None => 0
        };
        
        if vp == virtual_partition {
            return p
        }
        map_partitions.insert(s, p);
        s = (s + 1) % num_sources;
        
    }
    return -1
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_logging();
    info!("Starting Rust Subscriber");
    let target = env::var("TARGET").unwrap_or_else(|_| "Kafka".to_string());
    let host = env::var("HOST").unwrap_or_else(|_| "localhost".to_string());
    let port: u16 = env::var("PORT").unwrap_or_else(|_| "9094".to_string()).parse::<u16>().ok().unwrap();
    let nb_sources: i32 = env::var("NB_SOURCES").unwrap_or_else(|_| "2".to_string()).parse::<i32>().ok().unwrap();
    let nb_partitions: i32 = env::var("NB_PARTITIONS").unwrap_or_else(|_| "2".to_string()).parse::<i32>().ok().unwrap();
    let topic = env::var("TOPIC").unwrap_or_else(|_| "test".to_string());
    let group_id = env::var("GROUP_ID").unwrap_or_else(|_| "test".to_string());
    let all_partitions: Vec<i32> = 
        env::var("PARTITIONS")
            .unwrap_or_else(|_| "0,1,2,3".to_string())
            .split(",")
            .filter_map(|s| s.parse::<i32>().ok())
            .collect();
            
    let should_stop = Arc::new(AtomicBool::new(false));
    let mut handles = JoinSet::new();
    match target.as_str() {
        "MQTT" => {
            info!("Starting mqtt consumer");
            // Replace these with actual values or environment variables

            start_mqtt_client(&host, port, &topic).await?;
        }
        "Kafka" => {
            info!("Starting kafka consumer");
            let brokers = format!("{host}:{port}").to_string();
            /*
            for partition in &all_partitions {
                let brokers_clone = brokers.clone();
                let group_id_clone = group_id.clone();
                let topic_clone = topic.clone();
                let mut partitions: Vec<i32> = Vec::new();
                partitions.push(partition.clone());
                let stop_clone = should_stop.clone();
                let handle = handles.spawn(async move {
                    start_kafka_consumer(stop_clone, brokers_clone, group_id_clone, topic_clone, all_partitions).await.unwrap();
                });
                //handles.push(handle);
            } */
            for id_src in  0..nb_sources {
                let all_p = all_partitions.clone();
                let brokers_clone = brokers.clone();
                let group_id_clone = format!("{}-{}", group_id.clone(), id_src);
                let topic_clone = topic.clone();
                let mut partitions: Vec<i32> = Vec::new();
                for partition in all_p.iter() {
                    //let split_id = get_split_owner(&topic_clone, partition.clone(), nb_sources as i32);
                    //let split_id = get_partition_splitid(nb_sources, nb_partitions, partition.clone());
                    let split_id = partition % nb_sources;
                    if split_id == id_src { 
                        partitions.push(partition.clone());
                    }
                }
                let stop_clone = should_stop.clone();
                let handle = handles.spawn(async move {
                    start_kafka_consumer(stop_clone, brokers_clone, group_id_clone, topic_clone, partitions, id_src).await;
                });           
            }
            //start_kafka_consumer(brokers, group_id, topic, partitions).await?;
        }
        _ => println!("Unknown target {}", target),
    }
    let stop_clone = should_stop.clone();
    let ctrl_c_handle = tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        println!("Ctrl+C signal received, requesting shutdown...");
        stop_clone.store(true, Ordering::SeqCst);
    });

    // Await all spawned tasks and Ctrl+C future
    //future::try_join_all(handles).await?;//TODO: replace with JoinSet when Tokio is updated
    ctrl_c_handle.await?;
    handles.shutdown().await;
    // Await all spawned tasks
    /*
    for handle in handles {
        handle.await?;
    }*/
    Ok(())
}