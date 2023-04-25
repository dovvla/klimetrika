use std::collections::VecDeque;
use std::sync::Arc;

use kafka::producer::{Producer, Record, RequiredAcks};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};

fn get_time_from_reading(reading: &str) -> Option<String> {
    let read_values: Vec<String> = reading
        .split('\n')
        .into_iter()
        .map(|x| x.to_string())
        .collect();
    read_values
        .get(1)
        .map(|row| row.split_once('|').unwrap().0.to_string())
}

#[tokio::main]
async fn main() {
    let devices = reqwest::get("https://devel.klimerko.org/api/devices")
        .await
        .expect("klimerko unreachable")
        .text()
        .await
        .expect("resposne body");
    let devices: Vec<String> = devices
        .split('\n')
        .skip(1)
        .filter(|device| device.contains('|'))
        .map(|device| device.split_once('|').unwrap().0.to_string())
        .collect();

    let mut set = JoinSet::new();

    let message_queue = Arc::new(RwLock::new(VecDeque::<String>::new()));
    let publish_queue = message_queue.clone();

    set.spawn(async move {
        let mut producer = Producer::from_hosts(vec!["kafka:29092".to_owned()])
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
            .expect("kafka connection");
        loop {
            let mut queue = publish_queue.write().await;
            if let Some(reading) = (*queue).pop_front() {
                producer
                    .send(&Record::from_value("test-topic", reading.as_bytes()))
                    .unwrap();
                println!("{reading:} published!");
            };
            sleep(Duration::from_secs(2)).await;
        }
    });

    for device in devices {
        let queue = message_queue.clone();
        set.spawn(async move {
            let mut previous_time: Option<String> = None;
            let client = reqwest::Client::new();
            loop {
                sleep(Duration::from_millis(1000)).await;
                let reading = client
                    .get(format!(
                        "https://devel.klimerko.org/api/device/{device:}/latest"
                    ))
                    .send()
                    .await
                    .expect("klimerko unreachable")
                    .text()
                    .await
                    .expect("response body");
                let current_time = get_time_from_reading(&reading);

                match previous_time {
                    Some(ref pt) => {
                        if let Some(ct) = current_time {
                            if !pt.eq(&ct) {
                                previous_time = Some(ct.to_string());
                                queue.write().await.push_front(reading);
                            }
                        }
                    }
                    None => {
                        if let Some(ct) = current_time {
                            previous_time = Some(ct.to_string());
                            queue.write().await.push_front(reading);
                        }
                    }
                }
            }
        });
    }
    while let Some(_res) = set.join_next().await {}
}
