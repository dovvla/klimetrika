use kafka::producer::{Producer, Record, RequiredAcks};
use rand::seq::SliceRandom;
use serde_json::json;
use std::collections::HashMap;
use tokio::sync::mpsc;
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

fn reading_to_json(reading: &str, device: &str) -> String {
    let read_values: Vec<String> = reading
        .split('\n')
        .into_iter()
        .map(|x| x.to_string())
        .collect();

    let mut map: HashMap<&str, &str> = read_values
        .get(0)
        .expect("first row")
        .split('|')
        .zip(read_values.get(1).expect("second row").split('|'))
        .collect();

    map.insert("deviceId", device);

    map.insert(
        "city",
        (vec![
            "Novi Sad",
            "Beograd",
            "Nis",
            "Subotica",
            "Valjevo",
            "Kragujevac",
        ])
        .choose(&mut rand::thread_rng())
        .unwrap_or(&"Tovarisevo"),
    );

    let ret_val = json!(
        {
            "deviceId": map.get("deviceId"),
            "city": map.get("city"),
            "time": map.get("time").unwrap().parse::<i128>().unwrap(),
            "temperature": map.get("temperature").unwrap().parse::<f64>().unwrap(),
            "humidity": map.get("humidity").unwrap().parse::<f64>().unwrap(),
            "pressure": map.get("pressure").unwrap().parse::<f64>().unwrap(),
            "pm1": map.get("pm1").unwrap().parse::<f64>().unwrap(),
            "pm2": map.get("pm2").unwrap().parse::<f64>().unwrap(),
            "pm10": map.get("pm10").unwrap().parse::<f64>().unwrap(),
        }
    );

    serde_json::to_string(&ret_val).unwrap()
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

    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let mut set = JoinSet::new();
    let mut producer = Producer::from_hosts(vec!["kafka:29092".to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()
        .expect("kafka connection");

    set.spawn(async move {
        while let Some(reading) = rx.recv().await {
            producer
                .send(&Record::from_value("test-topic", reading.as_bytes()))
                .unwrap();
            println!("{reading:} published!");
            sleep(Duration::from_secs(2)).await;
        }
    });

    for device in devices {
        let queue = tx.clone();
        set.spawn(async move {
            let mut previous_time: Option<String> = None;
            let client = reqwest::Client::new();
            loop {
                sleep(Duration::from_secs(1)).await;
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
                                queue.send(reading_to_json(&reading, &device)).ok();
                            }
                        }
                    }
                    None => {
                        if let Some(ct) = current_time {
                            previous_time = Some(ct.to_string());
                            queue.send(reading_to_json(&reading, &device)).ok();
                        }
                    }
                }
            }
        });
    }
    while let Some(_res) = set.join_next().await {}
}
