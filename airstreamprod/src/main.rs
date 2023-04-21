use kafka::producer::{Producer, Record, RequiredAcks};
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};

fn get_time_from_reading(reading: &String) -> Option<String> {
    let read_values: Vec<String> = reading
        .split("\n")
        .into_iter()
        .map(|x| x.to_string())
        .collect();
    match read_values.get(1) {
        Some(row) => Some(row.split_once("|").unwrap().0.to_string()),
        None => None,
    }
}

#[allow(unused)]
#[tokio::main]
async fn main() {
    let devices = reqwest::get("https://devel.klimerko.org/api/devices")
        .await
        .expect("klimerko unreachable")
        .text()
        .await
        .expect("resposne body");
    let devices: Vec<String> = devices
        .split("\n")
        .skip(1)
        .filter(|device| device.contains("|"))
        .map(|device| device.split_once("|").unwrap().0.to_string())
        .collect();

    let mut set = JoinSet::new();

    for device in devices {
        set.spawn(async move {
            let mut previous_time: Option<String> = None;
            let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
                .with_ack_timeout(Duration::from_secs(1))
                .with_required_acks(RequiredAcks::One)
                .create()
                .expect("kafka connection");
            let client = reqwest::Client::new();
            loop {
                sleep(Duration::from_millis(10000)).await;
                let reading = client
                    .get(format!(
                        "https://devel.klimerko.org/api/device/{device:}/latest"
                    ))
                    .send()
                    .await
                    .expect("klimerko unreachable")
                    .text()
                    .await
                    .expect("resposne body");
                let current_time = get_time_from_reading(&reading);
                match previous_time {
                    Some(ref pt) => match current_time {
                        Some(ct) => {
                            if !pt.eq(&ct) {
                                previous_time = Some(ct.to_string());
                                producer
                                    .send(&Record::from_value("my-topic", reading.as_bytes()))
                                    .unwrap();
                            }
                        }
                        None => {}
                    },
                    None => match current_time {
                        Some(ct) => {
                            previous_time = Some(ct.to_string());
                            producer
                                .send(&Record::from_value("my-topic", reading.as_bytes()))
                                .unwrap();
                        }
                        None => {}
                    },
                }
            }
        });
    }
    while let Some(res) = set.join_next().await {}
}
