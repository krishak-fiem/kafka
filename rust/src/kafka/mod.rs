use models::products::user::User;
use std::str;
use tokio;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use std::thread;

pub async fn consume_messages(topic: &str, brokers: &str) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "my_consumer_group")
        .create()
        .expect("invalid consumer config");

    consumer
        .subscribe(&[topic])
        .expect("topic subscribe failed");

    thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            loop {
                for msg_result in consumer.iter() {
                    let msg = msg_result.unwrap();
                    let key: &str = msg.key_view().unwrap().unwrap();
                    let value = msg.payload().unwrap();
                    let user: User =
                        serde_json::from_slice(value).expect("failed to deser JSON to User");
                    user.add_user().await;
                    println!(
                        "received key {} with value {:?} in offset {:?} from partition {}",
                        key,
                        user,
                        msg.offset(),
                        msg.partition()
                    )
                }
            }
        });
    });
}
