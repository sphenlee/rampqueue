use rampqueue::{Connection, Error};
use std::time::Duration;

#[tokio::main]
pub async fn main() -> Result<(), Error> {
    env_logger::init();

    let mut conn = Connection::new("redis://localhost", "priority").await?;

    conn.declare_priority_queue("demo", 3).await?;

    let mut consumer = conn.consume("demo").await?;

    loop {
        let d = consumer.try_next().await?;
        let payload = String::from_utf8(d.payload.clone()).expect("payload wasn't UTF-8");
        println!("got message: {}", payload);

        tokio::time::sleep(Duration::from_secs(30)).await;

        consumer.ack(&d).await?;
        println!("ack'ed message: {}", d.delivery_tag);
    }
}
