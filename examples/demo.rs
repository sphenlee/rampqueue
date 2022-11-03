use clap::Parser;
use rampqueue::{Connection, Error};
use std::time::Duration;

#[derive(clap::Parser)]
struct Args {
    #[arg(short, long)]
    consumer_name: Option<String>,
}

#[tokio::main]
pub async fn main() -> Result<(), Error> {
    let args = Args::parse();

    env_logger::init();

    let mut conn = Connection::new("redis://localhost").await?;

    conn.declare_queue("demo").await?;

    let mut consumer = if let Some(consumer_name) = args.consumer_name {
        conn.consume_named("demo", consumer_name).await?
    } else {
        conn.consume("demo").await?
    };

    loop {
        let d = consumer.try_next().await?;
        let payload = String::from_utf8(d.payload).expect("payload wasn't UTF-8");
        println!("got message: {}", payload);

        tokio::time::sleep(Duration::from_secs(30)).await;

        consumer.ack(&d.delivery_tag).await?;
        println!("ack'ed message: {}", d.delivery_tag);
    }
}
