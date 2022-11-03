use log::trace;
use redis::aio::ConnectionLike;
use redis::streams::{
    StreamClaimReply, StreamId, StreamInfoConsumersReply, StreamInfoGroupsReply,
    StreamPendingCountReply, StreamReadOptions, StreamReadReply,
};
use redis::{AsyncCommands, Client, Cmd, FromRedisValue, IntoConnectionInfo, RedisError, Value};
use std::fmt::{Debug, Formatter};
use std::time::Duration;
use tokio::time::Instant;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("redis error: {0}")]
    RedisError(#[from] RedisError),

    #[error("message is missing payload")]
    NoPayload,
}

pub struct Connection {
    client: Client,
    redis: redis::aio::Connection,
}

fn get_steam_name(queue: &str) -> String {
    format!("rampqueue.{}", queue)
}

impl Connection {
    pub async fn new<T: IntoConnectionInfo>(params: T) -> Result<Connection, Error> {
        let client = Client::open(params)?;
        let conn = client.get_tokio_connection().await?;
        Ok(Connection {
            client,
            redis: conn,
        })
    }

    pub async fn declare_queue(&mut self, queue_name: &str) -> Result<(), Error> {
        let key = get_steam_name(queue_name);
        trace!("declare queue: key={}", key);
        self.redis
            .xgroup_create_mkstream(key, "primary", "$")
            .await
            .or_else(|err| {
                if err.code() == Some("BUSYGROUP") {
                    trace!("queue already existed");
                    Ok(()) // queue already exists
                } else {
                    Err(err)
                }
            })?;

        Ok(())
    }

    pub async fn publish(&mut self, queue_name: &str, msg: Message) -> Result<(), Error> {
        let key = get_steam_name(queue_name);
        self.redis
            .xadd(key, "*", &[("payload", msg.payload)])
            .await?;
        Ok(())
    }

    pub async fn consume(&mut self, queue_name: &str) -> Result<Consumer, Error> {
        self.consume_named(queue_name, uuid::Uuid::new_v4().to_string())
            .await
    }

    pub async fn consume_named(
        &mut self,
        queue_name: &str,
        consumer: impl Into<String>,
    ) -> Result<Consumer, Error> {
        Ok(Consumer {
            redis: self.client.get_tokio_connection().await?,
            last_id: Some("0-0".to_owned()),
            key: get_steam_name(queue_name),
            group: "primary".to_owned(),
            consumer: consumer.into(),
            next_maintenance: Instant::now(),
        })
    }
}

const CONSUMER_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5 * 60);

pub struct Consumer {
    redis: redis::aio::Connection,
    last_id: Option<String>,
    key: String,
    group: String,
    consumer: String,
    next_maintenance: Instant,
}

impl Debug for Consumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("key", &self.key)
            .field("group", &self.group)
            .field("consumer", &self.consumer)
            .finish()
    }
}

// TODO - make both of these configurable for the consumer
const CHECK_PENDING: usize = 60 * 1000;
const IDLE_TIMEOUT: usize = 5 * 60 * 1000;
const CONSUMER_IDLE_MULTIPLIER: usize = 3;

impl Consumer {
    pub async fn try_next(&mut self) -> Result<Delivery, Error> {
        trace!("try next: {:?}", self);

        loop {
            if self.next_maintenance < Instant::now() {
                self.next_maintenance += CONSUMER_MAINTENANCE_INTERVAL;

                trace!("running maintenance");
                let info: StreamInfoConsumersReply =
                    self.redis.xinfo_consumers(&self.key, &self.group).await?;
                for consumer in info.consumers {
                    if consumer.idle > IDLE_TIMEOUT * CONSUMER_IDLE_MULTIPLIER {
                        trace!("consumer {} has been idle too long", consumer.name);
                        self.redis
                            .xgroup_delconsumer(&self.key, &self.group, consumer.name)
                            .await?;
                    }
                }

                let groups: StreamInfoGroupsReply = self.redis.xinfo_groups(&self.key).await?;
                let min_id = groups.groups.into_iter().map(|g| g.last_delivered_id).min();
                if let Some(id) = min_id {
                    trace!("trimming entries older than {}", id);
                    let mut cmd = Cmd::new();
                    cmd.arg("XTRIM")
                        .arg(&self.key)
                        .arg("MINID")
                        .arg("~")
                        .arg(id);
                    let value = self.redis.req_packed_command(&cmd).await?;
                    let trimmed = usize::from_redis_value(&value)?;
                    trace!("trimmed {} entries", trimmed);
                }

                trace!("next maintenance in {:?}", CONSUMER_MAINTENANCE_INTERVAL);
            }

            trace!("check pending entries");

            let mut cmd = Cmd::new();
            cmd.arg("XPENDING")
                .arg(&self.key)
                .arg(&self.group)
                .arg("IDLE")
                .arg(IDLE_TIMEOUT)
                .arg("-")
                .arg("+")
                .arg(1);
            let value = self.redis.req_packed_command(&cmd).await?;
            let mut pending = StreamPendingCountReply::from_redis_value(&value)?;

            if let Some(entry) = pending.ids.pop() {
                trace!("claiming pending entry: {:?}", entry);

                let mut claim: StreamClaimReply = self
                    .redis
                    .xclaim(
                        &self.key,
                        &self.group,
                        &self.consumer,
                        IDLE_TIMEOUT,
                        &[entry.id],
                    )
                    .await?;

                if let Some(entry) = claim.ids.pop() {
                    trace!("claimed entry: id={}", entry.id);
                    return Delivery::from_stream_id(&entry);
                } else {
                    trace!("someone else claimed it, try again");
                    continue;
                }
            }

            trace!("no pending entries, start reading");

            let options = StreamReadOptions::default()
                .group(&self.group, &self.consumer)
                .block(CHECK_PENDING)
                .count(1);

            let read: Option<StreamReadReply> = match self.last_id.take() {
                Some(id) => {
                    trace!("resume read from id: {}", id);
                    self.redis
                        .xread_options(&[&self.key], &[id], &options)
                        .await?
                }
                None => {
                    trace!("read new entries: '>'");
                    self.redis
                        .xread_options(&[&self.key], &[">"], &options)
                        .await?
                }
            };

            if !read.is_some() {
                trace!("no entries read within timeout, check pending again");
                continue;
            }
            let read = read.unwrap();

            if read.keys[0].ids.is_empty() {
                trace!("no entries returned, wait for new entries");
                continue;
            }

            let msg = &read.keys[0].ids[0];
            trace!("read entry, next id is: {}", msg.id);
            self.last_id = Some(msg.id.clone());

            return Delivery::from_stream_id(msg);
        }
    }

    pub async fn ack(&mut self, delivery_tag: &str) -> Result<(), Error> {
        trace!("ack'ing entry: {}", delivery_tag);
        self.redis
            .xack(&self.key, &self.group, &[delivery_tag])
            .await?;
        Ok(())
    }
}

pub struct Delivery {
    pub payload: Vec<u8>,
    pub delivery_tag: String,
}

impl Delivery {
    fn from_stream_id(msg: &StreamId) -> Result<Self, Error> {
        match &msg.map["payload"] {
            Value::Data(payload) => Ok(Delivery {
                payload: payload.clone(),
                delivery_tag: msg.id.clone(),
            }),
            _ => Err(Error::NoPayload),
        }
    }
}

pub struct Message {
    pub payload: Vec<u8>,
}
