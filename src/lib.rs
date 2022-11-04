use std::cmp::Ordering;
use log::trace;
use redis::aio::ConnectionLike;
use redis::streams::{
    StreamClaimReply, StreamId, StreamInfoConsumersReply, StreamInfoGroupsReply,
    StreamPendingCountReply, StreamReadOptions, StreamReadReply,
};
use redis::{AsyncCommands, Client, Cmd, FromRedisValue, IntoConnectionInfo, RedisError, Value};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::time::Duration;
use tokio::time::Instant;
use binary_heap_plus::{BinaryHeap, MinComparator};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("redis error: {0}")]
    RedisError(#[from] RedisError),

    #[error("message is missing payload")]
    NoPayload,

    #[error("priority queue redeclared with different number of levels")]
    PriorityQueueConflict,
}

pub struct Connection {
    client: Client,
    redis: redis::aio::Connection,
    ns: String,
    priority_queues: HashMap<String, u8>,
}

fn get_steam_name(ns: &str, queue: &str) -> String {
    format!("{}.{}", ns, queue)
}

fn get_priority_steam_name(ns: &str, queue: &str, priority: u8) -> String {
    format!("{}.{}.{}", ns, queue, priority)
}

impl Connection {
    pub async fn new<T: IntoConnectionInfo>(
        params: T,
        namespace: impl Into<String>,
    ) -> Result<Connection, Error> {
        let client = Client::open(params)?;
        let conn = client.get_tokio_connection().await?;
        Ok(Connection {
            client,
            redis: conn,
            ns: namespace.into(),
            priority_queues: HashMap::new(),
        })
    }

    async fn do_declare_queue(&mut self, key: &str) -> Result<(), Error> {
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

    pub async fn declare_queue(&mut self, queue_name: &str) -> Result<(), Error> {
        let key = get_steam_name(&self.ns, queue_name);
        trace!("declare queue: key={}", key);
        self.do_declare_queue(&key).await
    }

    pub async fn declare_priority_queue(
        &mut self,
        queue_name: &str,
        levels: u8,
    ) -> Result<(), Error> {
        let existing_levels = *self
            .priority_queues
            .entry(queue_name.to_owned())
            .or_insert(levels);
        if existing_levels != levels {
            return Err(Error::PriorityQueueConflict);
        }

        for i in 0..levels {
            let key = get_priority_steam_name(&self.ns, queue_name, i);
            trace!("declare priority queue: key={}", key);
            self.do_declare_queue(&key).await?;
        }

        Ok(())
    }

    async fn do_publish(&mut self, key: &str, msg: Message) -> Result<(), Error> {
        self.redis
            .xadd(key, "*", &[("payload", msg.payload)])
            .await?;
        Ok(())
    }

    pub async fn publish(&mut self, queue_name: &str, msg: Message) -> Result<(), Error> {
        let key = get_steam_name(&self.ns, queue_name);
        self.do_publish(&key, msg).await
    }

    pub async fn publish_priority(
        &mut self,
        queue_name: &str,
        msg: Message,
        priority: u8,
    ) -> Result<(), Error> {
        let key = get_priority_steam_name(&self.ns, queue_name, priority);
        self.do_publish(&key, msg).await
    }

    pub async fn consume(&mut self, queue_name: &str) -> Result<Consumer, Error> {
        let consumer = uuid::Uuid::new_v4().to_string();

        match self.priority_queues.get(queue_name) {
            None => {
                let key = get_steam_name(&self.ns, queue_name);
                self.do_consume(vec![key], consumer).await
            }
            Some(&levels) => {
                let keys = (0..levels)
                    .map(|i| get_priority_steam_name(&self.ns, queue_name, i))
                    .collect();
                self.do_consume(keys, consumer).await
            }
        }
    }

    pub async fn consume_named(
        &mut self,
        queue_name: &str,
        consumer: impl Into<String>,
    ) -> Result<Consumer, Error> {
        let key = get_steam_name(&self.ns, queue_name);
        self.do_consume(vec![key], consumer).await
    }

    async fn do_consume(
        &mut self,
        keys: Vec<String>,
        consumer: impl Into<String>,
    ) -> Result<Consumer, Error> {
        Ok(Consumer {
            redis: self.client.get_tokio_connection().await?,
            last_id: std::iter::repeat(">".to_owned()).take(keys.len()).collect(),
            keys,
            group: "primary".to_owned(),
            consumer: consumer.into(),
            next_maintenance: Instant::now(),
            prefetch: BinaryHeap::new_min(),
        })
    }
}

const CONSUMER_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5 * 60);

pub struct Consumer {
    redis: redis::aio::Connection,
    last_id: Vec<String>,
    keys: Vec<String>,
    group: String,
    consumer: String,
    next_maintenance: Instant,
    prefetch: BinaryHeap<Delivery, MinComparator>
}

impl Debug for Consumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("keys", &self.keys)
            .field("group", &self.group)
            .field("consumer", &self.consumer)
            .finish()
    }
}

// TODO - make both of these configurable for the consumer
const READ_BLOCK_TIMEOUT: usize = 60 * 1000;
const IDLE_TIMEOUT: usize = 5 * 60 * 1000;
const CONSUMER_IDLE_MULTIPLIER: usize = 3;

impl Consumer {
    pub async fn try_next(&mut self) -> Result<Delivery, Error> {
        trace!("try next: {:?}", self);

        /*if let Some(delivery) = self.prefetch.pop() {
            trace!("returning entry from prefetch");
            return Ok(delivery);
        }*/

        loop {
            if self.next_maintenance < Instant::now() {
                self.do_maintenance().await?;
            }

            if let Some(delivery) = self.check_pending().await? {
                return Ok(delivery);
            }

            trace!("waiting for entries");

            let options = {
                let options = StreamReadOptions::default()
                    .group(&self.group, &self.consumer)
                    .count(1);

                if self.prefetch.is_empty() {
                    trace!("no entries prefetched, blocking read");
                    options.block(READ_BLOCK_TIMEOUT)
                } else {
                    options
                }
            };

            let read: Option<StreamReadReply> = self.redis
                .xread_options(&self.keys, &self.last_id, &options)
                .await?;

            if !read.is_some() {
                if let Some(delivery) = self.prefetch.pop() {
                    return Ok(delivery);
                }
                trace!("no entries read within timeout, check pending again");
                continue;
            }
            let read = read.unwrap();

            for key in read.keys {
                if key.ids.is_empty() {
                    trace!("no entries returned for {}", key.key);
                } else {
                    let msg = &key.ids[0];
                    trace!("read entry from key {}, id is: {}", key.key, msg.id);

                    let delivery = Delivery::from_stream_id(key.key, msg)?;
                    self.prefetch.push(delivery);

                }
            }

            if let Some(delivery) = self.prefetch.pop() {
                return Ok(delivery);
            }

            trace!("no entries returned for any key, wait for new entries");
        }
    }

    pub async fn ack(&mut self, delivery: &Delivery) -> Result<(), Error> {
        trace!("ack'ing entry: {}", delivery.delivery_tag);
        self.redis
            .xack(&delivery.key, &self.group, &[&delivery.delivery_tag])
            .await?;
        Ok(())
    }

    async fn do_maintenance(&mut self) -> Result<(), Error> {
        self.next_maintenance += CONSUMER_MAINTENANCE_INTERVAL;

        trace!("running maintenance");
        for key in &self.keys {
            let info: StreamInfoConsumersReply =
                self.redis.xinfo_consumers(key, &self.group).await?;
            for consumer in info.consumers {
                if consumer.idle > IDLE_TIMEOUT * CONSUMER_IDLE_MULTIPLIER {
                    trace!("consumer {} has been idle too long", consumer.name);
                    if consumer.pending > 0 {
                        trace!("consumer {} has {} pending entries", consumer.name, consumer.pending);
                    } else {
                        self.redis
                            .xgroup_delconsumer(key, &self.group, consumer.name)
                            .await?;
                    }
                }
            }

            let groups: StreamInfoGroupsReply = self.redis.xinfo_groups(key).await?;
            let min_id = groups.groups.into_iter().map(|g| g.last_delivered_id).min();
            if let Some(id) = min_id {
                trace!("trimming entries older than {}", id);
                let mut cmd = Cmd::new();
                cmd.arg("XTRIM")
                    .arg(key)
                    .arg("MINID")
                    .arg("~")
                    .arg(id);
                let value = self.redis.req_packed_command(&cmd).await?;
                let trimmed = usize::from_redis_value(&value)?;
                trace!("trimmed {} entries", trimmed);
            }
        }

        trace!("next maintenance in {:?}", CONSUMER_MAINTENANCE_INTERVAL);
        Ok(())
    }

    async fn check_pending(&mut self) -> Result<Option<Delivery>, Error> {
        for key in self.keys.clone() {
            match self.check_one_pending(&key).await? {
                Some(delivery) => return Ok(Some(delivery)),
                None => continue,
            };
        }

        Ok(None)
    }

    async fn check_one_pending(&mut self, key: &str) -> Result<Option<Delivery>, Error> {
        trace!("check pending entries for {}", key);

        loop {
            let mut cmd = Cmd::new();
            cmd.arg("XPENDING")
                .arg(key)
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
                        key,
                        &self.group,
                        &self.consumer,
                        IDLE_TIMEOUT,
                        &[entry.id],
                    )
                    .await?;

                if let Some(entry) = claim.ids.pop() {
                    trace!("claimed entry: id={}", entry.id);
                    let delivery = Delivery::from_stream_id(key.to_owned(),&entry)?;
                    return Ok(Some(delivery));
                } else {
                    trace!("someone else claimed it, try again");
                    continue;
                }
            } else {
                trace!("no more pending entries");
                return Ok(None);
            }
        }
    }
}

pub struct Delivery {
    pub key: String,
    pub payload: Vec<u8>,
    pub delivery_tag: String,
}

impl Delivery {
    fn from_stream_id(key: String, msg: &StreamId) -> Result<Self, Error> {
        match &msg.map["payload"] {
            Value::Data(payload) => Ok(Delivery {
                key,
                payload: payload.clone(),
                delivery_tag: msg.id.clone(),
            }),
            _ => Err(Error::NoPayload),
        }
    }
}

impl Eq for Delivery {}

impl PartialEq<Self> for Delivery {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.delivery_tag == other.delivery_tag
    }
}

impl PartialOrd<Self> for Delivery {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Delivery {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key).then(self.delivery_tag.cmp(&other.delivery_tag))
    }
}

pub struct Message {
    pub payload: Vec<u8>,
    pub priority: Option<u8>,
}
