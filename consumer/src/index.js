// =============================================================================
// Consumer：以 Consumer Group 讀取訂單事件，寫入 Redis。
// =============================================================================
//
// 誰執行：consumer 容器（consumer/Dockerfile 的 CMD），compose 以 deploy.replicas: 2 起 2 個實體。
// 何時跑：kafka-init 成功結束、redis healthy 之後啟動，常駐；restart: unless-stopped。
// 用哪個帳號：Kafka consumer（只能讀）、Redis app（只能碰 myapp:*）。
//
// 流程：讀設定 → 連 Redis → 連 Kafka → 加入 group 並訂閱 → 每筆訊息：驗證 → 寫 Redis → commit。
//
// 負載平衡：所有實體使用同一個 groupId，Kafka 把 3 個 partition 分給組內成員，
//           每個 partition 同時只會有一個成員在讀 → 同一個 order_id 一定由同一個實體依序處理。
// 投遞語意：eachMessage 回傳後 kafkajs 才把 offset 標成已處理並 commit（at-least-once），
//           重複投遞由 store 依 seq 去重。
//
// log 一律是一行一個 JSON，msg 欄位可用來 grep：
//   group_joined（分到哪些 partition）、stored、order_gap、duplicate_skipped、invalid_message
// =============================================================================
import os from 'node:os';
import { Kafka, logLevel } from 'kafkajs';
import { createClient } from 'redis';
import { loadConfig } from './config.js';
import { parseEvent } from './event.js';
import { createEventStore } from './store.js';

// 設定有錯就直接結束（exit 1）。os.hostname() 在容器內是 container id，用來當 consumerId 預設值
let config;
try {
  config = loadConfig(process.env, os.hostname());
} catch (err) {
  console.error(`設定錯誤：${err.message}`);
  process.exit(1);
}

// 每行 log 都帶 consumer 欄位，兩個實體的 log 混在一起時也分得出是誰
const log = (fields) => console.log(JSON.stringify({ ts: new Date().toISOString(), consumer: config.consumerId, ...fields }));

// node-redis client。用 node-redis 而不用 ioredis：ioredis 連線後預設送 INFO 做 ready check，
// app 帳號沒有 INFO 權限會連不上
const redis = createClient({
  url: config.redisUrl,
  username: config.redisUsername,
  password: config.redisPassword,
});
// 沒有監聽 error 事件的話，連線中斷會變成未處理的例外直接讓 process 當掉；
// 監聽後 node-redis 會自己重連
redis.on('error', (err) => log({ msg: 'redis_error', error: err.message }));

// 連線設定與 producer 相同：SCRAM-SHA-512、無 TLS、只印警告
const kafka = new Kafka({
  clientId: `order-consumer-${config.consumerId}`,
  brokers: config.brokers,
  ssl: false,
  sasl: { mechanism: 'scram-sha-512', username: config.kafkaUsername, password: config.kafkaPassword },
  logLevel: logLevel.WARN,
  retry: { retries: 10 },
});

// groupId 相同的 consumer 會被 Kafka 視為同一組，partition 在組內分配
const consumer = kafka.consumer({ groupId: config.groupId, allowAutoTopicCreation: false });
const store = createEventStore(redis, { prefix: config.keyPrefix, ttlSeconds: config.ttlSeconds });

// rebalance 時印出這個實體分到哪些 partition，驗證負載平衡最直接的證據。
// memberAssignment 的格式是 { topic名稱: [partition 編號...] }
consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
  log({ msg: 'group_joined', memberId: payload.memberId, partitions: payload.memberAssignment[config.topic] ?? [] });
});

// 每筆訊息呼叫一次。同一個 partition 的訊息依序呼叫，上一筆 await 完才會給下一筆
async function handleMessage({ partition, message }) {
  // 不合法的訊息記 log 後 return（正常結束）→ kafkajs commit 這筆，繼續處理下一筆
  const parsed = parseEvent(message);
  if (!parsed.ok) {
    log({ msg: 'invalid_message', partition, offset: message.offset, reason: parsed.reason });
    return;
  }

  const { event } = parsed;
  // Redis 寫入失敗會丟出例外 → kafkajs 不 commit 這筆並重試，不會遺失
  const outcome = await store.apply(event, { consumerId: config.consumerId, partition, offset: message.offset });

  // 依結果記不同的 msg，驗證時可以 grep 計數（正常情況下 order_gap 與 duplicate_skipped 都是 0）
  if (outcome === 'gap') {
    log({ msg: 'order_gap', key: event.order_id, seq: event.seq, partition, offset: message.offset });
  } else if (outcome === 'duplicate') {
    log({ msg: 'duplicate_skipped', key: event.order_id, seq: event.seq, partition, offset: message.offset });
  } else {
    log({ msg: 'stored', key: `${config.keyPrefix}:events:${event.order_id}`, seq: event.seq, status: event.status, partition, offset: message.offset });
  }
}

// 優雅關閉；stopping 避免 SIGTERM 與 CRASH 同時觸發時跑兩次
let stopping = false;
async function shutdown(code = 0) {
  if (stopping) return;
  stopping = true;
  // 先離開 group，讓其他實體立刻接手 partition，不必等 session timeout。
  // 兩個連線各自 try/catch：其中一個關閉失敗，另一個仍會關閉
  for (const [name, close] of [['kafka', () => consumer.disconnect()], ['redis', () => redis.quit()]]) {
    try {
      await close();
    } catch (err) {
      log({ msg: 'disconnect_failed', target: name, error: err.message });
    }
  }
  log({ msg: 'stopped', code });
  process.exit(code);
}

// docker compose stop / --scale 縮減實體時送 SIGTERM
process.on('SIGTERM', () => shutdown(0));
process.on('SIGINT', () => shutdown(0));
consumer.on(consumer.events.CRASH, ({ payload }) => {
  // kafkajs 對可重試的錯誤會自己重啟 consumer；不可重試時才退出交給 compose 重啟
  if (!payload.restart) {
    log({ msg: 'consumer_crashed', error: payload.error?.message });
    shutdown(1);
  }
});

// 啟動順序：先連 Redis（寫入目標要先就緒）→ 連 Kafka → 訂閱 → 開始消費。
// fromBeginning: true：group 第一次啟動、還沒有 committed offset 時，從最舊的訊息開始讀；
//   之後重啟會從上次 commit 的位置接著讀。
// consumer.run 啟動背景的消費迴圈後就回傳，不會一直卡在這行。
try {
  await redis.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: config.topic, fromBeginning: true });
  await consumer.run({ eachMessage: handleMessage });
} catch (err) {
  log({ msg: 'startup_failed', error: err.message });
  await shutdown(1);
}
log({ msg: 'running', topic: config.topic, groupId: config.groupId, kafkaUser: config.kafkaUsername, redisUser: config.redisUsername });
