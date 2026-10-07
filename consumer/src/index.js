// =============================================================================
// Consumer：以 Consumer Group 讀取訂單事件，寫入 Redis。
// =============================================================================
//
// 誰執行：consumer 容器（consumer/Dockerfile 的 CMD ["node", "src/index.js"]），
//         compose 以 deploy.replicas: 2 起 2 個實體。
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
//
// 本檔的函式與誰呼叫它們：
//   log            ← 本檔所有地方
//   handleMessage  ← kafkajs（consumer.run 的 eachMessage，每筆訊息一次）
//   shutdown       ← SIGTERM / SIGINT、CRASH 事件、啟動失敗
//   事件 callback  ← kafkajs / node-redis / Node.js 在對應事件發生時呼叫
//
// 閱讀提示：這支程式「怎麼跑起來」
//   1. node src/index.js 從第一行往下執行到最後一行（設定、建連線物件、註冊 callback、連線）。
//   2. 執行到底之後程式「不會結束」：Kafka 與 Redis 的連線還開著，Node.js 會一直等事件發生。
//   3. 之後每來一筆 Kafka 訊息，kafkajs 就呼叫一次 handleMessage；收到 SIGTERM 就呼叫 shutdown。
//   也就是說，下面很多段程式是「先登記：某件事發生時要做什麼」，不是馬上執行。
//
// 閱讀提示（本檔用到的 JavaScript / Node.js 語法）：
//   import x from 'y'           載入套件或其他檔案（類似 Python 的 import）
//   process                     Node.js 內建物件，代表目前這個程式：
//                               process.env 環境變數、process.exit(碼) 結束程式、process.on 收訊號
//   物件.on('事件', 函式)       「登記」：事件發生時呼叫這個函式（callback）。登記當下不會執行
//   ({ payload }) => { ... }    箭頭函式 + 解構：參數是物件，直接取出它的 payload 欄位
//   async / await               見 store.js：await 會等網路操作完成再往下
//   new 類別(...)               建立物件，例如 new Kafka({...}) 建立一個 Kafka client
// =============================================================================

// 白話：載入需要的東西。
//   node:os    → Node.js 內建，用來取得主機名稱
//   kafkajs    → Kafka client 套件（npm 安裝，見 package.json）
//   redis      → node-redis，Redis client 套件
//   ./xxx.js   → 本專案自己的檔案（./ 代表同一個資料夾）
// 語法：import os from ...           取該模組的「預設匯出」，命名為 os
//       import { Kafka, logLevel } ... 只取出指定名稱的東西
import os from 'node:os';
import { Kafka, logLevel } from 'kafkajs';
import { createClient } from 'redis';
import { loadConfig } from './config.js';
import { parseEvent } from './event.js';
import { createEventStore } from './store.js';

// 白話：讀設定。設定有錯（缺環境變數等）就印出原因並以 exit 1 結束，compose 的 logs 看得到。
//   os.hostname() 在容器內是 container id，用來當 consumerId 預設值。
// 語法：let config; 先宣告變數，因為要在 try 區塊裡才指定值，而 try 外面也要用到它。
//       try { ... } catch (err) { ... }：try 裡丟出例外就跳到 catch，err 是那個錯誤物件。
//       console.error 印到 stderr（console.log 印到 stdout）。
let config;
try {
  config = loadConfig(process.env, os.hostname());
} catch (err) {
  console.error(`設定錯誤：${err.message}`);
  process.exit(1);
}

/**
 * 印出一行 JSON log。
 *
 * 呼叫者：本檔所有需要記錄的地方。
 *
 * 自動加上 ts（時間）與 consumer（哪個實體），兩個實體的 log 混在一起時也分得出是誰。
 * 呼叫端只要傳這次要記的欄位，...fields 會把它們展開進同一個物件。
 *
 * 語法：const log = (fields) => ...  定義一個函式存進 log 變數，之後寫 log({...}) 呼叫。
 *       JSON.stringify(物件)          物件 → JSON 字串
 *       new Date().toISOString()      現在時間，例如 '2026-10-06T10:28:00.000Z'
 *
 * @param {object} fields  要記錄的欄位，慣例上一定有 msg，例如 { msg: 'stored', key, seq }
 * @returns {void}
 *
 * @example
 *   log({ msg: 'stored', seq: 2 })
 *   // 印出 {"ts":"2026-10-06T...","consumer":"a1b2c3","msg":"stored","seq":2}
 */
const log = (fields) => console.log(JSON.stringify({ ts: new Date().toISOString(), consumer: config.consumerId, ...fields }));

// 白話：建立 Redis client 物件（這時還沒連線，下面 redis.connect() 才真的連）。
//   帳密是 config 裡的 app 帳號。
//   用 node-redis 而不用 ioredis：ioredis 連線後預設送 INFO 做 ready check，
//   app 帳號沒有 INFO 權限會連不上
// 語法：createClient({ 選項 }) 傳一個物件當設定；url: config.redisUrl 表示選項 url 的值取自 config。
const redis = createClient({
  url: config.redisUrl,
  username: config.redisUsername,
  password: config.redisPassword,
});

/**
 * callback：Redis 連線發生錯誤時，由 node-redis 呼叫。
 *
 * 沒有監聽 error 事件的話，連線中斷會變成未處理的例外直接讓 process 當掉；
 * 監聽後只記 log，node-redis 會自己重連。
 *
 * 語法：redis.on('error', 函式) 是「登記」，這一行執行時不會呼叫函式，等錯誤發生才呼叫。
 *
 * @param {Error} err  連線錯誤
 */
redis.on('error', (err) => log({ msg: 'redis_error', error: err.message }));

// 白話：建立 Kafka client 物件（同樣還沒連線）。設定與 producer 相同：SCRAM-SHA-512、無 TLS、只印警告。
//   clientId：broker 端 log 裡辨識是誰連進來，例如 order-consumer-a1b2c3
//   ssl: false：lab 沒有 TLS（對應 broker 的 SASL_PLAINTEXT）
//   sasl：用 SCRAM-SHA-512 + consumer 帳號登入（對應 start.sh 的 INTERNAL listener）
//   logLevel.WARN：kafkajs 自己的 log 只印警告以上
//   retry.retries: 10：連線失敗自動重試 10 次
// 語法：new Kafka({ ... }) 用 Kafka 類別建立一個物件。
const kafka = new Kafka({
  clientId: `order-consumer-${config.consumerId}`,
  brokers: config.brokers,
  ssl: false,
  sasl: { mechanism: 'scram-sha-512', username: config.kafkaUsername, password: config.kafkaPassword },
  logLevel: logLevel.WARN,
  retry: { retries: 10 },
});

// 白話：從 Kafka client 建立 consumer。groupId 相同的 consumer 會被 Kafka 視為同一組，partition 在組內分配。
//   allowAutoTopicCreation: false：不讓 client 自動建 topic（topic 由 init.sh 建）
const consumer = kafka.consumer({ groupId: config.groupId, allowAutoTopicCreation: false });
// 白話：呼叫 store.js 的工廠函式，得到 store 物件；之後 handleMessage 用 store.apply() 寫入 Redis。
const store = createEventStore(redis, { prefix: config.keyPrefix, ttlSeconds: config.ttlSeconds });

/**
 * callback：這個實體加入 group（或 rebalance 後重新分配）時，由 kafkajs 呼叫。
 *
 * 印出這個實體分到哪些 partition，是驗證負載平衡最直接的證據
 * （驗證指令：docker compose logs consumer | grep group_joined）。
 *
 * 語法：({ payload }) => { ... } 參數是 kafkajs 傳來的事件物件，用解構只取出 payload 欄位。
 *       payload.memberAssignment[config.topic] 取出 topic 'orders' 對應的 partition 陣列；
 *       ?? [] 表示沒分到（undefined）時記成空陣列。
 *
 * @param {object} event.payload.memberId          Kafka 給這個成員的 ID
 * @param {object} event.payload.memberAssignment  分配結果，格式 { topic名稱: [partition 編號...] }，例如 { orders: [0, 1] }
 */
consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
  log({ msg: 'group_joined', memberId: payload.memberId, partitions: payload.memberAssignment[config.topic] ?? [] });
});

/**
 * 處理一筆 Kafka 訊息：驗證 → 寫入 Redis → 記 log。
 *
 * 呼叫者：kafkajs。下方 consumer.run({ eachMessage: handleMessage }) 把它註冊成每筆訊息的處理函式。
 * 會呼叫：event.js 的 parseEvent → store.js 的 apply（apply 內部再呼叫 event.js 的 classifySeq）。
 *
 * 執行順序：同一個 partition 的訊息依序呼叫，上一筆 await 完才會給下一筆（順序性的最後一環）。
 *
 * 結束方式決定 offset 會不會 commit：
 *   正常 return（含壞訊息被跳過） → kafkajs commit 這筆，繼續下一筆
 *   丟出例外（例如 Redis 寫入失敗） → kafkajs 不 commit，稍後重試同一筆
 *
 * 語法：參數 { partition, message } 是解構：kafkajs 傳一個物件進來，這裡只取 partition 和 message 兩個欄位。
 *
 * @param {object} payload            kafkajs 傳入的物件（還有 topic、heartbeat 等欄位，這裡用不到）
 * @param {number} payload.partition  訊息來自哪個 partition
 * @param {object} payload.message    訊息本體：key、value（Buffer）、offset（字串）
 * @returns {Promise<void>}
 */
async function handleMessage({ partition, message }) {
  // 白話【1. 驗證】呼叫 event.js 的 parseEvent，得到 { ok: true, event } 或 { ok: false, reason }。
  const parsed = parseEvent(message);
  // 白話：驗證失敗 → 記 invalid_message log 後 return（正常結束）→ kafkajs commit 這筆，繼續處理下一筆。
  //   不丟例外，是為了不讓一筆壞訊息卡住整個 partition。
  if (!parsed.ok) {
    log({ msg: 'invalid_message', partition, offset: message.offset, reason: parsed.reason });
    return;
  }

  // 白話：取出解析好的事件。
  // 語法：const { event } = parsed; 等於 const event = parsed.event;（解構）
  const { event } = parsed;
  // 白話【2. 寫入 Redis】呼叫 store.js 的 apply，等它完成，得到 'in_order' / 'gap' / 'duplicate'。
  //   Redis 寫入失敗會丟出例外 → 這個函式也跟著中止 → kafkajs 不 commit 這筆並重試，不會遺失。
  //   第二個參數是處理資訊：consumerId 是 config 的值，partition 與 offset 來自這筆訊息。
  const outcome = await store.apply(event, { consumerId: config.consumerId, partition, offset: message.offset });

  // 白話【3. 記 log】依結果記不同的 msg，驗證時可以 grep 計數（正常情況下 order_gap 與 duplicate_skipped 都是 0）。
  // 語法：if / else if / else 多條件分支，由上往下第一個成立的執行。
  if (outcome === 'gap') {
    log({ msg: 'order_gap', key: event.order_id, seq: event.seq, partition, offset: message.offset });
  } else if (outcome === 'duplicate') {
    log({ msg: 'duplicate_skipped', key: event.order_id, seq: event.seq, partition, offset: message.offset });
  } else {
    log({ msg: 'stored', key: `${config.keyPrefix}:events:${event.order_id}`, seq: event.seq, status: event.status, partition, offset: message.offset });
  }
}

// 白話：關閉旗標，記錄「是否已經在關閉中」。SIGTERM 與 CRASH 可能同時觸發，第二次呼叫 shutdown 直接 return。
// 語法：let 宣告可以改值的變數；false / true 是布林值。
let stopping = false;

/**
 * 優雅關閉：離開 group、關閉 Redis 連線，然後結束 process。
 *
 * 呼叫者：
 *   SIGTERM / SIGINT 的 callback → shutdown(0)
 *   CRASH 事件的 callback（不可重試的錯誤）→ shutdown(1)
 *   啟動失敗的 catch → shutdown(1)
 *
 * 為什麼要先 consumer.disconnect()：主動離開 group，Kafka 立刻 rebalance，
 * 另一個實體馬上接手這些 partition；不離開的話要等 session timeout（預設 30 秒）。
 *
 * 迴圈裡的陣列是 [名稱, 關閉函式] 的配對，每個關閉函式都是子函式（箭頭函式）：
 *   () => consumer.disconnect()  離開 group 並斷開 Kafka
 *   () => redis.quit()           送 QUIT 後關閉 Redis 連線（等已送出的指令完成）
 * 兩個各自 try/catch：其中一個關閉失敗，另一個仍會關閉。
 *
 * 語法：code = 0 是參數預設值，呼叫 shutdown() 不帶參數時 code 就是 0。
 *       for (const [name, close] of 陣列) 逐一取出陣列元素；每個元素本身是 [名稱, 函式]，
 *       用 [name, close] 解構成兩個變數。close() 就是呼叫那個關閉函式。
 *
 * @param {number} [code=0]  process 結束碼：0 = 正常停止，1 = 異常（compose 依 restart 政策重啟）
 * @returns {Promise<void>}  實際上不會回來，最後會 process.exit
 */
async function shutdown(code = 0) {
  // 白話：已經在關閉中就不重複執行。
  if (stopping) return;
  stopping = true;
  // 白話：依序關閉 Kafka、Redis；任一個失敗只記 log，繼續關下一個。
  for (const [name, close] of [['kafka', () => consumer.disconnect()], ['redis', () => redis.quit()]]) {
    try {
      await close();
    } catch (err) {
      log({ msg: 'disconnect_failed', target: name, error: err.message });
    }
  }
  // 白話：記錄「已停止」與結束碼，然後結束程式（容器跟著停止）。
  log({ msg: 'stopped', code });
  process.exit(code);
}

// callback：Node.js 收到作業系統訊號時呼叫。
//   SIGTERM：docker compose stop、--scale 縮減實體時送出
//   SIGINT ：在終端機按 Ctrl+C
// 語法：() => shutdown(0) 是不帶參數的箭頭函式；登記時不執行，收到訊號才執行。
process.on('SIGTERM', () => shutdown(0));
process.on('SIGINT', () => shutdown(0));

/**
 * callback：kafkajs 的 consumer 發生錯誤而停止時呼叫。
 *
 * payload.restart 為 true：kafkajs 判斷可重試，會自己重啟 consumer，這裡不用處理。
 * payload.restart 為 false：不可重試（例如權限錯誤），記 log 後 shutdown(1)，交給 compose 重啟容器。
 *
 * 語法：payload.error?.message：error 不存在時得到 undefined，不會當掉。
 *
 * @param {object} event.payload.error    發生的錯誤
 * @param {boolean} event.payload.restart  kafkajs 是否會自己重啟
 */
consumer.on(consumer.events.CRASH, ({ payload }) => {
  if (!payload.restart) {
    log({ msg: 'consumer_crashed', error: payload.error?.message });
    shutdown(1);
  }
});

// 白話：真正開始連線與消費。上面的程式都只是「建立物件、登記 callback」，到這裡才開始動作。
//   順序：先連 Redis（寫入目標要先就緒）→ 連 Kafka → 訂閱 topic → 開始消費。
//   fromBeginning: true：group 第一次啟動、還沒有 committed offset 時，從最舊的訊息開始讀；
//     之後重啟會從上次 commit 的位置接著讀。
//   consumer.run({ eachMessage: handleMessage })：告訴 kafkajs「每筆訊息請呼叫 handleMessage」。
//     它啟動背景的消費迴圈後就回傳，不會一直卡在這行。
//   任何一步失敗（例如帳密錯誤）→ 進 catch → 記 log 並 shutdown(1)。
// 語法：這個檔案是 ES module（package.json 的 "type": "module"），所以可以在函式外面直接寫 await。
//       eachMessage: handleMessage 是把函式「本身」傳進去（沒有加括號），由 kafkajs 之後呼叫。
try {
  await redis.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: config.topic, fromBeginning: true });
  await consumer.run({ eachMessage: handleMessage });
} catch (err) {
  log({ msg: 'startup_failed', error: err.message });
  await shutdown(1);
}
// 白話：啟動完成，印出目前的設定（不含密碼），之後程式就一直等訊息進來。
log({ msg: 'running', topic: config.topic, groupId: config.groupId, kafkaUser: config.kafkaUsername, redisUser: config.redisUsername });
