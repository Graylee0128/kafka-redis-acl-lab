// =============================================================================
// Producer：定期產生模擬訂單事件，以 order_id 作為 Message Key 送進 Kafka。
// =============================================================================
//
// 誰執行：producer 容器（producer/Dockerfile 的 CMD ["node", "src/index.js"]），compose 起 1 個實體。
// 何時跑：kafka-init 成功結束後啟動，常駐；restart: unless-stopped。
// 用哪個帳號：Kafka 的 producer（ACL 只有 topic orders 的 Write、Describe）。
//
// 流程：讀設定 → 連 Kafka → 每 intervalMs 送一筆 → 收到 SIGTERM 時斷線並結束。
//
// 順序性的三個關鍵：
//   1. Message Key = order_id → DefaultPartitioner（Java 相容 murmur2）把同一個 key 固定送到同一個 partition
//   2. maxInFlightRequests = 1 → 重試時不會讓後送的訊息超車
//   3. 逐筆 await send 再排下一筆 → 應用層也不會亂序
//
// log 一律是一行一個 JSON（例如 {"ts":...,"msg":"sent",...}），方便用 grep / jq 過濾。
//
// 本檔的函式與誰呼叫它們：
//   log         ← 本檔所有地方
//   produceOne  ← 啟動時呼叫第一次；之後由 setTimeout 每隔 intervalMs 再呼叫自己
//   shutdown    ← SIGTERM / SIGINT、送出失敗
//
// 閱讀提示：這支程式「怎麼跑起來」
//   1. node src/index.js 從第一行往下執行：讀設定、建 Kafka 物件、登記訊號處理、連線，最後呼叫 produceOne()。
//   2. produceOne 送完一筆後用 setTimeout 預約「1 秒後再呼叫 produceOne」，形成一個不停的循環。
//   3. 收到 SIGTERM（docker compose stop）就呼叫 shutdown，取消預約、斷線、結束程式。
//
// 閱讀提示（本檔用到的 JavaScript / Node.js 語法）：
//   import x from 'y'          載入套件或其他檔案
//   process                    Node.js 內建物件：process.env 環境變數、process.exit(碼) 結束、process.on 收訊號
//   async / await              await 等網路操作（例如送出訊息）完成再往下一行
//   setTimeout(函式, 毫秒)     預約「過幾毫秒後呼叫函式」，回傳一個計時器，可用 clearTimeout 取消
//   const [a] = 陣列           陣列解構：取出第一個元素放進 a
//   new 類別(...)              建立物件，例如 new Kafka({...})
// =============================================================================

// 白話：載入 kafkajs 套件的三樣東西，以及本專案的兩個檔案。
//   Kafka        → 建立 Kafka client 的類別
//   Partitioners → 分區器（決定訊息進哪個 partition）
//   logLevel     → kafkajs 自己的 log 等級常數
import { Kafka, Partitioners, logLevel } from 'kafkajs';
import { loadConfig } from './config.js';
import { createOrderGenerator } from './orders.js';

/**
 * 印出一行 JSON log。
 *
 * 呼叫者：本檔所有需要記錄的地方。
 *
 * 語法：...fields 把呼叫端傳的欄位展開進來，再加上時間戳記 ts。
 *
 * @param {object} fields  要記錄的欄位，慣例上一定有 msg，例如 { msg: 'sent', key, seq }
 * @returns {void}
 *
 * @example
 *   log({ msg: 'sent', seq: 2 })  // 印出 {"ts":"2026-10-06T...","msg":"sent","seq":2}
 */
const log = (fields) => console.log(JSON.stringify({ ts: new Date().toISOString(), ...fields }));

// 白話：讀設定。設定有錯就直接結束（exit 1），docker compose logs 會看到原因。
//   只印 err.message：config.js 的訊息只有欄位名稱，不含密碼。
// 語法：let config; 先宣告，因為要在 try 裡面才指定值，而 try 外面也要用到它。
let config;
try {
  config = loadConfig(process.env);
} catch (err) {
  console.error(`設定錯誤：${err.message}`);
  process.exit(1);
}

// 白話：建立 Kafka client 物件（這時還沒連線，下面 producer.connect() 才真的連）。
//   ssl: false：lab 沒有 TLS（對應 broker 的 SASL_PLAINTEXT）
//   sasl：對應 start.sh 的 INTERNAL listener：SCRAM-SHA-512。帳號必須是 init.sh 建過的
//   logLevel.WARN：kafkajs 預設 INFO 很吵，只印警告以上
//   retry.retries: 10：連線或送出失敗時自動重試的次數（間隔會指數拉長）
const kafka = new Kafka({
  clientId: config.clientId,
  brokers: config.brokers,
  ssl: false,
  sasl: { mechanism: 'scram-sha-512', username: config.username, password: config.password },
  logLevel: logLevel.WARN,
  retry: { retries: 10 },
});

// 白話：從 Kafka client 建立 producer，三個設定都跟順序性有關：
//   createPartitioner：與 Java client 相同的 murmur2 雜湊，相同 key → 相同 partition
//   maxInFlightRequests: 1：同一時間最多 1 個未確認的請求。
//     若允許多個，第 1 批重試時第 2 批可能先寫入 → 亂序
//   allowAutoTopicCreation: false：不讓 client 自動建 topic（broker 端也關了 auto.create.topics.enable）
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
  maxInFlightRequests: 1,
  allowAutoTopicCreation: false,
});

// 白話：產生這次啟動專屬的 runId，例如 'omut8ampr'。
//   runId 讓每次啟動的 order_id 都不同，重啟後 seq 從 1 開始也不會跟舊訊息混淆。
// 語法：Date.now() 是現在的毫秒時間戳（一個大整數）；.toString(36) 轉成 36 進位（0-9a-z），字串比較短。
const runId = `o${Date.now().toString(36)}`;
// 白話：呼叫 orders.js 的工廠函式，得到 nextEvent 函式；之後每呼叫一次 nextEvent() 就得到一筆新事件。
const nextEvent = createOrderGenerator({ activeOrders: config.activeOrders, runId });

// 白話：兩個狀態變數。
//   timer   ：下一次送出的計時器，關閉時要用 clearTimeout 取消它
//   stopping：是否已經在關閉中，避免關閉流程跑兩次，也讓 produceOne 知道不要再預約下一筆
// 語法：let timer; 宣告但先不給值（值是 undefined）。
let timer;
let stopping = false;

/**
 * 送出一筆事件，成功後預約下一筆。
 *
 * 呼叫者：檔案最後一行呼叫第一次；之後由 setTimeout 每隔 config.intervalMs 呼叫。
 * 會呼叫：nextEvent（orders.js 的 next）、producer.send（kafkajs）、log；送出失敗時呼叫 shutdown(1)。
 *
 * 為什麼用 setTimeout 而不用 setInterval：
 *   setInterval 不管上一筆送完沒有都會準時觸發，可能兩筆同時在送 → 順序亂掉。
 *   這裡是「送完才預約下一筆」，任何時候只有一筆在送。
 *
 * 送出失敗的處理：kafkajs 內部已重試；仍失敗代表這筆遺失，繼續送會讓該訂單 seq 跳號
 *   → 直接退出讓 compose 重啟（換新 runId，新訂單從 seq 1 開始）。
 *
 * @returns {Promise<void>}
 */
async function produceOne() {
  // 白話：拿下一筆事件，例如 { order_id: 'omut8ampr-0003', seq: 2, status: 'paid', ... }
  const event = nextEvent();
  try {
    // 白話：送出訊息並等 Kafka 確認寫入。
    //   acks: -1（all）：等所有同步副本都寫入才算成功（單節點就是等 leader 寫入）
    //   key：決定 partition（相同 order_id 永遠同一個 partition）
    //   value：訊息內容必須是字串，所以用 JSON.stringify 把物件轉成 JSON 字串
    // 語法：send 回傳「每個 partition 的寫入結果」陣列；這裡只送 1 筆，
    //       const [result] = ... 用陣列解構取出第一個元素。
    const [result] = await producer.send({
      topic: config.topic,
      acks: -1,
      messages: [{ key: event.order_id, value: JSON.stringify(event) }],
    });
    // 白話：記錄送出成功，包含實際落在哪個 partition、哪個 offset（驗證順序性時看這行）。
    log({ msg: 'sent', key: event.order_id, seq: event.seq, status: event.status, partition: result.partition, offset: result.baseOffset });
  } catch (err) {
    // 白話：送出失敗 → 記 log、關閉程式。return 讓函式到此結束，不再預約下一筆。
    log({ msg: 'send_failed', key: event.order_id, seq: event.seq, error: err.message });
    await shutdown(1);
    return;
  }
  // 白話：還沒在關閉中，就預約 intervalMs 毫秒後再呼叫一次 produceOne，形成循環。
  // 語法：setTimeout(produceOne, 1000) 傳的是函式本身（沒有括號），1 秒後由 Node.js 呼叫。
  if (!stopping) timer = setTimeout(produceOne, config.intervalMs);
}

/**
 * 優雅關閉：停止排程 → 斷開 Kafka（送完緩衝中的請求）→ 結束 process。
 *
 * 呼叫者：SIGTERM / SIGINT 的 callback → shutdown(0)；produceOne 送出失敗 → shutdown(1)。
 *
 * 驗證時看得到 {"msg":"stopped","code":0}，代表是正常關閉。
 *
 * 語法：code = 0 是參數預設值，呼叫 shutdown() 不帶參數時 code 就是 0。
 *
 * @param {number} [code=0]  process 結束碼：0 = 正常停止，1 = 異常（compose 依 restart 政策重啟）
 * @returns {Promise<void>}  實際上不會回來，最後會 process.exit
 */
async function shutdown(code = 0) {
  // 白話：已經在關閉中就不重複執行；否則標記為關閉中。
  if (stopping) return;
  stopping = true;
  // 白話：取消已預約的下一筆送出。
  clearTimeout(timer);
  // 白話：斷開 Kafka 連線；失敗也只記 log，繼續往下結束程式。
  try {
    await producer.disconnect();
  } catch (err) {
    log({ msg: 'disconnect_failed', error: err.message });
  }
  log({ msg: 'stopped', code });
  // 白話：結束程式，容器跟著停止。
  process.exit(code);
}

// callback：Node.js 收到作業系統訊號時呼叫。
//   SIGTERM：docker compose stop 送出
//   SIGINT ：在終端機按 Ctrl+C
// 語法：process.on('訊號', 函式) 是「登記」，這行執行時不會呼叫 shutdown，收到訊號才呼叫。
process.on('SIGTERM', () => shutdown(0));
process.on('SIGINT', () => shutdown(0));

// 白話：真正連上 Kafka。帳密錯誤、broker 連不上都會進 catch；exit 1 讓 compose 依 restart 政策重啟。
// 語法：這個檔案是 ES module（package.json 的 "type": "module"），可以在函式外面直接 await。
try {
  await producer.connect();
} catch (err) {
  log({ msg: 'connect_failed', error: err.message });
  process.exit(1);
}
// 白話：連線成功，記錄目前設定（不含密碼），然後送出第一筆；之後由 produceOne 自己循環。
log({ msg: 'connected', brokers: config.brokers, topic: config.topic, user: config.username, runId });
produceOne();
