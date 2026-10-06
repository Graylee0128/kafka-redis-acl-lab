// =============================================================================
// Producer：定期產生模擬訂單事件，以 order_id 作為 Message Key 送進 Kafka。
// =============================================================================
//
// 誰執行：producer 容器（producer/Dockerfile 的 CMD），compose 起 1 個實體。
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
// =============================================================================
import { Kafka, Partitioners, logLevel } from 'kafkajs';
import { loadConfig } from './config.js';
import { createOrderGenerator } from './orders.js';

// ...fields 把呼叫端傳的欄位展開進來，再加上時間戳記
const log = (fields) => console.log(JSON.stringify({ ts: new Date().toISOString(), ...fields }));

// 設定有錯就直接結束（exit 1），docker compose logs 會看到原因。
// 只印 err.message：config.js 的訊息只有欄位名稱，不含密碼。
let config;
try {
  config = loadConfig(process.env);
} catch (err) {
  console.error(`設定錯誤：${err.message}`);
  process.exit(1);
}

const kafka = new Kafka({
  clientId: config.clientId,
  brokers: config.brokers,
  // lab 沒有 TLS（對應 broker 的 SASL_PLAINTEXT）
  ssl: false,
  // 對應 start.sh 的 INTERNAL listener：SCRAM-SHA-512。帳號必須是 init.sh 建過的
  sasl: { mechanism: 'scram-sha-512', username: config.username, password: config.password },
  // kafkajs 預設 INFO 很吵，只印警告以上
  logLevel: logLevel.WARN,
  // 連線或送出失敗時自動重試的次數（間隔會指數拉長）
  retry: { retries: 10 },
});

// createPartitioner：與 Java client 相同的 murmur2 雜湊，相同 key → 相同 partition
// maxInFlightRequests: 1：同一時間最多 1 個未確認的請求。
//   若允許多個，第 1 批重試時第 2 批可能先寫入 → 亂序
// allowAutoTopicCreation: false：不讓 client 自動建 topic（broker 端也關了 auto.create.topics.enable）
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
  maxInFlightRequests: 1,
  allowAutoTopicCreation: false,
});

// runId 讓每次啟動的 order_id 都不同，重啟後 seq 從 1 開始也不會跟舊訊息混淆。
// Date.now().toString(36) 把毫秒時間戳轉成 36 進位（0-9a-z），字串比較短
const runId = `o${Date.now().toString(36)}`;
const nextEvent = createOrderGenerator({ activeOrders: config.activeOrders, runId });

// timer：下一次送出的計時器，關閉時要清掉；stopping：避免關閉流程跑兩次
let timer;
let stopping = false;

// 送一筆，成功後才用 setTimeout 排下一筆。
// 不用 setInterval：上一筆還沒送完時不會疊上下一筆，順序與節奏都由這裡控制。
async function produceOne() {
  const event = nextEvent();
  try {
    // send 回傳每個 partition 的寫入結果陣列；這裡只送 1 筆，用解構取第一個元素
    //   acks: -1（all）：等所有同步副本都寫入才算成功（單節點就是等 leader 寫入）
    //   key 決定 partition；value 必須是字串或 Buffer，所以 JSON.stringify
    const [result] = await producer.send({
      topic: config.topic,
      acks: -1,
      messages: [{ key: event.order_id, value: JSON.stringify(event) }],
    });
    log({ msg: 'sent', key: event.order_id, seq: event.seq, status: event.status, partition: result.partition, offset: result.baseOffset });
  } catch (err) {
    // kafkajs 內部已重試；仍失敗代表這筆遺失，繼續送會讓該訂單 seq 跳號 → 直接退出讓 compose 重啟（換新 runId）
    log({ msg: 'send_failed', key: event.order_id, seq: event.seq, error: err.message });
    await shutdown(1);
    return;
  }
  if (!stopping) timer = setTimeout(produceOne, config.intervalMs);
}

// 優雅關閉：停止排程 → 斷開 Kafka（送完緩衝中的請求）→ 結束 process。
// 驗證時看得到 {"msg":"stopped","code":0}，代表是正常關閉。
async function shutdown(code = 0) {
  if (stopping) return;
  stopping = true;
  clearTimeout(timer);
  try {
    await producer.disconnect();
  } catch (err) {
    log({ msg: 'disconnect_failed', error: err.message });
  }
  log({ msg: 'stopped', code });
  process.exit(code);
}

// docker compose stop 送 SIGTERM；在終端機按 Ctrl+C 是 SIGINT
process.on('SIGTERM', () => shutdown(0));
process.on('SIGINT', () => shutdown(0));

// 這個檔案是 ES module（package.json 的 "type": "module"），可以在最外層直接 await。
// 帳密錯誤、broker 連不上都會進 catch；exit 1 讓 compose 依 restart 政策重啟。
try {
  await producer.connect();
} catch (err) {
  log({ msg: 'connect_failed', error: err.message });
  process.exit(1);
}
log({ msg: 'connected', brokers: config.brokers, topic: config.topic, user: config.username, runId });
produceOne();
