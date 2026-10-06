// =============================================================================
// Consumer 設定：從環境變數讀取並驗證
// =============================================================================
//
// 誰呼叫：index.js 啟動時呼叫 loadConfig(process.env, os.hostname())。
// 值從哪來：docker-compose.yml 的 consumer.environment（密碼來自 .env）。
// 錯誤訊息只列欄位名稱，不帶值，避免把密碼印進 log。
//
// 兩組帳號：
//   Kafka consumer：ACL 只有 topic orders 的 Read、Describe，以及 group order-consumers 的 Read
//   Redis app     ：只能碰 myapp:* 的 key，指令白名單（見 infra/redis/entrypoint.sh）
// =============================================================================

// 必填：Kafka 與 Redis 的連線位址與帳密，少一個就無法運作
const REQUIRED = [
  'KAFKA_BROKERS', 'KAFKA_USERNAME', 'KAFKA_PASSWORD',
  'REDIS_URL', 'REDIS_USERNAME', 'REDIS_PASSWORD',
];

// 讀取「正整數」型的選填設定；沒設定或空字串就用預設值 fallback。
// 環境變數一律是字串，Number() 轉成數字後再檢查是不是正整數。
function positiveInt(env, name, fallback) {
  const raw = env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} 必須是正整數`);
  }
  return value;
}

// hostname 由呼叫端傳入（容器內即 container id），方便測試
export function loadConfig(env, hostname) {
  // !env[name] 同時涵蓋 undefined 與空字串
  const missing = REQUIRED.filter((name) => !env[name]);
  if (missing.length > 0) {
    throw new Error(`缺少必要環境變數：${missing.join(', ')}`);
  }

  // Object.freeze：設定物件唯讀，程式其他地方不會不小心改到它
  return Object.freeze({
    // 逗號分隔的 broker 清單 → 陣列（去空白、丟掉空字串）
    brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()).filter(Boolean),
    kafkaUsername: env.KAFKA_USERNAME,
    kafkaPassword: env.KAFKA_PASSWORD,
    topic: env.KAFKA_TOPIC || 'orders',
    // 必須與 init.sh 設 ACL 的 group 名稱完全一致，否則加入 group 時得到 GroupAuthorizationException。
    // 兩個實體用同一個 groupId，Kafka 才會把 partition 分給它們（負載平衡）。
    groupId: env.KAFKA_GROUP_ID || 'order-consumers',
    // 例如 redis://redis:6379（compose 網路內的服務名稱）
    redisUrl: env.REDIS_URL,
    redisUsername: env.REDIS_USERNAME,
    redisPassword: env.REDIS_PASSWORD,
    // Redis key 前綴，必須與 app 帳號的 ~myapp:* 一致，否則寫入得到 NOPERM
    keyPrefix: env.REDIS_KEY_PREFIX || 'myapp',
    // 寫進 Redis 的 key 保留多久（秒），預設 1 天，避免 lab 一直跑把記憶體塞滿
    ttlSeconds: positiveInt(env, 'REDIS_TTL_SECONDS', 86400),
    // 寫進 handled_by 與 log，用來分辨是哪個實體處理的（驗證負載平衡）
    consumerId: env.CONSUMER_ID || hostname,
  });
}
