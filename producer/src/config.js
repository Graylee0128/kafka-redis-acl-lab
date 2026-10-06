// =============================================================================
// Producer 設定：從環境變數讀取並驗證
// =============================================================================
//
// 誰呼叫：index.js 啟動時呼叫 loadConfig(process.env)。
// 值從哪來：docker-compose.yml 的 producer.environment（密碼來自 .env）。
// 錯誤訊息只列欄位名稱，不帶值，避免把密碼印進 log。
//
// 參數 env 由呼叫端傳入而不是直接讀 process.env：測試時可以傳假的物件（test/config.test.js）。
// =============================================================================

// 必填：少一個就無法連上 Kafka，啟動時就該失敗（fail-fast），而不是連線時才報奇怪的錯
const REQUIRED = ['KAFKA_BROKERS', 'KAFKA_USERNAME', 'KAFKA_PASSWORD'];

// 讀取「正整數」型的選填設定；沒設定（undefined）或空字串就用預設值 fallback。
// 環境變數一律是字串，所以要用 Number() 轉成數字再檢查。
function positiveInt(env, name, fallback) {
  const raw = env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  // Number('abc') 是 NaN、Number('1.5') 是 1.5，都不是整數 → 報錯，而不是默默用錯的值跑
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} 必須是正整數`);
  }
  return value;
}

export function loadConfig(env) {
  // filter 留下「值是空的」欄位名稱；!env[name] 同時涵蓋 undefined 與空字串
  const missing = REQUIRED.filter((name) => !env[name]);
  if (missing.length > 0) {
    throw new Error(`缺少必要環境變數：${missing.join(', ')}`);
  }

  // Object.freeze：設定物件唯讀，程式其他地方不會不小心改到它
  return Object.freeze({
    // "kafka:9092, kafka2:9092" → ['kafka:9092', 'kafka2:9092']
    //   split 切開、trim 去空白、filter(Boolean) 丟掉空字串（例如結尾多一個逗號）
    brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()).filter(Boolean),
    // → init.sh 建立的 producer 帳號（ACL：topic orders 的 Write、Describe）
    username: env.KAFKA_USERNAME,
    password: env.KAFKA_PASSWORD,
    // a || b：a 是空值（undefined、空字串）時用 b 當預設
    topic: env.KAFKA_TOPIC || 'orders',
    // clientId 會出現在 broker 端的 log 與 metrics，用來辨識是哪個程式連進來
    clientId: env.KAFKA_CLIENT_ID || 'order-producer',
    // 每隔幾毫秒送一筆
    intervalMs: positiveInt(env, 'PRODUCE_INTERVAL_MS', 1000),
    // 同時進行中的訂單數（見 orders.js）
    activeOrders: positiveInt(env, 'ACTIVE_ORDERS', 5),
  });
}
