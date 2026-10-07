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
//
// 閱讀提示（本檔用到的 JavaScript 語法）：
//   const X = ...      宣告一個「不能重新指定」的變數（類似 shell 的 readonly）
//   [ 'a', 'b' ]       陣列（array），類似 bash 的 ( a b )
//   { key: value }     物件（object），一組「名稱: 值」，類似 JSON / YAML 的 mapping
//   function f(a, b)   定義函式；呼叫時寫 f(1, 2)
//   export             讓別的檔案可以 import 這個東西；沒有 export 的只能在本檔內用
//   ===  !==           「完全相等 / 不相等」（型別也要一樣；JS 的 == 會自動轉型，容易出錯，所以一律用 ===）
//   `文字 ${變數}`     樣板字串（反引號），${} 裡的值會被代入，類似 shell 的 "文字 $VAR"
// =============================================================================

// 白話：必填的環境變數名稱清單。少任何一個，consumer 就連不上 Kafka 或 Redis。
// 語法：const 宣告常數；[ ... ] 是陣列，元素用逗號分隔，可以換行寫。
const REQUIRED = [
  'KAFKA_BROKERS', 'KAFKA_USERNAME', 'KAFKA_PASSWORD',
  'REDIS_URL', 'REDIS_USERNAME', 'REDIS_PASSWORD',
];

/**
 * 讀取「正整數」型的選填設定。
 *
 * 呼叫者：本檔的 loadConfig（目前只用在 REDIS_TTL_SECONDS）。不 export，外部用不到。
 *
 * 環境變數一律是字串，所以先用 Number() 轉成數字再檢查。
 * 填錯（例如 'abc'、'1.5'、'0'）就丟例外讓啟動失敗，而不是默默用錯的值跑。
 *
 * @param {object} env       環境變數物件（loadConfig 收到的 env）
 * @param {string} name      變數名稱，例如 'REDIS_TTL_SECONDS'；也用在錯誤訊息裡
 * @param {number} fallback  沒設定（undefined）或空字串時使用的預設值
 * @returns {number}  正整數
 * @throws {Error}  值不是正整數時，訊息只含變數名稱，不含值
 *
 * @example
 *   positiveInt({}, 'REDIS_TTL_SECONDS', 86400)                          // → 86400
 *   positiveInt({ REDIS_TTL_SECONDS: '60' }, 'REDIS_TTL_SECONDS', 86400) // → 60
 *   positiveInt({ REDIS_TTL_SECONDS: '0' }, 'REDIS_TTL_SECONDS', 86400)  // → 丟出例外
 */
function positiveInt(env, name, fallback) {
  // 白話：從 env 物件取出名稱為 name 的值，例如 env['REDIS_TTL_SECONDS'] → '60'（字串）。
  // 語法：物件[變數] 用「變數的值」當欄位名稱去取值；如果寫 env.name 會去找名叫 "name" 的欄位。
  const raw = env[name];
  // 白話：沒設定（undefined = 根本沒有這個欄位）或設成空字串，就直接回傳預設值，函式到此結束。
  // 語法：|| 是「或」；if (條件) return 值; 是單行 if，條件成立就回傳並離開函式。
  if (raw === undefined || raw === '') return fallback;
  // 白話：把字串轉成數字。'60' → 60；'abc' → NaN（Not a Number，代表轉換失敗）。
  const value = Number(raw);
  // 白話：不是整數（含 NaN、1.5）或小於等於 0，就丟出錯誤。
  // 語法：! 是「非」；Number.isInteger(x) 判斷 x 是不是整數，回傳 true / false。
  if (!Number.isInteger(value) || value <= 0) {
    // 語法：throw 丟出例外，函式立刻中止，錯誤往上傳給呼叫者（最後由 index.js 的 try/catch 接住）。
    //       new Error('訊息') 建立一個錯誤物件，之後可以用 err.message 讀到訊息。
    throw new Error(`${name} 必須是正整數`);
  }
  // 白話：通過檢查，回傳轉好的數字。
  return value;
}

/**
 * 讀取並驗證 consumer 的全部設定。
 *
 * 呼叫者：index.js 啟動時呼叫一次；失敗時 index.js 印出 err.message 並 exit(1)。
 * 會呼叫：本檔的 positiveInt。
 *
 * 參數由呼叫端傳入而不是直接讀 process.env、os.hostname()：
 *   測試時可以傳假的值（test/config.test.js）。
 *
 * @param {object} env       環境變數，正式執行時是 process.env
 * @param {string} hostname  容器 hostname（容器內即 container id），CONSUMER_ID 沒設定時當 consumerId
 * @returns {Readonly<object>}  凍結的設定物件，欄位見下方 return
 * @throws {Error}  缺少必填變數（訊息列出缺哪些），或 REDIS_TTL_SECONDS 不是正整數
 */
export function loadConfig(env, hostname) {
  // 白話：找出 REQUIRED 裡「env 沒有值」的那些名稱，例如 ['REDIS_URL']；全部都有就是空陣列 []。
  // 語法：陣列.filter(函式) 會對每個元素呼叫函式，留下回傳 true 的元素，組成新陣列。
  //       (name) => !env[name] 是「箭頭函式」，等於 function (name) { return !env[name]; }
  //       !env[name]：值是 undefined 或空字串時為 true（JS 把空字串、undefined、0 都當成「假」）。
  const missing = REQUIRED.filter((name) => !env[name]);
  // 白話：有缺就丟錯誤，訊息例如「缺少必要環境變數：REDIS_URL, REDIS_PASSWORD」。
  // 語法：陣列.length 是元素個數；陣列.join(', ') 把元素用 ', ' 串成一個字串。
  if (missing.length > 0) {
    throw new Error(`缺少必要環境變數：${missing.join(', ')}`);
  }

  // 白話：組出設定物件並回傳。之後 index.js 用 config.topic、config.groupId 等方式讀取。
  // 語法：Object.freeze(物件) 把物件「凍結」成唯讀，之後任何地方想改 config.topic = 'x' 都不會生效。
  //       return { 欄位: 值, ... } 回傳一個物件；每一行「名稱: 值,」就是一個欄位。
  return Object.freeze({
    // 白話：'kafka:9092, kafka2:9092' → ['kafka:9092', 'kafka2:9092']
    // 語法：連續呼叫（method chaining），由左往右依序執行：
    //       .split(',')        用逗號切成陣列        → ['kafka:9092', ' kafka2:9092']
    //       .map((b) => b.trim())  每個元素去掉前後空白 → ['kafka:9092', 'kafka2:9092']
    //       .filter(Boolean)   丟掉空字串（例如結尾多一個逗號時切出來的 ''）
    brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()).filter(Boolean),
    // 白話：Kafka 帳密，對應 init.sh 建的 consumer 帳號。
    // 語法：env.KAFKA_USERNAME 等於 env['KAFKA_USERNAME']，欄位名稱固定時用點號比較簡潔。
    kafkaUsername: env.KAFKA_USERNAME,
    kafkaPassword: env.KAFKA_PASSWORD,
    // 白話：要讀的 topic；沒設定就用 'orders'。
    // 語法：a || b：a 有值就用 a，a 是空的（undefined、空字串）就用 b。常用來寫「預設值」。
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
    // 寫進 Redis 的 key 保留多久（秒），預設 1 天，避免 lab 一直跑把記憶體塞滿。
    // 白話：呼叫上面的 positiveInt 檢查；填錯就在這裡丟錯誤，整個 loadConfig 失敗。
    ttlSeconds: positiveInt(env, 'REDIS_TTL_SECONDS', 86400),
    // 寫進 handled_by 與 log，用來分辨是哪個實體處理的（驗證負載平衡）
    consumerId: env.CONSUMER_ID || hostname,
  });
}
