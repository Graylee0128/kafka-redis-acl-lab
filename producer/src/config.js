// =============================================================================
// Producer 設定：從環境變數讀取並驗證
// =============================================================================
//
// 誰呼叫：index.js 啟動時呼叫 loadConfig(process.env)。
// 值從哪來：docker-compose.yml 的 producer.environment（密碼來自 .env）。
// 錯誤訊息只列欄位名稱，不帶值，避免把密碼印進 log。
//
// 參數 env 由呼叫端傳入而不是直接讀 process.env：測試時可以傳假的物件（test/config.test.js）。
//
// 閱讀提示（本檔用到的 JavaScript 語法）：
//   const X = ...      宣告一個「不能重新指定」的變數（類似 shell 的 readonly）
//   [ 'a', 'b' ]       陣列（array），類似 bash 的 ( a b )
//   { key: value }     物件（object），一組「名稱: 值」，類似 JSON / YAML 的 mapping
//   function f(a, b)   定義函式；呼叫時寫 f(1, 2)
//   export             讓別的檔案可以 import 這個東西；沒有 export 的只能在本檔內用
//   ===  !==           「完全相等 / 不相等」（型別也要一樣；JS 的 == 會自動轉型，容易出錯，所以一律用 ===）
//   `文字 ${變數}`     樣板字串（反引號），${} 裡的值會被代入，類似 shell 的 "文字 $VAR"
//   (x) => 運算式      箭頭函式，等於 function (x) { return 運算式; }
// =============================================================================

// 白話：必填的環境變數名稱。少一個就無法連上 Kafka，啟動時就該失敗（fail-fast），而不是連線時才報奇怪的錯。
const REQUIRED = ['KAFKA_BROKERS', 'KAFKA_USERNAME', 'KAFKA_PASSWORD'];

/**
 * 讀取「正整數」型的選填設定。
 *
 * 呼叫者：本檔的 loadConfig（PRODUCE_INTERVAL_MS、ACTIVE_ORDERS）。不 export，外部用不到。
 *
 * 環境變數一律是字串，所以先用 Number() 轉成數字再檢查。
 * 填錯（例如 'abc'、'1.5'、'0'）就丟例外讓啟動失敗，而不是默默用錯的值跑。
 *
 * @param {object} env       環境變數物件（loadConfig 收到的 env）
 * @param {string} name      變數名稱，例如 'ACTIVE_ORDERS'；也用在錯誤訊息裡
 * @param {number} fallback  沒設定（undefined）或空字串時使用的預設值
 * @returns {number}  正整數
 * @throws {Error}  值不是正整數時，訊息只含變數名稱，不含值
 *
 * @example
 *   positiveInt({}, 'ACTIVE_ORDERS', 5)                      // → 5
 *   positiveInt({ ACTIVE_ORDERS: '3' }, 'ACTIVE_ORDERS', 5)  // → 3
 *   positiveInt({ ACTIVE_ORDERS: 'abc' }, 'ACTIVE_ORDERS', 5) // → 丟出例外
 */
function positiveInt(env, name, fallback) {
  // 白話：取出名稱為 name 的環境變數值，例如 env['ACTIVE_ORDERS'] → '5'（字串）。
  // 語法：物件[變數] 用「變數的值」當欄位名稱；寫 env.name 會去找名叫 "name" 的欄位。
  const raw = env[name];
  // 白話：沒設定或空字串 → 回傳預設值，函式到此結束。
  // 語法：|| 是「或」；if (條件) return 值; 是單行 if。
  if (raw === undefined || raw === '') return fallback;
  // 白話：字串轉數字。'5' → 5；'abc' → NaN（轉換失敗）。
  const value = Number(raw);
  // 白話：Number('abc') 是 NaN、Number('1.5') 是 1.5，都不是整數 → 報錯，而不是默默用錯的值跑
  // 語法：! 是「非」；Number.isInteger(x) 回傳 true / false。
  if (!Number.isInteger(value) || value <= 0) {
    // 語法：throw 丟出例外，函式立刻中止，錯誤往上傳（最後由 index.js 的 try/catch 接住）。
    throw new Error(`${name} 必須是正整數`);
  }
  return value;
}

/**
 * 讀取並驗證 producer 的全部設定。
 *
 * 呼叫者：index.js 啟動時呼叫一次；失敗時 index.js 印出 err.message 並 exit(1)。
 * 會呼叫：本檔的 positiveInt。
 *
 * @param {object} env  環境變數，正式執行時是 process.env
 * @returns {Readonly<object>}  凍結的設定物件，欄位見下方 return
 * @throws {Error}  缺少必填變數，或 PRODUCE_INTERVAL_MS / ACTIVE_ORDERS 不是正整數
 */
export function loadConfig(env) {
  // 白話：找出 REQUIRED 裡「env 沒有值」的名稱；全部都有就是空陣列 []。
  // 語法：陣列.filter(函式) 留下函式回傳 true 的元素；!env[name] 在 undefined 或空字串時為 true。
  const missing = REQUIRED.filter((name) => !env[name]);
  // 白話：有缺就丟錯誤，訊息例如「缺少必要環境變數：KAFKA_PASSWORD」。
  // 語法：陣列.length 是元素個數；陣列.join(', ') 把元素串成一個字串。
  if (missing.length > 0) {
    throw new Error(`缺少必要環境變數：${missing.join(', ')}`);
  }

  // 白話：組出設定物件並回傳。之後 index.js 用 config.topic、config.intervalMs 等方式讀取。
  // 語法：Object.freeze 把物件凍結成唯讀，程式其他地方不會不小心改到它。
  return Object.freeze({
    // "kafka:9092, kafka2:9092" → ['kafka:9092', 'kafka2:9092']
    //   split 切開、trim 去空白、filter(Boolean) 丟掉空字串（例如結尾多一個逗號）
    // 語法：由左往右連續呼叫，每一步的結果交給下一步。
    brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()).filter(Boolean),
    // → init.sh 建立的 producer 帳號（ACL：topic orders 的 Write、Describe）
    username: env.KAFKA_USERNAME,
    password: env.KAFKA_PASSWORD,
    // 語法：a || b：a 是空值（undefined、空字串）時用 b 當預設
    topic: env.KAFKA_TOPIC || 'orders',
    // clientId 會出現在 broker 端的 log 與 metrics，用來辨識是哪個程式連進來
    clientId: env.KAFKA_CLIENT_ID || 'order-producer',
    // 每隔幾毫秒送一筆（compose 設 1000 = 每秒一筆）
    intervalMs: positiveInt(env, 'PRODUCE_INTERVAL_MS', 1000),
    // 同時進行中的訂單數（見 orders.js）
    activeOrders: positiveInt(env, 'ACTIVE_ORDERS', 5),
  });
}
