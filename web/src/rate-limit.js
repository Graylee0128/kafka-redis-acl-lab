// =============================================================================
// 固定時間窗的簡易限流
// =============================================================================
//
// 誰呼叫：server.js 建立一個 limiter（每個 IP 每 60 秒 10 次），每筆申請先呼叫 allow(ip)。
//
// 固定時間窗：每個 key 從第一次請求開始計時 windowMs，期間最多 limit 次；
//   時間窗過了就重新計算。實作簡單，缺點是時間窗交界處可能短時間內達到 2 倍 limit，lab 可以接受。
//
// 單一實體、存在記憶體；lab 足夠，多實體需改用 Redis 等共享儲存。
// now 可以從外面傳入，測試時用假的時間（test/rate-limit.test.js）。
//
// 結構：createRateLimiter 是「工廠函式」，執行一次、回傳 allow 函式。
//   allow 是子函式（closure），記得外層的 windows、limit、windowMs，每次呼叫都共用同一份計數。
//
// 閱讀提示（本檔用到的 JavaScript 語法）：
//   new Map()            「鍵 → 值」的對照表：map.get(鍵) 讀、map.set(鍵, 值) 寫、map.size 筆數、map.clear() 清空
//   { a, b = 1 }         參數解構 + 預設值：呼叫端傳物件，取出 a、b；b 沒傳就用 1
//   { ...obj, c: 1 }     展開：複製 obj 的所有欄位，再覆蓋 c
// =============================================================================

/**
 * 建立一個限流器。
 *
 * 呼叫者：server.js 啟動時呼叫一次：createRateLimiter({ limit: 10, windowMs: 60_000 })，結果存在 allow 變數。
 *
 * @param {object} options
 * @param {number} options.limit      每個時間窗內每個 key 最多幾次（server.js 設 10）
 * @param {number} options.windowMs   時間窗長度，毫秒（server.js 設 60_000 = 60 秒）
 * @param {Function} [options.now=Date.now]  回傳目前時間（毫秒）的函式；測試時換成假的時間
 * @param {number} [options.maxKeys=10_000]   最多記住幾個 key，超過就整個清空，防止記憶體被撐爆
 * @returns {Function}  allow(key)：見下方
 */
export function createRateLimiter({ limit, windowMs, now = Date.now, maxKeys = 10_000 }) {
  // 白話：每個 key（IP）對應 { start: 時間窗開始時間, count: 這個時間窗內的次數 }。
  //   例如 '::ffff:172.18.0.1' → { start: 1791251016390, count: 3 }
  const windows = new Map();

  /**
   * 子函式（回傳給呼叫端的就是它）：判斷這次請求能不能放行，並記一次。
   *
   * 呼叫者：server.js 的 handleAccessRequest 每筆申請第一步：allow(ip)。
   *
   * @param {string} key  要限流的對象，server.js 傳的是來源 IP
   * @returns {boolean}  true = 放行；false = 超過上限（server.js 回 429）
   *
   * @example（limit: 2）
   *   allow('1.2.3.4') // → true （第 1 次）
   *   allow('1.2.3.4') // → true （第 2 次）
   *   allow('1.2.3.4') // → false（第 3 次，超過）
   *   // 60 秒後
   *   allow('1.2.3.4') // → true （新的時間窗）
   */
  return function allow(key) {
    // 白話：現在時間（毫秒）與這個 key 目前的紀錄（沒有紀錄時是 undefined）。
    const t = now();
    const current = windows.get(key);

    // 白話：第一次看到這個 key，或上一個時間窗已經過了 → 開新的時間窗，這次算第 1 次，放行。
    // 語法：!current 在 current 是 undefined 時為 true；|| 是「或」。
    if (!current || t - current.start >= windowMs) {
      if (windows.size >= maxKeys) windows.clear();  // 防止大量來源把記憶體撐爆
      windows.set(key, { start: t, count: 1 });
      return true;
    }
    // 白話：還在時間窗內，已達上限 → 拒絕。
    if (current.count >= limit) return false;
    // 白話：還在時間窗內、未達上限 → 次數 + 1，放行。
    // 語法：{ ...current, count: current.count + 1 } 複製一份再把 count 加 1。
    windows.set(key, { ...current, count: current.count + 1 });
    return true;
  };
}
