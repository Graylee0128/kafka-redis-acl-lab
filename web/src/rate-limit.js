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
// =============================================================================

export function createRateLimiter({ limit, windowMs, now = Date.now, maxKeys = 10_000 }) {
  // key（IP）→ { start: 時間窗開始時間, count: 這個時間窗內的次數 }
  const windows = new Map();

  // 回傳 true = 放行；false = 超過上限（server.js 回 429）
  return function allow(key) {
    const t = now();
    const current = windows.get(key);

    // 第一次看到這個 key，或上一個時間窗已經過了 → 開新的時間窗，這次算第 1 次
    if (!current || t - current.start >= windowMs) {
      if (windows.size >= maxKeys) windows.clear();  // 防止大量來源把記憶體撐爆
      windows.set(key, { start: t, count: 1 });
      return true;
    }
    // 還在時間窗內：已達上限就拒絕，否則次數 + 1
    if (current.count >= limit) return false;
    windows.set(key, { ...current, count: current.count + 1 });
    return true;
  };
}
