// =============================================================================
// 訊息解析與順序判斷（純函式，不碰 Kafka / Redis，方便測試）
// =============================================================================
//
// 誰呼叫：
//   parseEvent  ← index.js 的 handleMessage，每收到一筆 Kafka 訊息就先驗證
//   classifySeq ← store.js，寫入 Redis 前判斷這筆是正常、重複還是跳號
//
// 為什麼要驗證：Kafka 訊息是外部輸入，任何有 Write 權限的人都能送任意內容。
// 不合法的訊息回報原因，由呼叫端記 log 後跳過，不讓一筆壞訊息卡住整個 partition
// （如果丟出例外，kafkajs 會一直重試同一筆，後面的訊息全部處理不到）。
// =============================================================================

// order_id 會被拼進 Redis key（myapp:events:<order_id>），只允許安全字元，
// 避免拼出意外的 key（例如含冒號、空白或 * 這類 pattern 字元）
const ORDER_ID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

// 回傳 { ok: true, event } 或 { ok: false, reason }，不丟例外，呼叫端用 ok 判斷即可
export function parseEvent(message) {
  // Kafka 的 key / value 是 Buffer（位元組），要 toString() 轉成字串。
  // ?. 是 optional chaining：key 是 null 時直接得到 undefined，不會因為呼叫 null.toString() 當掉
  const key = message.key?.toString();
  if (!key) return { ok: false, reason: '缺少 Message Key' };

  // ?? 是 nullish coalescing：左邊是 null / undefined 時用右邊（空字串 → JSON.parse 會丟例外 → 進 catch）
  let event;
  try {
    event = JSON.parse(message.value?.toString() ?? '');
  } catch {
    return { ok: false, reason: 'value 不是合法 JSON' };
  }

  // JSON.parse('123') 或 'null' 也是合法 JSON，但不是物件 → 擋掉
  if (typeof event !== 'object' || event === null) return { ok: false, reason: 'value 不是 JSON 物件' };
  if (!ORDER_ID_PATTERN.test(event.order_id ?? '')) return { ok: false, reason: 'order_id 缺少或含非法字元' };
  // key 決定 partition（順序性的依據），value 裡的 order_id 決定寫到哪個 Redis key；
  // 兩者不一致代表這筆可能被送到錯的 partition，順序保證就不成立
  if (event.order_id !== key) return { ok: false, reason: 'Message Key 與 order_id 不一致' };
  if (!Number.isInteger(event.seq) || event.seq <= 0) return { ok: false, reason: 'seq 必須是正整數' };

  return { ok: true, event };
}

// 比較 Redis 裡已存的 seq（lastSeq）與這筆的 seq，判斷這筆是：
//   duplicate：seq 不大於已存的 → 已經處理過（rebalance 後重送），略過
//   in_order ：剛好是下一號 → 正常
//   gap      ：跳號 → 中間有訊息遺失或亂序，仍寫入但記 log
// lastSeq 為 null 代表這筆訂單第一次出現，所以預期 seq 是 1
export function classifySeq(lastSeq, seq) {
  const expected = (lastSeq ?? 0) + 1;
  if (seq < expected) return 'duplicate';
  if (seq === expected) return 'in_order';
  return 'gap';
}
