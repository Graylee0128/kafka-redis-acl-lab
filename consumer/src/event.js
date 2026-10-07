// =============================================================================
// 訊息解析與順序判斷（純函式，不碰 Kafka / Redis，方便測試）
// =============================================================================
//
// 誰呼叫：
//   parseEvent  ← index.js 的 handleMessage，每收到一筆 Kafka 訊息就先驗證
//   classifySeq ← store.js 的 apply，寫入 Redis 前判斷這筆是正常、重複還是跳號
//
// 為什麼要驗證：Kafka 訊息是外部輸入，任何有 Write 權限的人都能送任意內容。
// 不合法的訊息回報原因，由呼叫端記 log 後跳過，不讓一筆壞訊息卡住整個 partition
// （如果丟出例外，kafkajs 會一直重試同一筆，後面的訊息全部處理不到）。
//
// 純函式：輸出只取決於輸入，不讀寫外部狀態，所以 test/event.test.js 可以直接餵各種輸入檢查。
//
// 閱讀提示（本檔用到的 JavaScript 語法）：
//   /正規表示式/        regex，用法同 grep -E；regex.test(字串) 回傳 true / false
//   a?.b               optional chaining：a 是 null / undefined 時直接得到 undefined，不會當掉
//   a ?? b             nullish coalescing：a 是 null / undefined 時用 b（空字串、0 不算，仍用 a）
//   try { } catch { }  嘗試執行，出錯就跳到 catch（類似 shell 的 cmd || 處理錯誤）
//   let x;             宣告「之後可以重新指定」的變數（const 不行）
//   typeof x           取得 x 的型別名稱，例如 'object'、'string'、'number'
// =============================================================================

// 白話：order_id 只允許英數字、底線、減號，長度 1～64。
// order_id 會被拼進 Redis key（myapp:events:<order_id>），只允許安全字元，
// 避免拼出意外的 key（例如含冒號、空白或 * 這類 pattern 字元）
// 語法：/^...$/ 是 regex；^ 開頭、$ 結尾，代表「整個字串」都要符合，不是只有一部分。
const ORDER_ID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

/**
 * 驗證並解析一筆 Kafka 訊息。
 *
 * 呼叫者：index.js 的 handleMessage（每筆訊息第一步）。
 *
 * 依序檢查（任一不過就回傳失敗原因，不再往下）：
 *   1. 有 Message Key
 *   2. value 是合法 JSON
 *   3. JSON 是物件（不是數字、null）
 *   4. order_id 存在且只含安全字元
 *   5. Message Key 與 order_id 相同
 *   6. seq 是正整數
 *
 * 不丟例外：回傳 { ok } 讓呼叫端用 if 判斷，壞訊息記 log 後跳過即可。
 *
 * @param {object} message  kafkajs 給的訊息物件；用到 message.key、message.value（兩者都是 Buffer 或 null）
 * @returns {{ ok: true, event: object } | { ok: false, reason: string }}
 *   成功：event 是解析後的 { order_id, seq, status, amount, ts }
 *   失敗：reason 是給人看的原因，handleMessage 會記成 invalid_message log
 *
 * @example
 *   parseEvent({ key: Buffer.from('o1-0001'), value: Buffer.from('{"order_id":"o1-0001","seq":1}') })
 *   // → { ok: true, event: { order_id: 'o1-0001', seq: 1 } }
 *   parseEvent({ key: null, value: ... })
 *   // → { ok: false, reason: '缺少 Message Key' }
 */
export function parseEvent(message) {
  // 白話【檢查 1】取出 Message Key 並轉成字串；沒有 key 就回傳失敗。
  // Kafka 的 key / value 是 Buffer（位元組），要 toString() 轉成字串。
  // ?. 是 optional chaining：key 是 null 時直接得到 undefined，不會因為呼叫 null.toString() 當掉
  const key = message.key?.toString();
  // 語法：!key 在 key 是 undefined 或空字串時為 true。
  //       return { ok: false, reason: '...' } 回傳一個物件，呼叫端用 parsed.ok、parsed.reason 讀。
  if (!key) return { ok: false, reason: '缺少 Message Key' };

  // 白話【檢查 2】把 value（JSON 字串）解析成物件；不是合法 JSON 就回傳失敗。
  // 語法：let 宣告變數但先不給值，因為要在下面的 try 裡面才指定。
  let event;
  try {
    // 白話：'{"order_id":"o1","seq":1}' → { order_id: 'o1', seq: 1 }
    // ?? 是 nullish coalescing：左邊是 null / undefined 時用右邊（空字串 → JSON.parse 會丟例外 → 進 catch）
    event = JSON.parse(message.value?.toString() ?? '');
  } catch {
    // 語法：catch 接住 try 裡丟出的例外；這裡不需要錯誤內容，所以 catch 後面沒寫 (err)。
    return { ok: false, reason: 'value 不是合法 JSON' };
  }

  // 白話【檢查 3】JSON.parse('123') 或 'null' 也是合法 JSON，但不是物件 → 擋掉
  // 語法：typeof null 在 JS 裡也是 'object'（歷史包袱），所以要另外檢查 event === null。
  if (typeof event !== 'object' || event === null) return { ok: false, reason: 'value 不是 JSON 物件' };
  // 白話【檢查 4】order_id 要存在且符合 ORDER_ID_PATTERN。
  // 語法：event.order_id ?? ''：沒有 order_id 欄位時改用空字串去比對（空字串不符合 {1,64}，所以會失敗）。
  if (!ORDER_ID_PATTERN.test(event.order_id ?? '')) return { ok: false, reason: 'order_id 缺少或含非法字元' };
  // 白話【檢查 5】Message Key 必須等於內容裡的 order_id。
  // key 決定 partition（順序性的依據），value 裡的 order_id 決定寫到哪個 Redis key；
  // 兩者不一致代表這筆可能被送到錯的 partition，順序保證就不成立
  if (event.order_id !== key) return { ok: false, reason: 'Message Key 與 order_id 不一致' };
  // 白話【檢查 6】seq 必須是正整數（1、2、3…），字串 '1' 或 1.5 都不行。
  if (!Number.isInteger(event.seq) || event.seq <= 0) return { ok: false, reason: 'seq 必須是正整數' };

  // 白話：全部通過，回傳成功與解析好的事件。
  // 語法：{ ok: true, event } 是 { ok: true, event: event } 的簡寫（欄位名稱和變數名稱相同時可省略）。
  return { ok: true, event };
}

/**
 * 比較已存的 seq 與這筆的 seq，判斷這筆事件的順序狀態。
 *
 * 呼叫者：store.js 的 apply（寫入 Redis 之前）。
 *
 * 預期值 expected = lastSeq + 1（第一次出現時 lastSeq 是 null，預期 1）：
 *   seq <  expected → 'duplicate'：已經處理過（rebalance 後重送），apply 會略過不寫
 *   seq === expected → 'in_order' ：剛好是下一號，正常
 *   seq >  expected → 'gap'      ：跳號，中間有訊息遺失或亂序；仍寫入，但 handleMessage 記 order_gap
 *
 * @param {number|null} lastSeq  Redis 裡已存的最新 seq；這筆訂單第一次出現時為 null
 * @param {number} seq           這筆事件的 seq（parseEvent 已確認是正整數）
 * @returns {'duplicate'|'in_order'|'gap'}
 *
 * @example
 *   classifySeq(null, 1) // → 'in_order'（第一筆）
 *   classifySeq(2, 3)    // → 'in_order'
 *   classifySeq(3, 2)    // → 'duplicate'
 *   classifySeq(1, 3)    // → 'gap'（少了 seq 2）
 */
export function classifySeq(lastSeq, seq) {
  // 白話：算出「下一筆應該是幾號」。lastSeq 是 null（第一次出現）時當成 0，所以預期 1。
  // 語法：(lastSeq ?? 0) 先算括號內：lastSeq 是 null 就用 0。
  const expected = (lastSeq ?? 0) + 1;
  // 白話：比預期小 → 已經處理過的舊訊息。
  if (seq < expected) return 'duplicate';
  // 白話：剛好等於預期 → 正常的下一筆。
  if (seq === expected) return 'in_order';
  // 白話：剩下的情況就是比預期大 → 跳號。
  return 'gap';
}
