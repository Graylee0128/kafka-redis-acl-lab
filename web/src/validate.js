// =============================================================================
// 驗證權限申請內容（純函式，方便測試）
// =============================================================================
//
// 誰呼叫：server.js 的 handleAccessRequest，JSON 解析完後第一件事就是呼叫 validateRequest。
// 這是使用者輸入進入系統的唯一關口：通過這裡的帳號、密碼才會被拿去建 Kafka / Redis 帳號，
// 所以格式一律用白名單（只允許哪些字元），而不是黑名單（擋哪些字元）。
//
// 錯誤訊息只描述規則，不回顯使用者輸入的密碼。
//
// 閱讀提示（本檔用到的 JavaScript 語法）：
//   /正規表示式/          regex，用法同 grep -E；regex.test(字串) 回傳 true / false
//   new Set([...])        集合：適合「某個值在不在裡面」的查詢，set.has(值) 回傳 true / false
//   typeof x              取得 x 的型別名稱，例如 'string'、'object'
//   const { a, b } = obj  解構：從物件取出 a、b 欄位，變成同名變數
//   陣列.push(值)         把值加到陣列尾端
//   陣列.every(函式)      每個元素都讓函式回傳 true，結果才是 true
//   陣列.includes(值)     陣列裡有沒有這個值
// =============================================================================

// 白話：可以申請的目標，只有這兩個。
export const TARGETS = Object.freeze(['kafka', 'redis']);

// 白話：系統內建帳號清單：若允許申請，任何人都能透過面板重設它們的密碼。
const RESERVED = new Set(['admin', 'producer', 'consumer', 'app', 'default', 'root', 'kafka', 'redis']);

// 白話：帳號規則：小寫英文開頭，之後 2～31 個 a-z、0-9、_、-（總長 3～32）。
// 語法：^ 與 $ 錨定開頭結尾，確保整個字串都符合，而不是只有一部分符合。
const USERNAME_PATTERN = /^[a-z][a-z0-9_-]{2,31}$/;
// 白話：密碼規則：12～64 碼英數字。
// 密碼會嵌進 SCRAM 設定字串 SCRAM-SHA-512=[password=...]，逗號、中括號、等號都會破壞語法
const PASSWORD_PATTERN = /^[A-Za-z0-9]{12,64}$/;

/**
 * 驗證一筆權限申請。
 *
 * 呼叫者：server.js 的 handleAccessRequest（JSON 解析成功後）。
 *
 * 所有錯誤一次收集完再回傳，使用者不用改一個錯、送一次、再看到下一個錯。
 *
 * @param {*} body  JSON.parse 後的請求內容，預期是 { username, password, targets }；
 *                  但使用者可以送任何 JSON，所以型別不能假設
 * @returns {{ ok: true, value: { username: string, password: string, targets: string[] } }
 *          | { ok: false, errors: string[] }}
 *   成功：value 是整理過的申請內容（targets 去重、固定順序）
 *   失敗：errors 是給使用者看的錯誤訊息陣列，server.js 回 400
 *
 * @example
 *   validateRequest({ username: 'team-a', password: 'ExamplePass2026', targets: ['redis', 'kafka'] })
 *   // → { ok: true, value: { username: 'team-a', password: '...', targets: ['kafka', 'redis'] } }
 *   validateRequest({ username: 'admin', password: 'short', targets: [] })
 *   // → { ok: false, errors: ['此帳號為系統保留帳號，不能申請', '密碼需為 12–64 碼英數字', '至少選擇一個申請目標（kafka / redis）'] }
 */
export function validateRequest(body) {
  // 白話：body 必須是一般物件 { ... }。
  // 語法：Array 的 typeof 也是 'object'，null 的 typeof 也是 'object'，所以兩個都要另外排除。
  if (typeof body !== 'object' || body === null || Array.isArray(body)) {
    return { ok: false, errors: ['請求內容必須是 JSON 物件'] };
  }

  // 白話：取出三個欄位，準備一個空陣列收集錯誤。
  // 語法：const { username, password, targets } = body; 等於分別寫 const username = body.username; 等三行。
  const { username, password, targets } = body;
  const errors = [];

  // 白話：檢查帳號格式；格式對了再檢查是不是保留帳號。
  // typeof 檢查：JSON 裡可能傳數字、陣列等非字串，直接丟給 regex.test 會被轉成字串而誤判通過
  if (typeof username !== 'string' || !USERNAME_PATTERN.test(username)) {
    errors.push('帳號需為 3–32 字，小寫英文開頭，只能包含 a-z、0-9、_、-');
  } else if (RESERVED.has(username)) {
    errors.push('此帳號為系統保留帳號，不能申請');
  }

  // 白話：檢查密碼格式。
  if (typeof password !== 'string' || !PASSWORD_PATTERN.test(password)) {
    errors.push('密碼需為 12–64 碼英數字');
  }

  // 白話：targets 必須是非空陣列，且每個元素都在 TARGETS 裡（只能是 'kafka' 或 'redis'）。
  // 語法：&& 是「且」，由左往右算，前面不成立就不往後算（所以 targets 不是陣列時不會去呼叫 .every 而當掉）。
  //       (t) => TARGETS.includes(t) 對每個元素 t 檢查它在不在 TARGETS 裡。
  const validTargets = Array.isArray(targets) && targets.length > 0 && targets.every((t) => TARGETS.includes(t));
  if (!validTargets) {
    errors.push('至少選擇一個申請目標（kafka / redis）');
  }

  // 白話：有任何錯誤就回傳失敗與全部錯誤訊息。
  if (errors.length > 0) return { ok: false, errors };
  // 白話：全部通過。targets 改從 TARGETS 篩出來：去掉重複（["kafka","kafka"]），順序也固定成 kafka → redis。
  // 語法：TARGETS.filter((t) => targets.includes(t)) 留下「使用者有選」的目標。
  return {
    ok: true,
    value: { username, password, targets: TARGETS.filter((t) => targets.includes(t)) },
  };
}
