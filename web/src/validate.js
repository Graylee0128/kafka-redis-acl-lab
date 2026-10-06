// =============================================================================
// 驗證權限申請內容（純函式，方便測試）
// =============================================================================
//
// 誰呼叫：server.js 的 handleAccessRequest，JSON 解析完後第一件事就是呼叫 validateRequest。
// 這是使用者輸入進入系統的唯一關口：通過這裡的帳號、密碼才會被拿去建 Kafka / Redis 帳號，
// 所以格式一律用白名單（只允許哪些字元），而不是黑名單（擋哪些字元）。
//
// 錯誤訊息只描述規則，不回顯使用者輸入的密碼。
// =============================================================================

// 可以申請的目標
export const TARGETS = Object.freeze(['kafka', 'redis']);

// 系統內建帳號：若允許申請，任何人都能透過面板重設它們的密碼
const RESERVED = new Set(['admin', 'producer', 'consumer', 'app', 'default', 'root', 'kafka', 'redis']);

// 帳號：小寫英文開頭，之後 2～31 個 a-z、0-9、_、-（總長 3～32）。
//   ^ 與 $ 錨定開頭結尾，確保整個字串都符合，而不是只有一部分符合
const USERNAME_PATTERN = /^[a-z][a-z0-9_-]{2,31}$/;
// 密碼會嵌進 SCRAM 設定字串 SCRAM-SHA-512=[password=...]，逗號、中括號、等號都會破壞語法
const PASSWORD_PATTERN = /^[A-Za-z0-9]{12,64}$/;

// 回傳 { ok: true, value } 或 { ok: false, errors: [...] }。
// 所有錯誤一次收集完再回傳，使用者不用改一個錯、送一次、再看到下一個錯
export function validateRequest(body) {
  // Array 的 typeof 也是 'object'，所以要另外排除
  if (typeof body !== 'object' || body === null || Array.isArray(body)) {
    return { ok: false, errors: ['請求內容必須是 JSON 物件'] };
  }

  const { username, password, targets } = body;
  const errors = [];

  // typeof 檢查：JSON 裡可能傳數字、陣列等非字串，直接丟給 regex.test 會被轉成字串而誤判通過
  if (typeof username !== 'string' || !USERNAME_PATTERN.test(username)) {
    errors.push('帳號需為 3–32 字，小寫英文開頭，只能包含 a-z、0-9、_、-');
  } else if (RESERVED.has(username)) {
    errors.push('此帳號為系統保留帳號，不能申請');
  }

  if (typeof password !== 'string' || !PASSWORD_PATTERN.test(password)) {
    errors.push('密碼需為 12–64 碼英數字');
  }

  // targets 必須是非空陣列，且每個元素都在 TARGETS 裡
  const validTargets = Array.isArray(targets) && targets.length > 0 && targets.every((t) => TARGETS.includes(t));
  if (!validTargets) {
    errors.push('至少選擇一個申請目標（kafka / redis）');
  }

  if (errors.length > 0) return { ok: false, errors };
  // targets 改從 TARGETS 篩出來：去掉重複（["kafka","kafka"]），順序也固定成 kafka → redis
  return {
    ok: true,
    value: { username, password, targets: TARGETS.filter((t) => targets.includes(t)) },
  };
}
