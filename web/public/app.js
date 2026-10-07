// =============================================================================
// 權限申請面板的前端（在瀏覽器執行）
// =============================================================================
//
// 載入方式：index.html 以 <script src="/app.js"> 載入（CSP 不允許 inline script，所以獨立成檔）。
//
// 做什麼：攔下表單送出 → 組成 JSON → fetch POST /api/access-requests → 顯示結果。
// 前端的欄位檢查只是提示，真正的驗證在後端（web/src/validate.js）：
// 任何人都可以繞過網頁直接用 curl 送請求。
//
// 注意：這個檔案在「瀏覽器」裡執行，不是 Node.js。
//   所以這裡用的是瀏覽器提供的東西（document、fetch、FormData），沒有 process、import。
//
// 本檔的函式與誰呼叫它們：
//   show          ← submit 處理函式（顯示成功或失敗）
//   describeGrant ← submit 處理函式（把權限摘要轉成一行文字）
//   markFields    ← submit 處理函式（送出前標紅 / 綠框）
//   clearMarks    ← submit 處理函式（成功後清掉紅 / 綠框）
//   submit 處理函式 ← 瀏覽器，使用者按下「送出」時
//
// 閱讀提示（本檔用到的瀏覽器 / JavaScript 語法）：
//   document.getElementById('x')     取得 HTML 裡 id="x" 的元素
//   元素.textContent = '文字'        設定元素的文字內容（當純文字處理，不會被當成 HTML 執行）
//   document.createElement('li')     建立一個新的 HTML 元素
//   元素.append(子元素)              把子元素加到元素裡面
//   元素.addEventListener('事件', 函式)  登記事件發生時要呼叫的函式
//   fetch(網址, 選項)                送出 HTTP 請求，await 等回應
// =============================================================================

// 白話：取得頁面上會用到的元素（id 定義在 index.html）。
//   form   ：申請表單
//   result ：顯示結果的區塊（一開始是隱藏的）
//   button ：送出按鈕
//   fields ：帳號、密碼兩個輸入欄位
// 語法：form.elements.username 取得表單裡 name="username" 的欄位。
const form = document.getElementById('request-form');
const result = document.getElementById('result');
const button = form.querySelector('button');
const fields = [form.elements.username, form.elements.password];

/**
 * 在結果區塊顯示標題與條列內容。
 *
 * 呼叫者：submit 處理函式（成功、失敗、連不上伺服器三種情況）。
 *
 * 所有伺服器回傳內容一律用 textContent 寫入，不用 innerHTML，避免 XSS
 * （伺服器回傳的文字就算含有 <script>，也只會被當成文字顯示）。
 *
 * @param {'success'|'error'} kind  對應 style.css 的 #result.success / #result.error 顏色
 * @param {string} title            粗體標題，例如「帳號 team-a 建立完成」
 * @param {string[]} [lines=[]]     條列內容；空陣列就不顯示清單
 * @returns {void}
 */
function show(kind, title, lines = []) {
  // 白話：設定顏色樣式、顯示區塊、清掉上一次的內容。
  result.className = kind;
  result.hidden = false;
  result.replaceChildren();
  // 白話：加上粗體標題 <strong>標題</strong>。
  const heading = document.createElement('strong');
  heading.textContent = title;
  result.append(heading);
  // 白話：有內容才建立清單 <ul><li>…</li></ul>，每一行一個 <li>。
  if (lines.length > 0) {
    const list = document.createElement('ul');
    for (const line of lines) {
      const item = document.createElement('li');
      item.textContent = line;
      list.append(item);
    }
    result.append(list);
  }
}

/**
 * 把後端回傳的單一目標權限摘要轉成一行文字。
 *
 * 呼叫者：submit 處理函式（申請成功時，對每個目標各呼叫一次）。
 *
 * grant 的內容來自 kafka-admin.js / redis-admin.js 的 createUser 回傳值，
 * 再加上 server.js 的 connectionHints（bootstrap、url）。
 *
 * @param {'kafka'|'redis'} target
 * @param {object} grant  權限摘要
 * @returns {string}
 *
 * @example
 *   describeGrant('redis', { keyPattern: 'myapp:*', commands: ['GET', 'TTL'], url: 'redis://redis:6379' })
 *   // → 'Redis：myapp:*，唯讀指令 GET、TTL，redis://redis:6379'
 */
function describeGrant(target, grant) {
  if (target === 'kafka') {
    return `Kafka：topic ${grant.topic}（${grant.operations.join('、')}），group 前綴 ${grant.groupPrefix}，${grant.mechanism}，${grant.bootstrap}`;
  }
  return `Redis：${grant.keyPattern}，唯讀指令 ${grant.commands.join('、')}，${grant.url}`;
}

/**
 * 依欄位格式標上紅 / 綠框（只做提示，真正的驗證在後端）。
 *
 * 呼叫者：submit 處理函式（送出前）。
 *
 * Pico CSS 依 aria-invalid 顯示紅 / 綠框；
 * checkValidity() 依 index.html 上的 required / pattern 屬性判斷
 *
 * @returns {void}
 */
function markFields() {
  for (const field of fields) {
    // 語法：String(true) → 'true'；屬性值必須是字串。
    field.setAttribute('aria-invalid', String(!field.checkValidity()));
  }
}

/**
 * 清掉紅 / 綠框。
 *
 * 呼叫者：submit 處理函式（申請成功、表單清空後）。
 *
 * @returns {void}
 */
function clearMarks() {
  for (const field of fields) field.removeAttribute('aria-invalid');
}

/**
 * submit 處理函式：使用者按下「送出」時由瀏覽器呼叫。
 *
 * 步驟：
 *   1. 阻止瀏覽器原本的表單送出（會換頁），改用 fetch 送 JSON
 *   2. 標紅 / 綠框，組出 { username, password, targets }
 *   3. 停用按鈕、顯示讀取動畫，送出 POST /api/access-requests
 *   4. 201 顯示成功與權限；其他狀態碼顯示後端給的錯誤；連不上就顯示連線錯誤
 *   5. 不論結果都恢復按鈕
 *
 * @param {Event} event  瀏覽器的 submit 事件物件
 */
form.addEventListener('submit', async (event) => {
  // 白話【1】阻止瀏覽器原本的表單送出（會換頁），改用 fetch 送 JSON
  event.preventDefault();
  // 白話【2】標紅 / 綠框，再讀出表單內容。
  markFields();
  // 語法：new FormData(form) 讀取表單所有欄位；data.get('名稱') 取單一值；
  //       data.getAll('targets') 取得所有勾選的 targets 核取方塊（可能有 0～2 個）。
  const data = new FormData(form);
  const payload = {
    username: data.get('username'),
    password: data.get('password'),
    targets: data.getAll('targets'),
  };

  // 白話【3】送出期間停用按鈕（避免重複送出），aria-busy 讓 Pico 顯示讀取動畫；先隱藏上一次的結果。
  button.disabled = true;
  button.setAttribute('aria-busy', 'true');
  result.hidden = true;
  try {
    // 白話：送出 POST 請求，body 是 JSON 字串；await 等伺服器回應。
    const response = await fetch('/api/access-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    // 白話：把回應內容解析成物件。
    const body = await response.json();
    // 白話【4】201 = 建立成功；其他狀態碼（400 / 409 / 429 / 500）都顯示後端給的錯誤訊息。
    // 語法：Object.entries(body.granted) 把 { kafka: {...}, redis: {...} } 轉成 [['kafka', {...}], ...]，
    //       .map(([t, g]) => describeGrant(t, g)) 每個目標轉成一行文字。
    //       400 回傳 errors 陣列，其他回傳單一 error 字串，?? 依序取第一個有值的
    if (response.status === 201) {
      show('success', `帳號 ${body.username} 建立完成`, Object.entries(body.granted).map(([t, g]) => describeGrant(t, g)));
      // 白話：成功後清空表單與紅 / 綠框。
      form.reset();
      clearMarks();
    } else {
      show('error', '申請失敗', body.errors ?? [body.error ?? `HTTP ${response.status}`]);
    }
  } catch {
    // 白話：網路錯誤或回應不是 JSON
    show('error', '無法連線到伺服器，請稍後再試');
  } finally {
    // 白話【5】不論成功失敗都恢復按鈕
    button.disabled = false;
    button.removeAttribute('aria-busy');
  }
});
