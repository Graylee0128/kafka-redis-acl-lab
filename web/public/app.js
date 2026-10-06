// =============================================================================
// 權限申請面板的前端（在瀏覽器執行）
// =============================================================================
//
// 載入方式：index.html 以 <script src="/app.js"> 載入（CSP 不允許 inline script，所以獨立成檔）。
//
// 做什麼：攔下表單送出 → 組成 JSON → fetch POST /api/access-requests → 顯示結果。
// 前端的欄位檢查只是提示，真正的驗證在後端（web/src/validate.js）：
// 任何人都可以繞過網頁直接用 curl 送請求。
// =============================================================================

// 頁面上會用到的元素（id 定義在 index.html）
const form = document.getElementById('request-form');
const result = document.getElementById('result');
const button = form.querySelector('button');
const fields = [form.elements.username, form.elements.password];

// 在結果區塊顯示標題與條列內容。kind 是 'success' 或 'error'，對應 style.css 的顏色。
// 所有伺服器回傳內容一律用 textContent 寫入，不用 innerHTML，避免 XSS
function show(kind, title, lines = []) {
  result.className = kind;
  result.hidden = false;
  // 清掉上一次的內容
  result.replaceChildren();
  const heading = document.createElement('strong');
  heading.textContent = title;
  result.append(heading);
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

// 把後端回傳的權限摘要（kafka-admin.js / redis-admin.js 的 createUser 回傳值 + 連線資訊）轉成一行文字
function describeGrant(target, grant) {
  if (target === 'kafka') {
    return `Kafka：topic ${grant.topic}（${grant.operations.join('、')}），group 前綴 ${grant.groupPrefix}，${grant.mechanism}，${grant.bootstrap}`;
  }
  return `Redis：${grant.keyPattern}，唯讀指令 ${grant.commands.join('、')}，${grant.url}`;
}

// Pico 依 aria-invalid 顯示紅 / 綠框；只做提示，真正的驗證在後端。
// checkValidity() 依 index.html 上的 required / pattern 屬性判斷
function markFields() {
  for (const field of fields) {
    field.setAttribute('aria-invalid', String(!field.checkValidity()));
  }
}

function clearMarks() {
  for (const field of fields) field.removeAttribute('aria-invalid');
}

form.addEventListener('submit', async (event) => {
  // 阻止瀏覽器原本的表單送出（會換頁），改用 fetch 送 JSON
  event.preventDefault();
  markFields();
  // FormData 讀取表單欄位；getAll 取得所有勾選的 targets 核取方塊
  const data = new FormData(form);
  const payload = {
    username: data.get('username'),
    password: data.get('password'),
    targets: data.getAll('targets'),
  };

  // 送出期間停用按鈕（避免重複送出），aria-busy 讓 Pico 顯示讀取動畫
  button.disabled = true;
  button.setAttribute('aria-busy', 'true');
  result.hidden = true;
  try {
    const response = await fetch('/api/access-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const body = await response.json();
    // 201 = 建立成功；其他狀態碼（400 / 409 / 429 / 500）都顯示後端給的錯誤訊息。
    // 400 回傳 errors 陣列，其他回傳單一 error 字串，?? 依序取第一個有值的
    if (response.status === 201) {
      show('success', `帳號 ${body.username} 建立完成`, Object.entries(body.granted).map(([t, g]) => describeGrant(t, g)));
      form.reset();
      clearMarks();
    } else {
      show('error', '申請失敗', body.errors ?? [body.error ?? `HTTP ${response.status}`]);
    }
  } catch {
    // 網路錯誤或回應不是 JSON
    show('error', '無法連線到伺服器，請稍後再試');
  } finally {
    // 不論成功失敗都恢復按鈕
    button.disabled = false;
    button.removeAttribute('aria-busy');
  }
});
