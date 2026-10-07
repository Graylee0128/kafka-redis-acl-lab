// =============================================================================
// 權限申請面板：靜態頁面 + POST /api/access-requests
// =============================================================================
//
// 誰執行：web 容器（web/Dockerfile 的 CMD ["node", "src/server.js"]），compose 起 1 個實體。
// 何時跑：kafka-init 成功結束、redis healthy 之後啟動，常駐；restart: unless-stopped。
// Port：容器內 8080，compose 只 publish 到 127.0.0.1:${WEB_HOST_PORT:-8080}。
//
// 這是整個系統中唯一持有 Kafka / Redis admin 憑證的元件。瀏覽器只能送申請，
// 拿不到 admin 憑證；新帳號一律只拿到固定的唯讀權限。
//
// 一筆申請的處理順序（handleAccessRequest）：
//   限流 → 檢查 Content-Type → 讀 body（大小上限）→ 解析 JSON → validate.js 驗證
//   → 同帳號不可同時處理 → provision.js 建帳號（失敗回滾）→ 記稽核 log → 回應
//
// 路由：
//   GET  /  /app.js  /style.css  /pico.min.css   靜態檔（白名單）
//   GET  /healthz                                 compose healthcheck 用
//   POST /api/access-requests                     申請帳號
//
// 不用 Express 等框架，只用 Node 內建的 http 模組：路由只有幾條，少一個依賴就少一份要追的漏洞。
//
// 本檔的函式與誰呼叫它們：
//   log                  ← 本檔所有地方
//   send                 ← 每個回應（靜態檔、JSON、錯誤）
//   readBody             ← handleAccessRequest
//   handleAccessRequest  ← 路由分派（POST /api/access-requests）
//   路由分派（匿名函式） ← Node.js 的 http server，每個 HTTP 請求一次
//   shutdown             ← SIGTERM / SIGINT
//
// 閱讀提示：這支程式「怎麼跑起來」
//   1. node src/server.js 從第一行往下執行：檢查環境變數、建立 Kafka / Redis 連線物件、組裝 provisioners、
//      建立 HTTP server 物件、登記訊號處理，最後連線並 server.listen(8080) 開始接請求。
//   2. 之後每個 HTTP 請求進來，Node.js 就呼叫一次 http.createServer 裡的路由分派函式。
//
// 閱讀提示（本檔用到的 JavaScript / Node.js 語法）：
//   import x from 'node:http'   Node.js 內建模組（http 伺服器、fs 檔案、os 系統、path 路徑）
//   process.env.X               讀環境變數 X
//   new Set() / new Promise()   集合 / 「之後才會有結果」的物件（見 readBody）
//   req / res                   HTTP 請求物件 / 回應物件，由 Node.js 傳給路由分派函式
//   物件.on('事件', 函式)       登記事件發生時要呼叫的函式（callback）
//   try { } catch { } finally { }  finally 不論成功或失敗最後都會執行
// =============================================================================

// 白話：載入 Node.js 內建模組、兩個 client 套件，以及本專案 web/src 下的 5 個檔案。
import http from 'node:http';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import { createClient } from 'redis';
import { validateRequest } from './validate.js';
import { provisionAccess } from './provision.js';
import { createConfigsRunner, createKafkaProvisioner } from './kafka-admin.js';
import { createRedisProvisioner } from './redis-admin.js';
import { createRateLimiter } from './rate-limit.js';

/**
 * 印出一行 JSON log；稽核紀錄（access_granted 等）也走這裡，絕不放密碼。
 *
 * 呼叫者：本檔所有需要記錄的地方。
 *
 * @param {object} fields  要記錄的欄位，慣例上一定有 msg
 * @returns {void}
 */
const log = (fields) => console.log(JSON.stringify({ ts: new Date().toISOString(), ...fields }));

// 白話：必填環境變數：缺了就無法連 Kafka / Redis，啟動時直接結束（只印欄位名稱）。
// 語法：REQUIRED.filter((name) => !process.env[name]) 留下沒有值的變數名稱。
const REQUIRED = ['KAFKA_BOOTSTRAP', 'KAFKA_ADMIN_PASSWORD', 'REDIS_URL', 'REDIS_ADMIN_PASSWORD'];
const missing = REQUIRED.filter((name) => !process.env[name]);
if (missing.length > 0) {
  console.error(`設定錯誤：缺少必要環境變數：${missing.join(', ')}`);
  process.exit(1);
}

// 白話：整理設定（沒設定的用預設值）。
//   密碼不放進 config 物件，只在建立連線的地方直接讀 process.env，減少被意外印出的機會
// 語法：Number('8080') 字串轉數字；a || b 是「a 沒值就用 b」。
const config = Object.freeze({
  port: Number(process.env.PORT || 8080),
  kafkaBootstrap: process.env.KAFKA_BOOTSTRAP,
  // apache/kafka 映像裡命令列工具的位置
  kafkaBin: process.env.KAFKA_BIN || '/opt/kafka/bin',
  // 申請的帳號只會拿到這個 topic 的讀取權限
  topic: process.env.KAFKA_TOPIC || 'orders',
  redisUrl: process.env.REDIS_URL,
  // 申請的帳號只能讀這個前綴的 key
  keyPrefix: process.env.REDIS_KEY_PREFIX || 'myapp',
});

// 白話：寫一份 admin 的 Kafka client 設定檔，給 kafka-configs.sh 用（--command-config）。
//   只存在容器的暫存目錄，權限 600（只有擁有者能讀寫）。
//   內容與 infra/kafka/start.sh 產生的 admin.properties 相同（不同容器的檔案不共用，所以各寫一份）
// 語法：path.join(os.tmpdir(), 'admin.properties') 組出 /tmp/admin.properties；
//       [...].join('\n') 把每行字串用換行接起來；
//       fs.writeFileSync(路徑, 內容, { mode: 0o600 }) 同步寫檔（寫完才往下），0o600 是八進位的權限值。
const adminPropsPath = path.join(os.tmpdir(), 'admin.properties');
fs.writeFileSync(adminPropsPath, [
  'security.protocol=SASL_PLAINTEXT',
  'sasl.mechanism=SCRAM-SHA-512',
  `sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="${process.env.KAFKA_ADMIN_PASSWORD}";`,
  '',
].join('\n'), { mode: 0o600 });

// 白話：建立 kafkajs 的 admin client（以 admin 帳號登入），用來建立 / 刪除 ACL。
//   SCRAM 憑證則交給 kafka-configs.sh（見 kafka-admin.js）。
// 語法：new Kafka({...}).admin() 先建 Kafka client，再從它取得 admin client，一行寫完。
const kafkaAdmin = new Kafka({
  clientId: 'access-panel',
  brokers: [config.kafkaBootstrap],
  ssl: false,
  sasl: { mechanism: 'scram-sha-512', username: 'admin', password: process.env.KAFKA_ADMIN_PASSWORD },
  logLevel: logLevel.WARN,
}).admin();

// 白話：建立 Redis admin client，用來執行 ACL SETUSER / ACL SAVE。
//   監聽 error 避免連線中斷時變成未處理的例外讓 process 當掉
const redisAdmin = createClient({ url: config.redisUrl, username: 'admin', password: process.env.REDIS_ADMIN_PASSWORD });
redisAdmin.on('error', (err) => log({ msg: 'redis_error', error: err.message }));

// 白話：把兩個建帳號的實作組裝起來，交給 provision.js 使用。key 對應申請的 targets（'kafka' / 'redis'）。
//   這裡就是 web-src 關係圖裡「建立時注入」那兩條虛線。
const provisioners = {
  kafka: createKafkaProvisioner({
    runConfigs: createConfigsRunner({ bin: config.kafkaBin, bootstrap: config.kafkaBootstrap, commandConfig: adminPropsPath }),
    admin: kafkaAdmin,
    topic: config.topic,
  }),
  redis: createRedisProvisioner(redisAdmin, { prefix: config.keyPrefix }),
};

// 白話：申請成功時附給使用者的連線資訊（只供參考），不含任何憑證。
const connectionHints = {
  kafka: { bootstrap: 'kafka:9092（compose 網路內）/ localhost:9094（host）', securityProtocol: 'SASL_PLAINTEXT' },
  redis: { url: 'redis://redis:6379（compose 網路內）' },
};

// 白話：建立限流器：每個 IP 每 60 秒最多 10 次申請。allow(ip) 回傳 true / false。
const allow = createRateLimiter({ limit: 10, windowMs: 60_000 });
// 白話：正在處理中的帳號名稱：同一個帳號同時送兩次，第二次直接回 409，避免兩筆同時建立互相干擾
// 語法：new Set() 建立空集合；set.has / set.add / set.delete 查詢、加入、移除。
const inFlight = new Set();
// 白話：申請內容只有帳號、密碼、目標，4KB 綽綽有餘
const MAX_BODY_BYTES = 4096;

// 白話：算出 web/ 與 web/public/ 的絕對路徑，給靜態檔用。
//   ES module 沒有 __dirname，用 import.meta.url（本檔的 file:// URL）推出目錄，再往上一層是 web/
// 語法：fileURLToPath 把 file:///app/src/server.js 轉成 /app/src/server.js；
//       path.dirname 取目錄 /app/src；path.join(..., '..') 往上一層 → /app。
const appRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const publicDir = path.join(appRoot, 'public');
// 白話：允許下載的靜態檔清單：網址路徑 → [檔案位置, Content-Type]。
//   白名單路由，不依請求路徑組檔名，避免 path traversal（例如請求 /../../etc/passwd）。
//   Pico CSS 從 npm 套件直接提供，不走 CDN：CSP 維持 default-src 'self'，離線也能用
const STATIC = {
  '/': [path.join(publicDir, 'index.html'), 'text/html; charset=utf-8'],
  '/app.js': [path.join(publicDir, 'app.js'), 'text/javascript; charset=utf-8'],
  '/style.css': [path.join(publicDir, 'style.css'), 'text/css; charset=utf-8'],
  '/pico.min.css': [path.join(appRoot, 'node_modules', '@picocss', 'pico', 'css', 'pico.min.css'), 'text/css; charset=utf-8'],
};

// 白話：每個回應都帶的安全標頭：
//   Content-Security-Policy  只能載入同源的資源（擋外部 script）；不能被嵌進 iframe（防點擊劫持）；
//                            form 只能送回同源
//   X-Content-Type-Options   瀏覽器不猜測檔案類型，一律照 Content-Type 處理
//   Referrer-Policy          連到其他網站時不帶本站網址
const SECURITY_HEADERS = {
  'Content-Security-Policy': "default-src 'self'; frame-ancestors 'none'; form-action 'self'",
  'X-Content-Type-Options': 'nosniff',
  'Referrer-Policy': 'no-referrer',
};

/**
 * 送出 HTTP 回應（統一加上安全標頭）。
 *
 * 呼叫者：路由分派與 handleAccessRequest 的每個回應。
 *
 * @param {object} res          Node.js 的回應物件
 * @param {number} status       HTTP 狀態碼，例如 200、201、400、409
 * @param {*} body              回應內容：字串或 Buffer（靜態檔）原樣送出，物件轉成 JSON
 * @param {string} [contentType='application/json; charset=utf-8']
 * @param {object} [extraHeaders={}]  額外標頭，例如 413 時加 { Connection: 'close' }
 * @returns {void}
 *
 * 語法：res.writeHead(狀態碼, 標頭物件) 寫入狀態碼與標頭；res.end(內容) 送出內容並結束這個回應。
 *       { ...SECURITY_HEADERS, 'Content-Type': ..., ...extraHeaders } 合併三組標頭，後面的覆蓋前面的。
 *       條件 ? 甲 : 乙：body 是字串或 Buffer 就原樣送，否則 JSON.stringify。
 */
function send(res, status, body, contentType = 'application/json; charset=utf-8', extraHeaders = {}) {
  res.writeHead(status, { ...SECURITY_HEADERS, 'Content-Type': contentType, ...extraHeaders });
  res.end(typeof body === 'string' || Buffer.isBuffer(body) ? body : JSON.stringify(body));
}

// 白話：body 的硬上限 1MB。超過 MAX_BODY_BYTES（4KB）後繼續讀但丟棄（立刻斷線的話 client 收不到 413），
//   超過 HARD_LIMIT 才強制斷線，避免有人送超大內容拖住伺服器。
const HARD_LIMIT_BYTES = 1024 * 1024;

/**
 * 讀取 HTTP 請求的 body，回傳完整字串。
 *
 * 呼叫者：handleAccessRequest。
 *
 * body 不是一次到齊，而是分成多個 chunk（片段）陸續送來：
 *   'data' 事件：每收到一個片段觸發一次
 *   'end'  事件：全部收完時觸發
 *   'error'事件：連線出錯時觸發
 *
 * 大小處理：
 *   ≤ 4KB      → 正常收集
 *   4KB ～ 1MB → 繼續讀但丟棄內容，讀完回 413（讓 client 收得到回應）
 *   > 1MB      → req.destroy() 直接斷線
 *
 * 語法：new Promise((resolve, reject) => { ... }) 把「事件式」的讀取包成可以 await 的 Promise：
 *         成功時呼叫 resolve(結果)，await 就拿到結果；
 *         失敗時呼叫 reject(錯誤)，await 那一行就會丟出例外。
 *       Object.assign(new Error('...'), { status: 413 }) 建立錯誤物件並加上 status 欄位。
 *       Buffer.concat(chunks).toString('utf8') 把所有片段接起來轉成文字。
 *
 * @param {object} req  Node.js 的請求物件
 * @returns {Promise<string>}  完整的 body 文字
 * @throws {Error}  超過 4KB（err.status = 413）或連線錯誤
 */
function readBody(req) {
  return new Promise((resolve, reject) => {
    // 白話：收集片段的陣列與目前累計的大小（bytes）。
    const chunks = [];
    let size = 0;
    // 白話：每收到一個片段，就累計大小並決定收下、丟棄或斷線。
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > HARD_LIMIT_BYTES) {
        req.destroy();
      } else if (size > MAX_BODY_BYTES) {
        // 語法：把陣列長度設成 0 等於清空陣列（丟掉已收的內容）。
        chunks.length = 0;
      } else {
        chunks.push(chunk);
      }
    });
    // 白話：全部收完：超過 4KB 就回報 413 錯誤，否則把片段接成字串回傳。
    req.on('end', () => {
      if (size > MAX_BODY_BYTES) reject(Object.assign(new Error('body too large'), { status: 413 }));
      else resolve(Buffer.concat(chunks).toString('utf8'));
    });
    // 白話：連線錯誤直接當成失敗。
    req.on('error', reject);
  });
}

/**
 * 處理 POST /api/access-requests：一筆權限申請從頭到尾的流程。
 *
 * 呼叫者：下方的路由分派函式。
 * 會呼叫：allow（rate-limit.js）→ readBody → validateRequest（validate.js）
 *         → provisionAccess（provision.js，內部再呼叫 kafka-admin.js / redis-admin.js）→ send、log。
 *
 * 回應碼：
 *   201 建立成功    400 格式錯誤 / 驗證失敗    409 帳號已存在或處理中
 *   413 內容過大    415 不是 JSON              429 太頻繁    500 建立失敗（已回滾）
 *
 * @param {object} req  請求物件
 * @param {object} res  回應物件
 * @returns {Promise<void>}
 */
async function handleAccessRequest(req, res) {
  // 白話【1. 限流】取得來源 IP，超過頻率就回 429。
  //   經過 Docker port mapping 時，這裡拿到的是 Docker bridge 的 gateway 位址，不一定是真正的使用者 IP
  // 語法：return send(...) 送出回應並結束這個函式。
  const ip = req.socket.remoteAddress;
  if (!allow(ip)) return send(res, 429, { error: '申請太頻繁，請稍後再試' });
  // 白話【2. 只收 JSON】跨站的 HTML form 送不出 application/json，瀏覽器會先發 preflight 而被擋下
  // 語法：(req.headers['content-type'] || '') 沒有這個標頭時改用空字串，避免對 undefined 呼叫 startsWith 而當掉。
  if (!(req.headers['content-type'] || '').startsWith('application/json')) {
    return send(res, 415, { error: 'Content-Type 必須是 application/json' });
  }

  // 白話【3. 讀 body 並解析 JSON】兩種錯誤分開回應：太大回 413（並要求關閉連線），格式錯回 400。
  let body;
  try {
    body = JSON.parse(await readBody(req));
  } catch (err) {
    if (err.status === 413) return send(res, 413, { error: '請求內容過大' }, undefined, { Connection: 'close' });
    return send(res, 400, { error: '請求內容不是合法 JSON' });
  }

  // 白話【4. 驗證格式】不合格回 400，並附上所有錯誤訊息。
  const validation = validateRequest(body);
  if (!validation.ok) return send(res, 400, { errors: validation.errors });

  // 白話【5. 同帳號不可同時處理】已經有一筆同名申請在處理中就回 409；否則登記為處理中。
  const { username, targets } = validation.value;
  if (inFlight.has(username)) return send(res, 409, { error: '此帳號的申請正在處理中' });
  inFlight.add(username);
  // 白話：finally 確保不論成功、衝突或失敗，最後都會把帳號從 inFlight 移除
  try {
    // 白話【6. 建帳號】呼叫 provision.js；結果是 created 或 conflict，失敗會丟例外進 catch。
    const result = await provisionAccess(validation.value, provisioners);
    if (result.status === 'conflict') {
      log({ msg: 'access_conflict', username, target: result.target, ip });
      return send(res, 409, { error: `帳號已存在於 ${result.target}，請換一個帳號` });
    }
    // 白話【7. 稽核紀錄】誰（IP）申請了哪個帳號、哪些目標；不記密碼
    log({ msg: 'access_granted', username, targets, ip });
    // 白話：每個目標的權限摘要再合併連線資訊：{ kafka: {...grant, ...hint}, redis: {...} }
    // 語法：Object.entries(物件) 把物件轉成 [[鍵, 值], ...] 陣列；
    //       .map(([target, grant]) => [target, 新值]) 逐一轉換；
    //       Object.fromEntries(...) 再把 [[鍵, 值], ...] 轉回物件。
    const granted = Object.fromEntries(
      Object.entries(result.granted).map(([target, grant]) => [target, { ...grant, ...connectionHints[target] }]),
    );
    // 白話【8. 回應】201 Created，附上帳號與授予的權限。
    return send(res, 201, { username, granted });
  } catch (err) {
    // 白話：詳細原因只記在 server 端 log（已遮蔽密碼），回給使用者的訊息保持籠統
    log({ msg: 'access_failed', username, targets, ip, error: err.message, rollbackFailures: err.rollbackFailures });
    return send(res, 500, { error: '建立帳號失敗，已回滾，請稍後再試' });
  } finally {
    inFlight.delete(username);
  }
}

/**
 * 路由分派（匿名函式）：每個 HTTP 請求由 Node.js 呼叫一次。
 *
 * 依「方法 + 路徑」決定怎麼處理：
 *   GET  白名單內的靜態檔 → 讀檔回傳
 *   GET  /healthz        → 回 { status: 'ok' }（compose healthcheck 用）
 *   POST /api/access-requests → handleAccessRequest
 *   其他                 → 404
 *
 * 最外層 try/catch：任何未預期的錯誤都回 500，不讓 process 當掉。
 *
 * 語法：http.createServer(函式) 建立伺服器物件，並登記「每個請求要呼叫這個函式」；
 *       這行執行時還沒開始接請求，要等最後一行 server.listen 才開始。
 */
const server = http.createServer(async (req, res) => {
  try {
    // 白話：從網址取出路徑部分（去掉 ?後面的查詢參數）。
    //   req.url 只有路徑（例如 /app.js?v=1），補一個假的 origin 才能用 URL 解析出 pathname
    // 語法：const { pathname } = new URL(...) 解構取出 pathname 欄位。
    const { pathname } = new URL(req.url, 'http://localhost');
    // 白話：GET 且路徑在 STATIC 白名單裡 → 讀檔並回傳。
    // 語法：const [file, type] = STATIC[pathname] 陣列解構，取出檔案位置與 Content-Type；
    //       fs.promises.readFile 非同步讀檔，await 等讀完。
    if (req.method === 'GET' && STATIC[pathname]) {
      const [file, type] = STATIC[pathname];
      return send(res, 200, await fs.promises.readFile(file), type);
    }
    if (req.method === 'GET' && pathname === '/healthz') return send(res, 200, { status: 'ok' });
    if (req.method === 'POST' && pathname === '/api/access-requests') return await handleAccessRequest(req, res);
    return send(res, 404, { error: 'Not Found' });
  } catch (err) {
    log({ msg: 'request_error', error: err.message });
    // 白話：回應標頭已送出的話就不能再改狀態碼，只能放棄
    if (!res.headersSent) send(res, 500, { error: '伺服器錯誤，請稍後再試' });
  }
});

/**
 * 優雅關閉：停止接新連線 → 斷開 Kafka 與 Redis → 結束 process。
 *
 * 呼叫者：SIGTERM / SIGINT（Node.js 收到訊號時）。
 *
 * 兩個連線各自 try/catch，一個失敗不影響另一個。
 * 語法：for (const close of [函式1, 函式2]) 逐一取出陣列裡的關閉函式並呼叫。
 *
 * @returns {Promise<void>}  實際上不會回來，最後會 process.exit
 */
async function shutdown() {
  server.close();
  for (const close of [() => kafkaAdmin.disconnect(), () => redisAdmin.quit()]) {
    try { await close(); } catch (err) { log({ msg: 'disconnect_failed', error: err.message }); }
  }
  process.exit(0);
}
// 白話：登記訊號處理：docker compose stop 送 SIGTERM；Ctrl+C 是 SIGINT。
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// 白話：先確認 Kafka 與 Redis 都連得上，才開始接受 HTTP 請求；連不上就 exit 1 交給 compose 重啟。
// 語法：這個檔案是 ES module，可以在函式外面直接 await。
try {
  await redisAdmin.connect();
  await kafkaAdmin.connect();
} catch (err) {
  log({ msg: 'startup_failed', error: err.message });
  process.exit(1);
}
// 白話：開始在 8080 port 接請求；開好後印 listening。從這裡開始，每個請求都會進到上面的路由分派函式。
server.listen(config.port, () => log({ msg: 'listening', port: config.port }));
