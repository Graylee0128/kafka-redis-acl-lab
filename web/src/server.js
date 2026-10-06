// =============================================================================
// 權限申請面板：靜態頁面 + POST /api/access-requests
// =============================================================================
//
// 誰執行：web 容器（web/Dockerfile 的 CMD），compose 起 1 個實體。
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
// =============================================================================
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

// 一行一個 JSON 的 log；稽核紀錄（access_granted 等）也走這裡，絕不放密碼
const log = (fields) => console.log(JSON.stringify({ ts: new Date().toISOString(), ...fields }));

// 必填環境變數：缺了就無法連 Kafka / Redis，啟動時直接結束（只印欄位名稱）
const REQUIRED = ['KAFKA_BOOTSTRAP', 'KAFKA_ADMIN_PASSWORD', 'REDIS_URL', 'REDIS_ADMIN_PASSWORD'];
const missing = REQUIRED.filter((name) => !process.env[name]);
if (missing.length > 0) {
  console.error(`設定錯誤：缺少必要環境變數：${missing.join(', ')}`);
  process.exit(1);
}

// 密碼不放進 config 物件，只在建立連線的地方直接讀 process.env，減少被意外印出的機會
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

// kafka-configs.sh 需要 admin 的 client 設定檔：只存在容器的暫存目錄，權限 600。
// 內容與 infra/kafka/start.sh 產生的 admin.properties 相同（不同容器的檔案不共用，所以各寫一份）
const adminPropsPath = path.join(os.tmpdir(), 'admin.properties');
fs.writeFileSync(adminPropsPath, [
  'security.protocol=SASL_PLAINTEXT',
  'sasl.mechanism=SCRAM-SHA-512',
  `sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="${process.env.KAFKA_ADMIN_PASSWORD}";`,
  '',
].join('\n'), { mode: 0o600 });

// kafkajs 的 admin client：用來建立 / 刪除 ACL（SCRAM 憑證則交給 kafka-configs.sh）
const kafkaAdmin = new Kafka({
  clientId: 'access-panel',
  brokers: [config.kafkaBootstrap],
  ssl: false,
  sasl: { mechanism: 'scram-sha-512', username: 'admin', password: process.env.KAFKA_ADMIN_PASSWORD },
  logLevel: logLevel.WARN,
}).admin();

// Redis admin client：用來執行 ACL SETUSER / ACL SAVE。
// 監聽 error 避免連線中斷時變成未處理的例外讓 process 當掉
const redisAdmin = createClient({ url: config.redisUrl, username: 'admin', password: process.env.REDIS_ADMIN_PASSWORD });
redisAdmin.on('error', (err) => log({ msg: 'redis_error', error: err.message }));

// 把兩個建帳號的實作組裝起來，交給 provision.js 使用。key 對應申請的 targets
const provisioners = {
  kafka: createKafkaProvisioner({
    runConfigs: createConfigsRunner({ bin: config.kafkaBin, bootstrap: config.kafkaBootstrap, commandConfig: adminPropsPath }),
    admin: kafkaAdmin,
    topic: config.topic,
  }),
  redis: createRedisProvisioner(redisAdmin, { prefix: config.keyPrefix }),
};

// 連線資訊只回傳給申請者參考，不含任何憑證
const connectionHints = {
  kafka: { bootstrap: 'kafka:9092（compose 網路內）/ localhost:9094（host）', securityProtocol: 'SASL_PLAINTEXT' },
  redis: { url: 'redis://redis:6379（compose 網路內）' },
};

// 每個 IP 每 60 秒最多 10 次申請
const allow = createRateLimiter({ limit: 10, windowMs: 60_000 });
// 正在處理中的帳號名稱：同一個帳號同時送兩次，第二次直接回 409，避免兩筆同時建立互相干擾
const inFlight = new Set();
// 申請內容只有帳號、密碼、目標，4KB 綽綽有餘
const MAX_BODY_BYTES = 4096;

// ES module 沒有 __dirname，用 import.meta.url（本檔的 file:// URL）推出目錄，再往上一層是 web/
const appRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const publicDir = path.join(appRoot, 'public');
// 白名單路由，不依請求路徑組檔名，避免 path traversal。
// Pico CSS 從 npm 套件直接提供，不走 CDN：CSP 維持 default-src 'self'，離線也能用
const STATIC = {
  '/': [path.join(publicDir, 'index.html'), 'text/html; charset=utf-8'],
  '/app.js': [path.join(publicDir, 'app.js'), 'text/javascript; charset=utf-8'],
  '/style.css': [path.join(publicDir, 'style.css'), 'text/css; charset=utf-8'],
  '/pico.min.css': [path.join(appRoot, 'node_modules', '@picocss', 'pico', 'css', 'pico.min.css'), 'text/css; charset=utf-8'],
};

// 每個回應都帶的安全標頭：
//   Content-Security-Policy  只能載入同源的資源（擋外部 script）；不能被嵌進 iframe（防點擊劫持）；
//                            form 只能送回同源
//   X-Content-Type-Options   瀏覽器不猜測檔案類型，一律照 Content-Type 處理
//   Referrer-Policy          連到其他網站時不帶本站網址
const SECURITY_HEADERS = {
  'Content-Security-Policy': "default-src 'self'; frame-ancestors 'none'; form-action 'self'",
  'X-Content-Type-Options': 'nosniff',
  'Referrer-Policy': 'no-referrer',
};

// 統一的回應函式：字串或 Buffer（靜態檔）原樣送出，其他（物件）轉成 JSON
function send(res, status, body, contentType = 'application/json; charset=utf-8', extraHeaders = {}) {
  res.writeHead(status, { ...SECURITY_HEADERS, 'Content-Type': contentType, ...extraHeaders });
  res.end(typeof body === 'string' || Buffer.isBuffer(body) ? body : JSON.stringify(body));
}

// 讀取 request body，回傳 Promise<字串>。
// body 是分成多個 chunk 陸續送來的（data 事件），全部收完（end 事件）才完整。
// 超過上限後繼續讀但丟棄（立刻斷線的話 client 收不到 413），超過 HARD_LIMIT 才強制斷線
const HARD_LIMIT_BYTES = 1024 * 1024;
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > HARD_LIMIT_BYTES) {
        req.destroy();
      } else if (size > MAX_BODY_BYTES) {
        chunks.length = 0;
      } else {
        chunks.push(chunk);
      }
    });
    req.on('end', () => {
      if (size > MAX_BODY_BYTES) reject(Object.assign(new Error('body too large'), { status: 413 }));
      else resolve(Buffer.concat(chunks).toString('utf8'));
    });
    req.on('error', reject);
  });
}

// POST /api/access-requests 的處理流程（順序見檔案開頭）
async function handleAccessRequest(req, res) {
  // 經過 Docker port mapping 時，這裡拿到的是 Docker bridge 的 gateway 位址，不一定是真正的使用者 IP
  const ip = req.socket.remoteAddress;
  if (!allow(ip)) return send(res, 429, { error: '申請太頻繁，請稍後再試' });
  // 只收 JSON：跨站的 HTML form 送不出 application/json，瀏覽器會先發 preflight 而被擋下
  if (!(req.headers['content-type'] || '').startsWith('application/json')) {
    return send(res, 415, { error: 'Content-Type 必須是 application/json' });
  }

  // 讀 body 與解析 JSON 的錯誤分開回應：太大回 413（並要求關閉連線），格式錯回 400
  let body;
  try {
    body = JSON.parse(await readBody(req));
  } catch (err) {
    if (err.status === 413) return send(res, 413, { error: '請求內容過大' }, undefined, { Connection: 'close' });
    return send(res, 400, { error: '請求內容不是合法 JSON' });
  }

  const validation = validateRequest(body);
  if (!validation.ok) return send(res, 400, { errors: validation.errors });

  const { username, targets } = validation.value;
  if (inFlight.has(username)) return send(res, 409, { error: '此帳號的申請正在處理中' });
  inFlight.add(username);
  // finally 確保不論成功、衝突或失敗，最後都會把帳號從 inFlight 移除
  try {
    const result = await provisionAccess(validation.value, provisioners);
    if (result.status === 'conflict') {
      log({ msg: 'access_conflict', username, target: result.target, ip });
      return send(res, 409, { error: `帳號已存在於 ${result.target}，請換一個帳號` });
    }
    // 稽核紀錄：誰（IP）申請了哪個帳號、哪些目標；不記密碼
    log({ msg: 'access_granted', username, targets, ip });
    // 每個目標的權限摘要再合併連線資訊：{ kafka: {...grant, ...hint}, redis: {...} }
    const granted = Object.fromEntries(
      Object.entries(result.granted).map(([target, grant]) => [target, { ...grant, ...connectionHints[target] }]),
    );
    return send(res, 201, { username, granted });
  } catch (err) {
    // 詳細原因只記在 server 端 log（已遮蔽密碼），回給使用者的訊息保持籠統
    log({ msg: 'access_failed', username, targets, ip, error: err.message, rollbackFailures: err.rollbackFailures });
    return send(res, 500, { error: '建立帳號失敗，已回滾，請稍後再試' });
  } finally {
    inFlight.delete(username);
  }
}

// 路由分派。最外層 try/catch：任何未預期的錯誤都回 500，不讓 process 當掉
const server = http.createServer(async (req, res) => {
  try {
    // req.url 只有路徑（例如 /app.js?v=1），補一個假的 origin 才能用 URL 解析出 pathname（去掉 query）
    const { pathname } = new URL(req.url, 'http://localhost');
    if (req.method === 'GET' && STATIC[pathname]) {
      const [file, type] = STATIC[pathname];
      return send(res, 200, await fs.promises.readFile(file), type);
    }
    if (req.method === 'GET' && pathname === '/healthz') return send(res, 200, { status: 'ok' });
    if (req.method === 'POST' && pathname === '/api/access-requests') return await handleAccessRequest(req, res);
    return send(res, 404, { error: 'Not Found' });
  } catch (err) {
    log({ msg: 'request_error', error: err.message });
    // 回應標頭已送出的話就不能再改狀態碼，只能放棄
    if (!res.headersSent) send(res, 500, { error: '伺服器錯誤，請稍後再試' });
  }
});

// 優雅關閉：停止接新連線 → 斷開 Kafka 與 Redis（各自 try/catch，一個失敗不影響另一個）
async function shutdown() {
  server.close();
  for (const close of [() => kafkaAdmin.disconnect(), () => redisAdmin.quit()]) {
    try { await close(); } catch (err) { log({ msg: 'disconnect_failed', error: err.message }); }
  }
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// 先確認 Kafka 與 Redis 都連得上，才開始接受 HTTP 請求；連不上就 exit 1 交給 compose 重啟
try {
  await redisAdmin.connect();
  await kafkaAdmin.connect();
} catch (err) {
  log({ msg: 'startup_failed', error: err.message });
  process.exit(1);
}
server.listen(config.port, () => log({ msg: 'listening', port: config.port }));
