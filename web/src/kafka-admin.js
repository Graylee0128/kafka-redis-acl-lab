// =============================================================================
// Kafka 帳號管理（面板申請的帳號）
// =============================================================================
//
// 誰呼叫：provision.js 透過 { userExists, createUser, removeUser } 三個方法使用。
// 用哪個帳號：Kafka admin（super user，server.js 產生的 admin.properties 與 kafkajs admin client）。
//
// 建一個帳號分兩步：
//   1. SCRAM 憑證（帳號 + 密碼雜湊）→ kafka-configs.sh
//   2. ACL（這個帳號能做什麼）       → kafkajs admin.createAcls
//
// SCRAM 憑證：Node 的 Kafka client（kafkajs、@confluentinc/kafka-javascript）都沒有實作
// AlterUserScramCredentials，所以呼叫官方的 kafka-configs.sh（web 映像以 apache/kafka 為基底）。
// 用 execFile 傳參數陣列，不經 shell，使用者輸入不會被當成指令解析。
// ACL：kafkajs admin 有 createAcls / deleteAcls，直接用。
// =============================================================================
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import kafkajs from 'kafkajs';

// kafkajs 是 CommonJS，ACL 常數不在 Node 偵測得到的具名匯出裡，只能從 default 取
const { AclOperationTypes, AclPermissionTypes, AclResourceTypes, ResourcePatternTypes } = kafkajs;
// execFile 原本是 callback 風格，promisify 包成回傳 Promise 的版本，才能 await
const execFileAsync = promisify(execFile);

// kafka-configs.sh 失敗時會把完整參數（含 --add-config 的密碼）印進錯誤訊息。
// 把 text 裡出現的每個 secret 換成 ***：split(secret).join('***') 等於「全部取代」
export function redact(text, secrets) {
  return secrets.filter(Boolean).reduce((out, secret) => out.split(secret).join('***'), String(text));
}

// 回傳一個執行 kafka-configs.sh 的函式，連線參數（bootstrap、admin 設定檔）固定好，
// 呼叫時只要傳動作相關的參數。secrets：這次參數裡含有的密碼，失敗時要從錯誤訊息遮掉
export function createConfigsRunner({ bin, bootstrap, commandConfig, timeoutMs = 60_000 }) {
  return async function runConfigs(args, secrets = []) {
    try {
      // execFile(程式, 參數陣列, 選項)：直接執行程式，不經過 shell，
      // 所以參數裡的 ; | $() 等字元都只是普通文字。
      // KAFKA_HEAP_OPTS 限制這個 JVM 最多用 128MB，避免每次申請都吃掉預設的大量記憶體
      const { stdout } = await execFileAsync(
        `${bin}/kafka-configs.sh`,
        ['--bootstrap-server', bootstrap, '--command-config', commandConfig, ...args],
        { timeout: timeoutMs, env: { ...process.env, KAFKA_HEAP_OPTS: '-Xmx128m' } },
      );
      return stdout;
    } catch (err) {
      // 只取 stderr 第一行非空白的內容當作原因（完整 stack trace 太長），並遮蔽密碼
      const detail = (err.stderr || err.message || '').split('\n').find((line) => line.trim()) ?? '';
      throw new Error(`kafka-configs ${args[0]} 失敗（exit ${err.code}）：${redact(detail, secrets)}`);
    }
  };
}

// 面板帳號的 ACL，固定三條（建立與刪除都用同一份，確保回滾時刪得乾淨）：
//   topic orders：READ、DESCRIBE（LITERAL = 名稱完全相符）
//   group <帳號>-*：READ（PREFIXED = 以這個字串開頭的 group 都算）
function aclEntries(username, topic) {
  // 三條共用的欄位：principal 是 Kafka 的帳號表示法 User:<名稱>；host '*' = 從任何來源連線都適用
  const base = { principal: `User:${username}`, host: '*', permissionType: AclPermissionTypes.ALLOW };
  return [
    { ...base, resourceType: AclResourceTypes.TOPIC, resourceName: topic, resourcePatternType: ResourcePatternTypes.LITERAL, operation: AclOperationTypes.READ },
    { ...base, resourceType: AclResourceTypes.TOPIC, resourceName: topic, resourcePatternType: ResourcePatternTypes.LITERAL, operation: AclOperationTypes.DESCRIBE },
    // 只能使用以自己帳號開頭的 consumer group，碰不到 order-consumers
    { ...base, resourceType: AclResourceTypes.GROUP, resourceName: `${username}-`, resourcePatternType: ResourcePatternTypes.PREFIXED, operation: AclOperationTypes.READ },
  ];
}

// runConfigs：上面 createConfigsRunner 的結果；admin：已連線的 kafkajs admin client
export function createKafkaProvisioner({ runConfigs, admin, topic }) {
  const userArgs = (username) => ['--entity-type', 'users', '--entity-name', username];

  return {
    // --describe 帳號：有 SCRAM 憑證時輸出會包含 "SCRAM credential configs"
    async userExists(username) {
      const out = await runConfigs(['--describe', ...userArgs(username)]);
      return out.includes('SCRAM credential configs');
    },

    // 先建憑證再建 ACL；第二個參數 [password] 告訴 runConfigs 失敗時要遮掉密碼
    async createUser(username, password) {
      await runConfigs(['--alter', ...userArgs(username), '--add-config', `SCRAM-SHA-512=[password=${password}]`], [password]);
      await admin.createAcls({ acl: aclEntries(username, topic) });
      // 回傳給使用者看的權限摘要（不含密碼）
      return {
        mechanism: 'SCRAM-SHA-512',
        topic,
        operations: ['READ', 'DESCRIBE'],
        groupPrefix: `${username}-`,
      };
    },

    // 回滾用：順序與建立相反，先刪 ACL 再刪憑證
    async removeUser(username) {
      await admin.deleteAcls({ filters: aclEntries(username, topic) });
      await runConfigs(['--alter', ...userArgs(username), '--delete-config', 'SCRAM-SHA-512']);
    },
  };
}
