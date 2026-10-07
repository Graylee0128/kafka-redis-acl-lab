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
//
// 本檔的函式與誰呼叫它們：
//   redact                 ← runConfigs 失敗時遮蔽密碼（也 export 給測試用）
//   createConfigsRunner    ← server.js 啟動時呼叫一次，得到 runConfigs
//     └ runConfigs（子函式）← 下面三個方法
//   aclEntries             ← createUser、removeUser
//   createKafkaProvisioner ← server.js 啟動時呼叫一次，結果放進 provisioners.kafka
//     ├ userExists（子函式）← provision.js 階段 1
//     ├ createUser（子函式）← provision.js 階段 2
//     └ removeUser（子函式）← provision.js 階段 3（回滾）
//
// 閱讀提示（本檔用到的 JavaScript / Node.js 語法）：
//   import x from 'node:child_process'  Node.js 內建模組；child_process 用來執行外部程式
//   promisify(函式)                     把「callback 風格」的函式包成回傳 Promise 的版本，才能用 await
//   const { a, b } = 物件               解構：取出物件的 a、b 欄位
//   陣列.reduce(函式, 初始值)           把陣列「累積」成一個值：逐一把元素和目前結果交給函式，得到新結果
//   { ...obj, c: 1 }                    展開：複製 obj 的所有欄位，再加上 c
//   [...陣列1, ...陣列2]                展開：把兩個陣列的元素接成一個新陣列
// =============================================================================

// 白話：execFile 用來執行 kafka-configs.sh；promisify 把它變成可以 await 的版本；kafkajs 用來建 ACL。
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import kafkajs from 'kafkajs';

// 白話：從 kafkajs 取出 ACL 用到的常數（例如「允許」「topic」「READ」各自對應的代碼）。
// kafkajs 是 CommonJS，ACL 常數不在 Node 偵測得到的具名匯出裡，只能從 default 取
const { AclOperationTypes, AclPermissionTypes, AclResourceTypes, ResourcePatternTypes } = kafkajs;
// 白話：execFile 原本是 callback 風格，promisify 包成回傳 Promise 的版本，才能 await
const execFileAsync = promisify(execFile);

/**
 * 把文字裡出現的密碼換成 ***。
 *
 * 呼叫者：本檔的 runConfigs（失敗時處理錯誤訊息）；test/admins.test.js。
 *
 * 為什麼需要：kafka-configs.sh 失敗時會把完整參數（含 --add-config 的密碼）印進錯誤訊息，
 *   不遮掉的話密碼會進 log。
 *
 * @param {*} text           原始文字（通常是錯誤訊息）
 * @param {string[]} secrets  要遮掉的密碼清單；空值會被略過
 * @returns {string}  遮蔽後的文字
 *
 * 語法：secrets.filter(Boolean) 丟掉空值；
 *       .reduce((out, secret) => ..., String(text)) 從原始文字開始，每個密碼做一次取代，結果交給下一個；
 *       out.split(secret).join('***') 用密碼切開再用 *** 接回去，等於「全部取代」。
 *
 * @example
 *   redact('password=ExamplePass2026 failed', ['ExamplePass2026'])  // → 'password=*** failed'
 */
export function redact(text, secrets) {
  return secrets.filter(Boolean).reduce((out, secret) => out.split(secret).join('***'), String(text));
}

/**
 * 建立「執行 kafka-configs.sh」的函式，連線參數先固定好。
 *
 * 呼叫者：server.js 啟動時呼叫一次，結果（runConfigs）交給 createKafkaProvisioner。
 *
 * @param {object} options
 * @param {string} options.bin            Kafka 命令列工具目錄（/opt/kafka/bin）
 * @param {string} options.bootstrap      broker 位址（kafka:9092）
 * @param {string} options.commandConfig  admin 設定檔路徑（server.js 寫的 /tmp/admin.properties）
 * @param {number} [options.timeoutMs=60000]  超過這個時間就中止（JVM 啟動慢，給 60 秒）
 * @returns {Function}  runConfigs(args, secrets)：見下方
 */
export function createConfigsRunner({ bin, bootstrap, commandConfig, timeoutMs = 60_000 }) {
  /**
   * 子函式：執行一次 kafka-configs.sh，回傳它的輸出。
   *
   * 呼叫者：本檔 createKafkaProvisioner 裡的 userExists、createUser、removeUser。
   *
   * 實際執行的指令等於：
   *   /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --command-config /tmp/admin.properties <args...>
   *
   * @param {string[]} args       動作相關的參數，例如 ['--describe', '--entity-type', 'users', '--entity-name', 'team-a']
   * @param {string[]} [secrets=[]]  這次參數裡含有的密碼，失敗時要從錯誤訊息遮掉
   * @returns {Promise<string>}  指令的標準輸出（stdout）
   * @throws {Error}  指令失敗；訊息只含 stderr 第一行，且已遮蔽密碼
   */
  return async function runConfigs(args, secrets = []) {
    try {
      // 白話：執行 kafka-configs.sh 並等它結束，取得輸出。
      //   execFile(程式, 參數陣列, 選項)：直接執行程式，不經過 shell，
      //   所以參數裡的 ; | $() 等字元都只是普通文字。
      //   KAFKA_HEAP_OPTS 限制這個 JVM 最多用 128MB，避免每次申請都吃掉預設的大量記憶體
      // 語法：['--bootstrap-server', bootstrap, ..., ...args] 固定參數在前，args 展開接在後面；
      //       { ...process.env, KAFKA_HEAP_OPTS: ... } 複製目前所有環境變數，再加一個；
      //       const { stdout } = await ... 等執行完成，從結果物件取出 stdout 欄位。
      const { stdout } = await execFileAsync(
        `${bin}/kafka-configs.sh`,
        ['--bootstrap-server', bootstrap, '--command-config', commandConfig, ...args],
        { timeout: timeoutMs, env: { ...process.env, KAFKA_HEAP_OPTS: '-Xmx128m' } },
      );
      return stdout;
    } catch (err) {
      // 白話：只取 stderr 第一行非空白的內容當作原因（完整 stack trace 太長），並遮蔽密碼。
      // 語法：(a || b || '') 依序取第一個有值的；.split('\n') 切成多行；
      //       .find((line) => line.trim()) 找第一個去掉空白後不是空字串的行；?? '' 都找不到就用空字串。
      const detail = (err.stderr || err.message || '').split('\n').find((line) => line.trim()) ?? '';
      // 白話：丟出新的錯誤，例如「kafka-configs --alter 失敗（exit 1）：...」。
      throw new Error(`kafka-configs ${args[0]} 失敗（exit ${err.code}）：${redact(detail, secrets)}`);
    }
  };
}

/**
 * 產生面板帳號的 ACL 清單，固定三條。
 *
 * 呼叫者：本檔的 createUser（建立）與 removeUser（刪除）。兩邊用同一份，確保回滾時刪得乾淨。
 *
 *   topic orders：READ、DESCRIBE（LITERAL = 名稱完全相符）
 *   group <帳號>-*：READ（PREFIXED = 以這個字串開頭的 group 都算）
 *
 * @param {string} username  帳號名稱
 * @param {string} topic     topic 名稱（orders）
 * @returns {object[]}  kafkajs createAcls / deleteAcls 需要的 ACL 物件陣列
 *
 * 等於下面三條 kafka-acls.sh 指令：
 *   --add --allow-principal User:<帳號> --operation Read     --topic orders
 *   --add --allow-principal User:<帳號> --operation Describe --topic orders
 *   --add --allow-principal User:<帳號> --operation Read     --group <帳號>- --resource-pattern-type prefixed
 */
function aclEntries(username, topic) {
  // 白話：三條共用的欄位：principal 是 Kafka 的帳號表示法 User:<名稱>；host '*' = 從任何來源連線都適用；ALLOW = 允許。
  const base = { principal: `User:${username}`, host: '*', permissionType: AclPermissionTypes.ALLOW };
  // 白話：回傳三條 ACL，每條都是 base 再加上「資源類型、資源名稱、比對方式、操作」。
  // 語法：{ ...base, resourceType: ... } 複製 base 的欄位，再加上這條自己的欄位。
  return [
    { ...base, resourceType: AclResourceTypes.TOPIC, resourceName: topic, resourcePatternType: ResourcePatternTypes.LITERAL, operation: AclOperationTypes.READ },
    { ...base, resourceType: AclResourceTypes.TOPIC, resourceName: topic, resourcePatternType: ResourcePatternTypes.LITERAL, operation: AclOperationTypes.DESCRIBE },
    // 只能使用以自己帳號開頭的 consumer group，碰不到 order-consumers
    { ...base, resourceType: AclResourceTypes.GROUP, resourceName: `${username}-`, resourcePatternType: ResourcePatternTypes.PREFIXED, operation: AclOperationTypes.READ },
  ];
}

/**
 * 建立 Kafka 帳號管理器。
 *
 * 呼叫者：server.js 啟動時呼叫一次，結果放進 provisioners.kafka，交給 provision.js 使用。
 *
 * @param {object} options
 * @param {Function} options.runConfigs  createConfigsRunner 的結果
 * @param {object} options.admin         已建立的 kafkajs admin client（server.js 的 kafkaAdmin）
 * @param {string} options.topic         面板帳號能讀的 topic
 * @returns {{ userExists: Function, createUser: Function, removeUser: Function }}
 *   與 redis-admin.js 的 createRedisProvisioner 回傳相同形狀，provision.js 才能一視同仁地呼叫
 */
export function createKafkaProvisioner({ runConfigs, admin, topic }) {
  // 白話：子函式：產生「指定某個使用者」的共用參數。
  //   例如 userArgs('team-a') → ['--entity-type', 'users', '--entity-name', 'team-a']
  const userArgs = (username) => ['--entity-type', 'users', '--entity-name', username];

  return {
    /**
     * 子函式：查詢帳號是否已有 SCRAM 憑證。
     *
     * 呼叫者：provision.js 的階段 1（檢查重複）。
     *
     * 等於執行：kafka-configs.sh ... --describe --entity-type users --entity-name <帳號>
     *   有憑證時輸出會包含 "SCRAM credential configs"。
     *
     * @param {string} username
     * @returns {Promise<boolean>}  true = 已存在
     */
    async userExists(username) {
      const out = await runConfigs(['--describe', ...userArgs(username)]);
      // 語法：字串.includes('文字') 判斷字串裡有沒有這段文字。
      return out.includes('SCRAM credential configs');
    },

    /**
     * 子函式：建立帳號（先建 SCRAM 憑證，再建 ACL）。
     *
     * 呼叫者：provision.js 的階段 2（建立）。
     * 會呼叫：runConfigs、aclEntries、kafkajs 的 admin.createAcls。
     *
     * @param {string} username
     * @param {string} password
     * @returns {Promise<object>}  回傳給使用者看的權限摘要（不含密碼）
     */
    async createUser(username, password) {
      // 白話：建 SCRAM 憑證，等於 kafka-configs.sh ... --alter --entity-type users --entity-name <帳號>
      //       --add-config 'SCRAM-SHA-512=[password=<密碼>]'
      //   第二個參數 [password] 告訴 runConfigs 失敗時要遮掉密碼。
      await runConfigs(['--alter', ...userArgs(username), '--add-config', `SCRAM-SHA-512=[password=${password}]`], [password]);
      // 白話：建三條 ACL。
      await admin.createAcls({ acl: aclEntries(username, topic) });
      // 白話：回傳權限摘要，provision.js 收集後由 server.js 回給瀏覽器。
      return {
        mechanism: 'SCRAM-SHA-512',
        topic,
        operations: ['READ', 'DESCRIBE'],
        groupPrefix: `${username}-`,
      };
    },

    /**
     * 子函式：刪除帳號（回滾用）。順序與建立相反：先刪 ACL 再刪憑證。
     *
     * 呼叫者：provision.js 的階段 3（回滾，例如 Kafka 建好了但 Redis 失敗）。
     *
     * @param {string} username
     * @returns {Promise<void>}
     */
    async removeUser(username) {
      // 白話：刪掉三條 ACL（用同一份 aclEntries 當篩選條件）。
      await admin.deleteAcls({ filters: aclEntries(username, topic) });
      // 白話：刪掉 SCRAM 憑證，等於 kafka-configs.sh ... --alter ... --delete-config SCRAM-SHA-512
      await runConfigs(['--alter', ...userArgs(username), '--delete-config', 'SCRAM-SHA-512']);
    },
  };
}
