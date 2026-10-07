// =============================================================================
// Redis 帳號管理（面板申請的帳號）
// =============================================================================
//
// 誰呼叫：provision.js 透過 { userExists, createUser, removeUser } 三個方法使用。
// 用哪個帳號：Redis admin（server.js 建立 client 時登入）。
//
// ACL SETUSER 建立帳號後立刻 ACL SAVE 寫回 aclfile（位於 volume），重啟後仍在。
// （infra/redis/entrypoint.sh 重啟時只重寫內建帳號，面板建立的帳號原樣保留。）
// 指令以參數陣列送出，帳號與密碼已先經 validate.js 限制字元，不會被解析成額外的 ACL 規則。
//
// 結構：createRedisProvisioner 是「工廠函式」，回傳一個有三個方法的物件。
//   三個方法都是子函式，共用外層傳進來的 client 與 prefix。
//   kafka-admin.js 的 createKafkaProvisioner 回傳相同形狀的物件，provision.js 才能一視同仁地呼叫。
//
// 閱讀提示（本檔用到的 JavaScript 語法）：
//   (a, b) => [ ... ]       箭頭函式回傳一個陣列
//   [ 'x', ...陣列 ]        展開：把陣列的元素一個個放進新陣列
//   { async f() { } }       物件裡定義方法（函式）；呼叫時寫 物件.f()
//   async / await           await 等 Redis 回應再往下（指令要經過網路）
// =============================================================================

/**
 * 產生面板帳號的 ACL 規則（唯讀、只限 <prefix>:*）。
 *
 * 呼叫者：本檔的 createUser。
 *
 * 刻意不給 KEYS / SCAN：這兩個指令不受 key pattern 限制，會洩漏其他 key 名稱。
 * 各規則的意思（與 entrypoint.sh 的 ACL 檔語法相同）：
 *   reset          先清空這個帳號的所有設定，確保從零開始
 *   on             啟用帳號
 *   >密碼          設定明文密碼（Redis 自己會存成雜湊）
 *   ~myapp:*       只能存取這個前綴的 key
 *   resetchannels  不能用 Pub/Sub
 *   -@all          先拿掉全部指令，再用 + 逐一加回唯讀指令（白名單）
 *   ping、hello、client|setinfo、client|setname → client 連線握手需要
 *
 * @param {string} password  使用者申請的密碼（已由 validate.js 確認只有英數字）
 * @param {string} prefix    key 前綴（config.keyPrefix，預設 myapp）
 * @returns {string[]}  規則陣列，會接在 ACL SETUSER <帳號> 後面
 *
 * @example
 *   readOnlyRules('ExamplePass2026', 'myapp')
 *   // → ['reset', 'on', '>ExamplePass2026', '~myapp:*', 'resetchannels', '-@all', '+get', ...]
 *   // 等於 redis-cli ACL SETUSER team-a reset on >ExamplePass2026 ~myapp:* resetchannels -@all +get ...
 */
const readOnlyRules = (password, prefix) => [
  'reset', 'on', `>${password}`, `~${prefix}:*`, 'resetchannels', '-@all',
  '+get', '+exists', '+ttl', '+type', '+strlen', '+lrange', '+llen',
  '+ping', '+hello', '+client|setinfo', '+client|setname',
];

/**
 * 建立 Redis 帳號管理器。
 *
 * 呼叫者：server.js 啟動時呼叫一次，結果放進 provisioners.redis，交給 provision.js 使用。
 *
 * @param {object} client  server.js 傳入、以 admin 登入的 node-redis client
 * @param {object} options
 * @param {string} options.prefix  面板帳號能存取的 key 前綴
 * @returns {{ userExists: Function, createUser: Function, removeUser: Function }}
 */
export function createRedisProvisioner(client, { prefix }) {
  // 白話：回傳一個物件，裡面有三個方法。provision.js 會呼叫 provisioners.redis.userExists(...) 等。
  return {
    /**
     * 子函式：查詢帳號是否已存在。
     *
     * 呼叫者：provision.js 的階段 1（檢查重複）。
     *
     * @param {string} username
     * @returns {Promise<boolean>}  true = 已存在
     */
    async userExists(username) {
      // 白話：送出 ACL GETUSER <帳號>；帳號不存在時 Redis 回傳 null。
      //   等於 redis-cli ACL GETUSER team-a
      // 語法：sendCommand([...]) 用陣列送出原始指令，每個元素是一個參數；!== null 判斷「不是 null」。
      return (await client.sendCommand(['ACL', 'GETUSER', username])) !== null;
    },

    /**
     * 子函式：建立唯讀帳號並寫回 aclfile。
     *
     * 呼叫者：provision.js 的階段 2（建立）。
     * 會呼叫：本檔的 readOnlyRules。
     *
     * @param {string} username
     * @param {string} password
     * @returns {Promise<object>}  回傳給使用者看的權限摘要（不含密碼）
     */
    async createUser(username, password) {
      // 白話：送出 ACL SETUSER <帳號> <規則...>，建立帳號。
      // 語法：...readOnlyRules(password, prefix) 把規則陣列的元素一個個展開，接在 username 後面。
      //       陣列送出，每個元素是一個獨立參數，不會因為內容有空白而被拆開。
      await client.sendCommand(['ACL', 'SETUSER', username, ...readOnlyRules(password, prefix)]);
      // 白話：送出 ACL SAVE，把目前所有帳號寫回 /data/users.acl（重啟後才不會消失）。
      await client.sendCommand(['ACL', 'SAVE']);
      // 白話：回傳權限摘要，provision.js 收集後由 server.js 回給瀏覽器。
      return {
        keyPattern: `${prefix}:*`,
        commands: ['GET', 'EXISTS', 'TTL', 'TYPE', 'STRLEN', 'LRANGE', 'LLEN'],
        readOnly: true,
      };
    },

    /**
     * 子函式：刪除帳號並寫回 aclfile。
     *
     * 呼叫者：provision.js 的階段 3（回滾）。
     *   validate.js 把建立順序固定成 kafka → redis，Redis 是最後一個，
     *   所以目前實際上不會被回滾；保留這個方法是為了和 kafka-admin.js 介面一致（provision.js 不需要知道順序）。
     *
     * @param {string} username
     * @returns {Promise<void>}
     */
    async removeUser(username) {
      // 白話：等於 redis-cli ACL DELUSER team-a，再 ACL SAVE。
      await client.sendCommand(['ACL', 'DELUSER', username]);
      await client.sendCommand(['ACL', 'SAVE']);
    },
  };
}
