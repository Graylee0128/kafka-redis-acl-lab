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
// =============================================================================

// 唯讀、只限 <prefix>:*。刻意不給 KEYS / SCAN：這兩個指令不受 key pattern 限制，會洩漏其他 key 名稱
// 各規則的意思（與 entrypoint.sh 的 ACL 檔語法相同）：
//   reset          先清空這個帳號的所有設定，確保從零開始
//   on             啟用帳號
//   >密碼          設定明文密碼（Redis 自己會存成雜湊）
//   ~myapp:*       只能存取這個前綴的 key
//   resetchannels  不能用 Pub/Sub
//   -@all          先拿掉全部指令，再用 + 逐一加回唯讀指令（白名單）
//   ping、hello、client|setinfo、client|setname → client 連線握手需要
const readOnlyRules = (password, prefix) => [
  'reset', 'on', `>${password}`, `~${prefix}:*`, 'resetchannels', '-@all',
  '+get', '+exists', '+ttl', '+type', '+strlen', '+lrange', '+llen',
  '+ping', '+hello', '+client|setinfo', '+client|setname',
];

// client：server.js 傳入、以 admin 登入的 node-redis client
export function createRedisProvisioner(client, { prefix }) {
  return {
    // ACL GETUSER 對不存在的帳號回傳 null
    async userExists(username) {
      return (await client.sendCommand(['ACL', 'GETUSER', username])) !== null;
    },

    // sendCommand 以陣列送出，每個元素是一個獨立參數，不會因為內容有空白而被拆開
    async createUser(username, password) {
      await client.sendCommand(['ACL', 'SETUSER', username, ...readOnlyRules(password, prefix)]);
      await client.sendCommand(['ACL', 'SAVE']);
      // 回傳給使用者看的權限摘要（不含密碼）
      return {
        keyPattern: `${prefix}:*`,
        commands: ['GET', 'EXISTS', 'TTL', 'TYPE', 'STRLEN', 'LRANGE', 'LLEN'],
        readOnly: true,
      };
    },

    // 回滾用：刪掉帳號並寫回 aclfile
    async removeUser(username) {
      await client.sendCommand(['ACL', 'DELUSER', username]);
      await client.sendCommand(['ACL', 'SAVE']);
    },
  };
}
