// =============================================================================
// 申請流程：檢查重複 → 逐一建立 → 失敗回滾
// =============================================================================
//
// 誰呼叫：server.js 的 handleAccessRequest，驗證通過後呼叫 provisionAccess。
//
// 先確認所有目標都沒有同名帳號，再逐一建立；中途失敗就回滾已建立的部分，
// 避免留下「Kafka 有、Redis 沒有」的半套帳號，讓使用者重試時永遠卡在「帳號已存在」。
//
// provisioners 是 { kafka: ..., redis: ... }，每個都有相同的三個方法：
//   userExists(username) / createUser(username, password) / removeUser(username)
// 實作在 kafka-admin.js 與 redis-admin.js。這裡只管流程，不管怎麼建帳號，
// 所以測試時可以換成假的 provisioner（test/provision.test.js）。
//
// 閱讀提示（本檔用到的 JavaScript 語法）：
//   for (const [a, b] of 陣列)  逐一取出元素；每個元素本身是 [a, b] 兩個值，直接拆成兩個變數
//   物件[變數] = 值             以變數的值當欄位名稱寫入，例如 granted['kafka'] = {...}
//   陣列.reverse()              把陣列順序反過來
//   [...舊陣列, 新元素]          展開：複製舊陣列的元素，再加上新元素，組成新陣列
//   a ?? b                      a 是 null / undefined 時用 b
//   throw err                   把錯誤再往上丟給呼叫者
// =============================================================================

/**
 * 依申請內容在各目標建立帳號；任何一步失敗就回滾。
 *
 * 呼叫者：server.js 的 handleAccessRequest。
 * 會呼叫：provisioners 裡各目標的 userExists / createUser / removeUser
 *         （實作在 kafka-admin.js 的 createKafkaProvisioner、redis-admin.js 的 createRedisProvisioner）。
 *
 * 三個階段：
 *   1. 檢查：所有選擇的目標都沒有同名帳號，否則整筆拒絕（一個都不建，也不覆寫別人的帳號）
 *   2. 建立：依序在每個目標建帳號，記住建好了哪些
 *   3. 回滾（只在 2 失敗時）：反向刪除已建立的帳號，再把原本的錯誤往上丟
 *
 * @param {object} request  validate.js 驗證過的申請內容
 * @param {string} request.username
 * @param {string} request.password
 * @param {string[]} request.targets  例如 ['kafka', 'redis']
 * @param {object} provisioners  { kafka: {...}, redis: {...} }，由 server.js 組裝後傳入
 * @returns {Promise<{ status: 'created', granted: object } | { status: 'conflict', target: string }>}
 *   created ：granted 是每個目標的權限摘要，例如 { kafka: {...}, redis: {...} }，server.js 回 201
 *   conflict：target 是哪個目標已有同名帳號，server.js 回 409
 * @throws {Error}  建立失敗（已回滾）；若回滾也失敗，錯誤物件上會多一個 rollbackFailures 陣列
 */
export async function provisionAccess({ username, password, targets }, provisioners) {
  // 白話：['kafka', 'redis'] → [['kafka', kafka的provisioner], ['redis', redis的provisioner]]
  //   把「目標名稱」和「負責建帳號的物件」配成一對，後面的迴圈兩個都要用。
  // 語法：targets.map((target) => [...]) 對每個目標回傳一個兩元素的陣列。
  const selected = targets.map((target) => [target, provisioners[target]]);

  // 白話【階段 1】任何一個目標已有同名帳號就整筆拒絕，一個都不建（不覆寫別人的帳號）。
  // 語法：await provisioner.userExists(username) 等該目標查詢完成，得到 true / false。
  for (const [target, provisioner] of selected) {
    if (await provisioner.userExists(username)) {
      return { status: 'conflict', target };
    }
  }

  // 白話【階段 2】逐一建立。
  //   granted：收集每個目標授予的權限（回傳給使用者看）
  //   created：記錄已經建好的，失敗時才知道要回滾哪些
  const granted = {};
  const created = [];
  try {
    for (const [target, provisioner] of selected) {
      // 白話：建帳號並把權限摘要存進 granted，例如 granted['kafka'] = { mechanism: 'SCRAM-SHA-512', ... }
      granted[target] = await provisioner.createUser(username, password);
      // 白話：建成功才記進 created。
      created.push([target, provisioner]);
    }
  } catch (err) {
    // 白話【階段 3】建立途中失敗：反向刪除已建立的帳號（後建的先刪）。
    //   回滾本身也可能失敗，失敗的部分附在原本的錯誤上（rollbackFailures），由 server.js 記進 log
    for (const [target, provisioner] of created.reverse()) {
      try {
        await provisioner.removeUser(username);
      } catch (rollbackErr) {
        // 語法：err.rollbackFailures ?? []：第一次還沒有這個欄位（undefined），就從空陣列開始；
        //       [...舊陣列, 新元素] 組出加了一筆的新陣列，再存回 err.rollbackFailures。
        err.rollbackFailures = [...(err.rollbackFailures ?? []), { target, error: rollbackErr.message }];
      }
    }
    // 白話：回滾完仍把原本的錯誤往上丟，讓 server.js 回 500
    throw err;
  }

  // 白話：全部建好，回傳成功與權限摘要。
  return { status: 'created', granted };
}
