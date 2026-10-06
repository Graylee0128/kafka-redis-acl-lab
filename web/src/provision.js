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
// =============================================================================

export async function provisionAccess({ username, password, targets }, provisioners) {
  // ['kafka', 'redis'] → [['kafka', kafka的provisioner], ['redis', redis的provisioner]]
  const selected = targets.map((target) => [target, provisioners[target]]);

  // 第 1 步：任何一個目標已有同名帳號就整筆拒絕，一個都不建（不覆寫別人的帳號）
  for (const [target, provisioner] of selected) {
    if (await provisioner.userExists(username)) {
      return { status: 'conflict', target };
    }
  }

  // 第 2 步：逐一建立。granted 收集每個目標授予的權限（回傳給使用者看），
  // created 記錄已經建好的，失敗時才知道要回滾哪些
  const granted = {};
  const created = [];
  try {
    for (const [target, provisioner] of selected) {
      granted[target] = await provisioner.createUser(username, password);
      created.push([target, provisioner]);
    }
  } catch (err) {
    // 第 3 步（失敗時）：反向刪除已建立的帳號（後建的先刪）。
    // 回滾本身也可能失敗，失敗的部分附在原本的錯誤上（rollbackFailures），由 server.js 記進 log
    for (const [target, provisioner] of created.reverse()) {
      try {
        await provisioner.removeUser(username);
      } catch (rollbackErr) {
        err.rollbackFailures = [...(err.rollbackFailures ?? []), { target, error: rollbackErr.message }];
      }
    }
    // 回滾完仍把原本的錯誤往上丟，讓 server.js 回 500
    throw err;
  }

  return { status: 'created', granted };
}
