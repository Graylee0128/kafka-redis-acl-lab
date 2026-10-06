// =============================================================================
// 模擬訂單事件產生器（純函式，不碰 Kafka，方便測試）
// =============================================================================
//
// 誰呼叫：index.js 啟動時建立一個產生器，之後每次要送訊息就呼叫一次 next()。
//
// 做什麼：
//   同時維持 activeOrders 筆「進行中」的訂單（訂單池），每次隨機挑一筆推進一個狀態：
//     created → paid → shipped → delivered
//   走完 delivered 的訂單從池子移除，補一筆新訂單進來，所以池子大小固定。
//
// 為什麼要「多筆訂單交錯」：
//   如果一次只做一筆訂單，所有事件天生就有順序，驗證不出什麼。
//   多筆交錯送出，同一個 partition 裡會混著不同訂單的事件，
//   才能證明「同一個 order_id 的事件仍然依序」。
//
// seq：同一筆訂單內的序號，從 1 連續遞增（created=1、paid=2、shipped=3、delivered=4）。
//   consumer 用它判斷順序（event.js 的 classifySeq）與去重（store.js）。
//
// 純函式的意思：隨機數 random 與時間 now 都可以從外面傳入，
//   測試時傳固定值，每次結果都一樣（test/orders.test.js）。
// =============================================================================

// 訂單狀態，陣列索引就是 stage（0 = created … 3 = delivered）
export const STATUSES = Object.freeze(['created', 'paid', 'shipped', 'delivered']);

// 參數用解構 + 預設值：呼叫端只要傳 runId，其他沒傳就用 = 後面的預設。
// 最後的 = {} 讓「完全不傳參數」也不會在解構時當掉（會改由下面的 runId 檢查報錯）。
// runId：每次啟動不同的前綴（見 index.js），讓 order_id 不會跟上次啟動的訊息撞號。
export function createOrderGenerator({
  activeOrders = 5,
  runId,
  random = Math.random,
  now = () => new Date().toISOString(),
} = {}) {
  if (!runId) throw new Error('runId is required');

  // 下一張新訂單的流水號。nextNumber++ 是「先取值再加 1」
  // orderId 例如 omut8ampr-0003：padStart 把 3 補成 0003，讓長度一致
  // amount：0～1000 的隨機金額，取到小數第二位（100_000 的底線只是數字分隔，等於 100000）
  let nextNumber = 1;
  const newOrder = () => ({
    orderId: `${runId}-${String(nextNumber++).padStart(4, '0')}`,
    stage: 0,
    amount: Math.round(random() * 100_000) / 100,
  });

  // 初始訂單池：Array.from({ length: N }, fn) 呼叫 fn N 次，產生 N 筆新訂單
  let pool = Array.from({ length: activeOrders }, newOrder);

  // 回傳的 next 是 closure：它記得上面的 pool 與 nextNumber，每次呼叫都接著上次的狀態
  return function next() {
    // 隨機挑池子裡的一筆
    const index = Math.floor(random() * pool.length);
    const order = pool[index];
    // 這次要送出的事件內容（就是 Kafka 訊息的 value）
    const event = {
      order_id: order.orderId,
      seq: order.stage + 1,
      status: STATUSES[order.stage],
      amount: order.amount,
      ts: now(),
    };

    // 推進這筆訂單：已經是最後一個狀態就換成新訂單，否則 stage + 1。
    // { ...order, stage: ... } 是複製一份再改 stage，不直接修改原物件（不可變更新）
    const isLastStage = order.stage + 1 >= STATUSES.length;
    const advanced = isLastStage ? newOrder() : { ...order, stage: order.stage + 1 };
    // 用 map 產生新陣列，只替換 index 那一格
    pool = pool.map((o, i) => (i === index ? advanced : o));

    return event;
  };
}
