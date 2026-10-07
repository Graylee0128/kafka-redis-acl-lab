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
//
// 結構：createOrderGenerator 是「工廠函式」，執行一次、回傳 next 函式。
//   newOrder 與 next 都是子函式（closure），共用外層的 pool 與 nextNumber，
//   所以每次呼叫 next() 都接著上一次的狀態。
//
// 閱讀提示（本檔用到的 JavaScript 語法）：
//   { a = 5, b } = {}       參數解構 + 預設值：呼叫端傳一個物件，取出 a、b；a 沒傳就用 5
//   () => ({ ... })         箭頭函式回傳物件：物件外面要多包一層 ( )，否則 { } 會被當成函式本體
//   x++                     先取 x 的值，再把 x 加 1
//   條件 ? 甲 : 乙          三元運算：條件成立取甲，否則取乙
//   { ...obj, c: 1 }        展開：複製 obj 的所有欄位，再覆蓋 c
//   陣列.map((o, i) => ...) 對每個元素（o）與它的位置（i）算出新值，組成新陣列
// =============================================================================

// 白話：訂單狀態清單，陣列位置就是 stage（0 = created … 3 = delivered）。
// 語法：Object.freeze 讓陣列不能被修改；export 讓測試檔可以 import 它。
export const STATUSES = Object.freeze(['created', 'paid', 'shipped', 'delivered']);

/**
 * 建立訂單事件產生器。
 *
 * 呼叫者：index.js 啟動時呼叫一次，結果存在 nextEvent 變數；之後每次送訊息呼叫 nextEvent()。
 *         test/orders.test.js 也會呼叫，並傳入固定的 random / now。
 *
 * @param {object} [options]
 * @param {number} [options.activeOrders=5]  同時進行中的訂單數（config.activeOrders）
 * @param {string} options.runId             每次啟動不同的前綴（index.js 產生），讓 order_id 不會跟上次啟動的撞號；必填
 * @param {Function} [options.random=Math.random]  回傳 0～1 隨機數的函式；測試時換成固定序列
 * @param {Function} [options.now]                 回傳時間字串的函式；預設是現在時間
 * @returns {Function}  next()：每呼叫一次回傳一筆事件，見下方
 * @throws {Error}  沒有傳 runId
 *
 * 語法：參數用解構 + 預設值：呼叫端只要傳 runId，其他沒傳就用 = 後面的預設。
 *       最後的 = {} 讓「完全不傳參數」也不會在解構時當掉（會改由下面的 runId 檢查報錯）。
 */
export function createOrderGenerator({
  activeOrders = 5,
  runId,
  random = Math.random,
  now = () => new Date().toISOString(),
} = {}) {
  // 白話：沒有 runId 就無法產生唯一的 order_id，直接報錯。
  if (!runId) throw new Error('runId is required');

  // 白話：下一張新訂單的流水號，從 1 開始。
  // 語法：let 宣告可以改值的變數（每建一張新訂單就 +1）。
  let nextNumber = 1;

  /**
   * 子函式：建立一張新訂單（尚未送出任何事件，停在 stage 0 = created）。
   *
   * 呼叫者：本函式建立初始訂單池時；next() 在某張訂單走完 delivered 時補新的。
   *
   * @returns {{ orderId: string, stage: number, amount: number }}
   *   orderId：例如 omut8ampr-0003（runId + 4 位數流水號）
   *   stage  ：目前走到哪個狀態（STATUSES 的位置）
   *   amount ：0～1000 的隨機金額，同一張訂單各階段都一樣
   *
   * 語法：String(nextNumber++).padStart(4, '0')
   *         nextNumber++     取目前的號碼，然後把 nextNumber 加 1
   *         String(3)        數字轉字串 '3'
   *         .padStart(4,'0') 左邊補 0 到 4 位 → '0003'
   *       Math.round(random() * 100_000) / 100：隨機數 × 100000 取整數再除以 100 → 兩位小數的金額
   *       （100_000 的底線只是方便閱讀，等於 100000）
   */
  const newOrder = () => ({
    orderId: `${runId}-${String(nextNumber++).padStart(4, '0')}`,
    stage: 0,
    amount: Math.round(random() * 100_000) / 100,
  });

  // 白話：建立初始訂單池，裡面有 activeOrders 張新訂單。
  // 語法：Array.from({ length: N }, 函式) 呼叫函式 N 次，把回傳值組成陣列。
  let pool = Array.from({ length: activeOrders }, newOrder);

  /**
   * 子函式（回傳給呼叫端的就是它）：產生下一筆事件，並推進那張訂單的狀態。
   *
   * 呼叫者：index.js 的 produceOne（透過 nextEvent 變數），每送一筆訊息呼叫一次。
   *
   * 步驟：
   *   1. 隨機挑池子裡的一張訂單
   *   2. 依它目前的 stage 組出事件（seq = stage + 1）
   *   3. 推進這張訂單：還沒走完就 stage + 1；走完 delivered 就換成一張新訂單
   *
   * @returns {{ order_id: string, seq: number, status: string, amount: number, ts: string }}
   *   這就是 Kafka 訊息的 value 內容（index.js 會 JSON.stringify 後送出）
   *
   * @example
   *   const next = createOrderGenerator({ runId: 'o1' });
   *   next() // → { order_id: 'o1-0003', seq: 1, status: 'created', amount: 512.37, ts: '...' }
   *   next() // → { order_id: 'o1-0001', seq: 1, status: 'created', ... }（隨機挑到別張）
   */
  return function next() {
    // 白話【1】隨機挑一個位置。random() 是 0～1（不含 1），乘以池子大小後取整數 → 0 ～ 池子大小-1。
    // 語法：Math.floor 無條件捨去；pool[index] 取陣列中第 index 個元素（從 0 開始算）。
    const index = Math.floor(random() * pool.length);
    const order = pool[index];
    // 白話【2】這次要送出的事件內容（就是 Kafka 訊息的 value）。
    //   seq 從 1 開始，所以是 stage + 1；status 從 STATUSES 依 stage 取出對應的名稱。
    const event = {
      order_id: order.orderId,
      seq: order.stage + 1,
      status: STATUSES[order.stage],
      amount: order.amount,
      ts: now(),
    };

    // 白話【3】推進這筆訂單：已經是最後一個狀態（delivered）就換成新訂單，否則 stage + 1。
    // 語法：條件 ? 甲 : 乙；{ ...order, stage: ... } 是複製一份再改 stage，不直接修改原物件（不可變更新）
    const isLastStage = order.stage + 1 >= STATUSES.length;
    const advanced = isLastStage ? newOrder() : { ...order, stage: order.stage + 1 };
    // 白話：產生新的池子，只把第 index 格換成推進後的訂單，其他格不變。
    // 語法：map 對每個元素 o（位置 i）回傳新值：i 等於 index 就換成 advanced，否則保留原本的 o。
    pool = pool.map((o, i) => (i === index ? advanced : o));

    return event;
  };
}
