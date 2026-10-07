// =============================================================================
// 把訂單事件寫進 Redis
// =============================================================================
//
// 誰呼叫：index.js 的 handleMessage，通過 parseEvent 驗證後呼叫 store.apply()。
// 用哪個帳號：Redis app（只能碰 myapp:*，指令只開 get / set / rpush / expire 等）。
//
// 寫入兩種 key：
//   <prefix>:events:<order_id>   STRING  最新狀態（JSON，含處理者 consumer 與 partition）
//   <prefix>:history:<order_id>  LIST    每筆處理紀錄，驗證順序與負載平衡用（scripts/check-order.sh 讀它）
//
// 冪等：at-least-once 投遞在 rebalance 後可能重送，seq 不大於已存的就當重複、直接略過。
// 同一個 order_id 只會在同一個 partition，同一時間只有一個 consumer 處理，所以這裡的 get → set 不會互相競爭。
//
// 結構：createEventStore 是「工廠函式」，執行一次、回傳 { apply }。
//   裡面的 latestKey / historyKey / apply 都是子函式（closure），
//   記得建立時傳入的 redis、prefix、ttlSeconds，之後呼叫 apply 不用再傳這些。
//
// 閱讀提示（本檔用到的 JavaScript 語法）：
//   import { a } from './x.js'   從 x.js 拿出它 export 的 a 來用
//   async function / await       非同步：Redis 指令要經過網路，await 會「等它回來」再往下一行，
//                                期間 Node.js 可以去處理別的事，不會整個卡住
//   (x) => 運算式                箭頭函式，等於 function (x) { return 運算式; }
//   { a, b } = 物件              解構：從物件取出 a、b 兩個欄位，變成同名變數
//   { ...obj, c: 1 }             展開：複製 obj 的所有欄位，再加上（或覆蓋）c
//   條件 ? 甲 : 乙               三元運算：條件成立取甲，否則取乙（類似 if/else 的單行版）
// =============================================================================

// 白話：從 event.js 拿 classifySeq 來用（判斷正常 / 跳號 / 重複）。
import { classifySeq } from './event.js';

/**
 * 建立事件儲存器：把 Redis client 與 key 規則包起來，對外只露出 apply()。
 *
 * 呼叫者：index.js 啟動時呼叫一次，結果存在 `store` 變數；之後 handleMessage 每筆訊息呼叫 store.apply()。
 *
 * @param {object} redis  已建立的 node-redis client（index.js 的 createClient 結果，以 app 帳號登入）。
 *                        由外部傳入而不是在這裡建立：測試時可以換成假的物件（test/store.test.js）。
 * @param {object} options
 * @param {string} options.prefix      Redis key 前綴（config.keyPrefix，預設 myapp），必須符合 app 帳號的 ~myapp:*
 * @param {number} options.ttlSeconds  key 保留秒數（config.ttlSeconds，預設 86400）
 * @returns {{ apply: Function }}  只有一個方法的物件，見下方 apply
 *
 * 語法：第二個參數 { prefix, ttlSeconds } 是「解構」：呼叫端傳一個物件進來，
 *       這裡直接把它的 prefix、ttlSeconds 欄位取出來當變數用。
 *       呼叫端寫法：createEventStore(redis, { prefix: 'myapp', ttlSeconds: 86400 })
 */
export function createEventStore(redis, { prefix, ttlSeconds }) {
  /**
   * 子函式：組出「最新狀態」的 key。
   *
   * 呼叫者：本檔的 apply（讀取時 GET、寫入時 SET 都用它，確保兩邊 key 一致）。
   *
   * @param {string} orderId  訂單編號，已由 event.js 的 ORDER_ID_PATTERN 驗證過，只含安全字元
   * @returns {string}  例如 'myapp:events:omut8ampr-0003'
   *
   * 語法：const 名稱 = (參數) => 回傳值; 用箭頭函式定義一個小函式並存進變數。
   *       `${prefix}:events:${orderId}` 是樣板字串，${} 的值會被代入。
   *       prefix 是外層 createEventStore 的參數；子函式「記得」它（這就是 closure）。
   */
  const latestKey = (orderId) => `${prefix}:events:${orderId}`;

  /**
   * 子函式：組出「處理歷程」的 key。
   *
   * 呼叫者：本檔的 apply（RPUSH 與 EXPIRE）。scripts/check-order.sh 以 myapp:history:* 掃這些 key。
   *
   * @param {string} orderId  訂單編號
   * @returns {string}  例如 'myapp:history:omut8ampr-0003'
   */
  const historyKey = (orderId) => `${prefix}:history:${orderId}`;

  /**
   * 子函式（對外唯一的方法）：處理一筆已驗證的事件，必要時寫入 Redis。
   *
   * 呼叫者：index.js 的 handleMessage → store.apply(event, {...})。
   * 會呼叫：event.js 的 classifySeq（判斷正常 / 跳號 / 重複）。
   *
   * 步驟：
   *   1. GET 最新狀態，取出已存的 seq
   *   2. classifySeq 判斷；重複就直接回傳，不寫入
   *   3. SET 最新狀態（含 TTL）→ RPUSH 歷程 → EXPIRE 歷程
   *
   * 錯誤處理：這裡不 catch。Redis 指令失敗會把例外丟回 handleMessage，
   *   再丟給 kafkajs，kafkajs 就不 commit 這筆 offset 並重試（訊息不會遺失）。
   *
   * @param {object} event  parseEvent 驗證過的事件：{ order_id, seq, status, amount, ts }
   * @param {object} meta   處理資訊，會一起寫進 Redis，供驗證負載平衡
   * @param {string} meta.consumerId  哪個實體處理的（config.consumerId，容器 hostname）
   * @param {number} meta.partition   訊息來自哪個 partition
   * @param {string} meta.offset      訊息在 partition 內的位置（kafkajs 給的是字串）
   * @returns {Promise<'in_order'|'gap'|'duplicate'>}  classifySeq 的結果，handleMessage 依它決定記哪種 log
   *
   * 語法：async function 表示這個函式裡會用 await 等待網路操作；
   *       呼叫它會得到一個 Promise（「之後才會有結果」的物件），呼叫端要 await 才拿得到回傳值。
   */
  async function apply(event, { consumerId, partition, offset }) {
    // 白話【步驟 1】到 Redis 讀這筆訂單目前的最新狀態，相當於 redis-cli GET myapp:events:<order_id>。
    //   第一次出現時 Redis 沒有這個 key，previous 會是 null。
    // 語法：await 等 Redis 回應後才把結果放進 previous，再執行下一行。
    const previous = await redis.get(latestKey(event.order_id));
    // 白話：有舊資料就把 JSON 字串解析成物件，取出它的 seq；沒有就是 null。
    // 語法：條件 ? 甲 : 乙 —— previous 有值（不是 null）就算 JSON.parse(previous).seq，否則得到 null。
    const lastSeq = previous ? JSON.parse(previous).seq : null;
    // 白話【步驟 2】判斷這筆是正常、跳號還是重複；重複就什麼都不寫，直接回傳 'duplicate'。
    const outcome = classifySeq(lastSeq, event.seq);
    if (outcome === 'duplicate') return outcome;

    // 白話【步驟 3】原始事件加上「誰處理、哪個 partition、哪個 offset」，驗證負載平衡時用得到。
    //   例如 { order_id: 'o1-0001', seq: 2, status: 'paid', ..., handled_by: 'a1b2c3', partition: 0, offset: '57' }
    // 語法：...event 把 event 的所有欄位複製進新物件；後面的 handled_by: consumerId 是新增欄位；
    //       partition、offset 是 partition: partition、offset: offset 的簡寫。
    //       不直接修改 event，而是做一份新的（test/store.test.js 會檢查 event 沒被改到）。
    const record = { ...event, handled_by: consumerId, partition, offset };
    // 白話：寫入最新狀態並設定過期時間，相當於 redis-cli SET myapp:events:<id> '<json>' EX 86400。
    // 語法：JSON.stringify(物件) 把物件轉成 JSON 字串（Redis 只能存字串）；
    //       { EX: ttlSeconds } 是 node-redis 的選項寫法，對應 SET 指令的 EX 參數。
    await redis.set(latestKey(event.order_id), JSON.stringify(record), { EX: ttlSeconds });
    // 白話：把這筆加到處理歷程 LIST 的尾端，相當於 redis-cli RPUSH myapp:history:<id> '<json>'。
    //   LIST 的順序就是處理順序，check-order.sh 就是靠它檢查 seq 有沒有連續。
    await redis.rPush(historyKey(event.order_id), JSON.stringify(record));
    // 白話：設定歷程的過期時間，相當於 redis-cli EXPIRE myapp:history:<id> 86400。
    //   LIST 沒有「寫入時順便設 TTL」的參數，所以另外下一個指令（每次寫入都會重設倒數）。
    await redis.expire(historyKey(event.order_id), ttlSeconds);
    // 白話：回傳 'in_order' 或 'gap'，讓 handleMessage 決定記哪種 log。
    return outcome;
  }

  // 白話：把 apply 包在物件裡回傳，呼叫端就能寫 store.apply(...)。
  //   latestKey、historyKey 沒有放進來，所以外部用不到（只在本函式內部使用）。
  // 語法：{ apply } 是 { apply: apply } 的簡寫。
  return { apply };
}
