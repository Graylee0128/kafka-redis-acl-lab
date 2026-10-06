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
// =============================================================================
import { classifySeq } from './event.js';

// redis 由呼叫端傳入（已連線的 node-redis client），測試時可以換成假的物件（test/store.test.js）
export function createEventStore(redis, { prefix, ttlSeconds }) {
  const latestKey = (orderId) => `${prefix}:events:${orderId}`;
  const historyKey = (orderId) => `${prefix}:history:${orderId}`;

  // 回傳 classifySeq 的結果（'in_order' / 'gap' / 'duplicate'），讓 index.js 決定記哪種 log
  async function apply(event, { consumerId, partition, offset }) {
    // 1. 讀出目前已存的最新狀態，取它的 seq（第一次出現時 get 回傳 null）
    const previous = await redis.get(latestKey(event.order_id));
    const lastSeq = previous ? JSON.parse(previous).seq : null;
    // 2. 判斷這筆是正常、跳號還是重複；重複就什麼都不寫
    const outcome = classifySeq(lastSeq, event.seq);
    if (outcome === 'duplicate') return outcome;

    // 3. 原始事件加上「誰處理、哪個 partition、哪個 offset」，驗證負載平衡時用得到
    const record = { ...event, handled_by: consumerId, partition, offset };
    // SET 覆寫最新狀態；{ EX: 秒數 } 同時設定 TTL
    await redis.set(latestKey(event.order_id), JSON.stringify(record), { EX: ttlSeconds });
    // RPUSH 加到 LIST 尾端，LIST 的順序就是處理順序
    await redis.rPush(historyKey(event.order_id), JSON.stringify(record));
    // LIST 沒有「寫入時順便設 TTL」的參數，另外用 EXPIRE 設定（每次寫入都會重設倒數）
    await redis.expire(historyKey(event.order_id), ttlSeconds);
    return outcome;
  }

  return { apply };
}
