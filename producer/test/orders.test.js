import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createOrderGenerator, STATUSES } from '../src/orders.js';

// 可重現的假亂數（LCG），讓測試結果固定
function seededRandom(seed = 42) {
  let s = seed;
  return () => {
    s = (s * 1664525 + 1013904223) % 2 ** 32;
    return s / 2 ** 32;
  };
}

function generate(count, options = {}) {
  const next = createOrderGenerator({ random: seededRandom(), runId: 'test', ...options });
  return Array.from({ length: count }, () => next());
}

test('同一個 order_id 的 seq 從 1 開始連續遞增、不跳號', () => {
  const byOrder = Map.groupBy(generate(500), (e) => e.order_id);
  for (const [orderId, events] of byOrder) {
    const seqs = events.map((e) => e.seq);
    assert.deepEqual(seqs, seqs.map((_, i) => i + 1), `${orderId} seq 不連續：${seqs}`);
  }
});

test('status 與 seq 一一對應，走完 delivered 就不再出現', () => {
  const byOrder = Map.groupBy(generate(500), (e) => e.order_id);
  for (const events of byOrder.values()) {
    assert.ok(events.length <= STATUSES.length);
    events.forEach((e) => assert.equal(e.status, STATUSES[e.seq - 1]));
  }
});

test('同時進行中的訂單數不超過 activeOrders', () => {
  const events = generate(500, { activeOrders: 3 });
  const open = new Set();
  for (const e of events) {
    open.add(e.order_id);
    if (e.status === 'delivered') open.delete(e.order_id);
    assert.ok(open.size <= 3, `進行中訂單數 ${open.size} > 3`);
  }
});

test('order_id 帶 runId 前綴，producer 重啟後不會與舊 key 撞號', () => {
  const [a] = generate(1, { runId: 'run-a' });
  const [b] = generate(1, { runId: 'run-b' });
  assert.match(a.order_id, /^run-a-\d{4}$/);
  assert.notEqual(a.order_id, b.order_id);
});

test('同一筆訂單的 amount 在各階段保持不變', () => {
  const byOrder = Map.groupBy(generate(500), (e) => e.order_id);
  for (const events of byOrder.values()) {
    assert.equal(new Set(events.map((e) => e.amount)).size, 1);
  }
});
