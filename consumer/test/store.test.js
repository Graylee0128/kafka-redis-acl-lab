import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createEventStore } from '../src/store.js';

// 只實作 store 用到的 node-redis 指令
function fakeRedis() {
  const strings = new Map();
  const lists = new Map();
  const ttls = new Map();
  return {
    strings, lists, ttls,
    async get(key) { return strings.get(key) ?? null; },
    async set(key, value, options) { strings.set(key, value); if (options?.EX) ttls.set(key, options.EX); return 'OK'; },
    async rPush(key, value) { const l = lists.get(key) ?? []; lists.set(key, [...l, value]); return l.length + 1; },
    async expire(key, seconds) { ttls.set(key, seconds); return 1; },
  };
}

const event = (seq, status = 'paid') => ({ order_id: 'run1-0001', seq, status, amount: 1, ts: 't' });
const meta = { consumerId: 'c1', partition: 2, offset: '10' };

test('第一筆寫入 latest 與 history，key 帶 myapp 前綴', async () => {
  const redis = fakeRedis();
  const store = createEventStore(redis, { prefix: 'myapp', ttlSeconds: 60 });

  const outcome = await store.apply(event(1, 'created'), meta);

  assert.equal(outcome, 'in_order');
  const latest = JSON.parse(redis.strings.get('myapp:events:run1-0001'));
  assert.equal(latest.seq, 1);
  assert.equal(latest.handled_by, 'c1');
  assert.equal(latest.partition, 2);
  assert.equal(redis.lists.get('myapp:history:run1-0001').length, 1);
  assert.equal(redis.ttls.get('myapp:events:run1-0001'), 60);
  assert.equal(redis.ttls.get('myapp:history:run1-0001'), 60);
});

test('重複投遞（seq 不大於上一筆）不覆蓋、不追加 history', async () => {
  const redis = fakeRedis();
  const store = createEventStore(redis, { prefix: 'myapp', ttlSeconds: 60 });
  await store.apply(event(1), meta);
  await store.apply(event(2), meta);

  const outcome = await store.apply(event(1), meta);

  assert.equal(outcome, 'duplicate');
  assert.equal(JSON.parse(redis.strings.get('myapp:events:run1-0001')).seq, 2);
  assert.equal(redis.lists.get('myapp:history:run1-0001').length, 2);
});

test('跳號仍寫入，但回報 gap', async () => {
  const redis = fakeRedis();
  const store = createEventStore(redis, { prefix: 'myapp', ttlSeconds: 60 });
  await store.apply(event(1), meta);

  const outcome = await store.apply(event(3), meta);

  assert.equal(outcome, 'gap');
  assert.equal(JSON.parse(redis.strings.get('myapp:events:run1-0001')).seq, 3);
});

test('不修改傳入的 event 物件', async () => {
  const store = createEventStore(fakeRedis(), { prefix: 'myapp', ttlSeconds: 60 });
  const input = Object.freeze(event(1));
  await assert.doesNotReject(store.apply(input, meta));
});
