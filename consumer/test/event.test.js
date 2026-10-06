import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parseEvent, classifySeq } from '../src/event.js';

const msg = (key, value) => ({
  key: key === null ? null : Buffer.from(key),
  value: value === null ? null : Buffer.from(typeof value === 'string' ? value : JSON.stringify(value)),
});

const valid = { order_id: 'run1-0001', seq: 2, status: 'paid', amount: 12.5, ts: '2026-10-04T00:00:00Z' };

test('合法訊息解析成 event', () => {
  const result = parseEvent(msg('run1-0001', valid));
  assert.equal(result.ok, true);
  assert.deepEqual(result.event, valid);
});

test('沒有 key 的訊息被拒絕', () => {
  const result = parseEvent(msg(null, valid));
  assert.equal(result.ok, false);
  assert.match(result.reason, /key/i);
});

test('value 不是 JSON 被拒絕', () => {
  const result = parseEvent(msg('run1-0001', 'not-json{'));
  assert.equal(result.ok, false);
  assert.match(result.reason, /JSON/);
});

test('缺 seq 或 seq 不是正整數被拒絕（例如步驟 1 手動測試送的 {"seq":"x"}）', () => {
  assert.equal(parseEvent(msg('run1-0001', { ...valid, seq: undefined })).ok, false);
  assert.equal(parseEvent(msg('run1-0001', { ...valid, seq: 0 })).ok, false);
  assert.equal(parseEvent(msg('run1-0001', { ...valid, seq: '2' })).ok, false);
});

test('key 與 value 裡的 order_id 不一致被拒絕', () => {
  const result = parseEvent(msg('run1-9999', valid));
  assert.equal(result.ok, false);
  assert.match(result.reason, /order_id/);
});

test('order_id 含非法字元被拒絕（避免拼出意外的 Redis key）', () => {
  const bad = { ...valid, order_id: 'a:b*c' };
  assert.equal(parseEvent(msg('a:b*c', bad)).ok, false);
});

test('classifySeq：依上一筆 seq 判斷順序狀態', () => {
  assert.equal(classifySeq(null, 1), 'in_order');
  assert.equal(classifySeq(1, 2), 'in_order');
  assert.equal(classifySeq(2, 2), 'duplicate');
  assert.equal(classifySeq(3, 2), 'duplicate');
  assert.equal(classifySeq(1, 3), 'gap');
  assert.equal(classifySeq(null, 2), 'gap');
});
