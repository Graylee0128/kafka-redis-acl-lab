import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadConfig } from '../src/config.js';

const base = {
  KAFKA_BROKERS: 'kafka:9092',
  KAFKA_USERNAME: 'producer',
  KAFKA_PASSWORD: 'secret',
};

test('只給必填欄位時套用預設值', () => {
  const config = loadConfig(base);
  assert.deepEqual(config.brokers, ['kafka:9092']);
  assert.equal(config.topic, 'orders');
  assert.equal(config.intervalMs, 1000);
  assert.equal(config.activeOrders, 5);
});

test('KAFKA_BROKERS 可用逗號分隔多個 broker', () => {
  const config = loadConfig({ ...base, KAFKA_BROKERS: 'a:9092, b:9092' });
  assert.deepEqual(config.brokers, ['a:9092', 'b:9092']);
});

test('缺少必填欄位時列出所有缺漏，且不洩漏密碼值', () => {
  assert.throws(
    () => loadConfig({ KAFKA_PASSWORD: 'super-secret' }),
    (err) => err.message.includes('KAFKA_BROKERS')
      && err.message.includes('KAFKA_USERNAME')
      && !err.message.includes('super-secret'),
  );
});

test('數字欄位不是正整數時拒絕', () => {
  assert.throws(() => loadConfig({ ...base, PRODUCE_INTERVAL_MS: 'fast' }), /PRODUCE_INTERVAL_MS/);
  assert.throws(() => loadConfig({ ...base, ACTIVE_ORDERS: '0' }), /ACTIVE_ORDERS/);
});
