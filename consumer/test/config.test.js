import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadConfig } from '../src/config.js';

const base = {
  KAFKA_BROKERS: 'kafka:9092',
  KAFKA_USERNAME: 'consumer',
  KAFKA_PASSWORD: 'kafka-secret',
  REDIS_URL: 'redis://redis:6379',
  REDIS_USERNAME: 'app',
  REDIS_PASSWORD: 'redis-secret',
};

test('只給必填欄位時套用預設值', () => {
  const config = loadConfig(base, 'host-abc');
  assert.deepEqual(config.brokers, ['kafka:9092']);
  assert.equal(config.topic, 'orders');
  assert.equal(config.groupId, 'order-consumers');
  assert.equal(config.keyPrefix, 'myapp');
  assert.equal(config.ttlSeconds, 86400);
  assert.equal(config.consumerId, 'host-abc');
});

test('CONSUMER_ID 可覆寫預設的 hostname', () => {
  assert.equal(loadConfig({ ...base, CONSUMER_ID: 'c1' }, 'host-abc').consumerId, 'c1');
});

test('缺少必填欄位時列出所有缺漏，且不洩漏密碼值', () => {
  assert.throws(
    () => loadConfig({ KAFKA_PASSWORD: 'kafka-secret', REDIS_PASSWORD: 'redis-secret' }, 'h'),
    (err) => ['KAFKA_BROKERS', 'KAFKA_USERNAME', 'REDIS_URL', 'REDIS_USERNAME'].every((n) => err.message.includes(n))
      && !err.message.includes('secret'),
  );
});

test('REDIS_TTL_SECONDS 不是正整數時拒絕', () => {
  assert.throws(() => loadConfig({ ...base, REDIS_TTL_SECONDS: '-1' }, 'h'), /REDIS_TTL_SECONDS/);
});
