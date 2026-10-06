import { test } from 'node:test';
import assert from 'node:assert/strict';
import { provisionAccess } from '../src/provision.js';

function fakeProvisioner(name, { exists = false, failCreate = false } = {}) {
  const calls = [];
  return {
    calls,
    async userExists(u) { calls.push(['exists', u]); return exists; },
    async createUser(u) {
      calls.push(['create', u]);
      if (failCreate) throw new Error(`${name} down`);
      return { target: name };
    },
    async removeUser(u) { calls.push(['remove', u]); },
  };
}

const request = { username: 'alice', password: 'Abcdef123456', targets: ['kafka', 'redis'] };

test('兩邊都不存在時依序建立，回傳各自的授權內容', async () => {
  const kafka = fakeProvisioner('kafka');
  const redis = fakeProvisioner('redis');

  const result = await provisionAccess(request, { kafka, redis });

  assert.equal(result.status, 'created');
  assert.deepEqual(result.granted, { kafka: { target: 'kafka' }, redis: { target: 'redis' } });
});

test('任一邊帳號已存在就整筆拒絕，什麼都不建立', async () => {
  const kafka = fakeProvisioner('kafka');
  const redis = fakeProvisioner('redis', { exists: true });

  const result = await provisionAccess(request, { kafka, redis });

  assert.deepEqual(result, { status: 'conflict', target: 'redis' });
  assert.ok(!kafka.calls.some(([op]) => op === 'create'));
  assert.ok(!redis.calls.some(([op]) => op === 'create'));
});

test('只選一個 target 時不碰另一邊', async () => {
  const kafka = fakeProvisioner('kafka');
  const redis = fakeProvisioner('redis');

  await provisionAccess({ ...request, targets: ['redis'] }, { kafka, redis });

  assert.equal(kafka.calls.length, 0);
});

test('後一個 target 建立失敗時回滾已建立的，並把錯誤往上丟', async () => {
  const kafka = fakeProvisioner('kafka');
  const redis = fakeProvisioner('redis', { failCreate: true });

  await assert.rejects(provisionAccess(request, { kafka, redis }), /redis down/);
  assert.ok(kafka.calls.some(([op]) => op === 'remove'), 'kafka 帳號應被回滾');
});
