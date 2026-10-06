import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createKafkaProvisioner, redact } from '../src/kafka-admin.js';
import { createRedisProvisioner } from '../src/redis-admin.js';

// ---------- Kafka ----------

function fakeKafka({ describeOutput = '' } = {}) {
  const configCalls = [];
  const acls = { created: [], deleted: [] };
  return {
    configCalls, acls,
    runConfigs: async (args) => { configCalls.push(args); return args[0] === '--describe' ? describeOutput : ''; },
    admin: {
      createAcls: async ({ acl }) => { acls.created.push(...acl); },
      deleteAcls: async ({ filters }) => { acls.deleted.push(...filters); },
    },
  };
}

test('kafka userExists 依 kafka-configs --describe 的輸出判斷', async () => {
  const missing = fakeKafka();
  assert.equal(await createKafkaProvisioner({ ...missing, topic: 'orders' }).userExists('alice'), false);

  const existing = fakeKafka({ describeOutput: "SCRAM credential configs for user-principal 'alice' are SCRAM-SHA-512=iterations=4096" });
  assert.equal(await createKafkaProvisioner({ ...existing, topic: 'orders' }).userExists('alice'), true);
});

test('kafka createUser：參數陣列（不經 shell）建立 SCRAM，並只授予唯讀 ACL', async () => {
  const fake = fakeKafka();
  const grant = await createKafkaProvisioner({ ...fake, topic: 'orders' }).createUser('alice', 'Abcdef123456');

  assert.deepEqual(fake.configCalls[0], [
    '--alter', '--entity-type', 'users', '--entity-name', 'alice',
    '--add-config', 'SCRAM-SHA-512=[password=Abcdef123456]',
  ]);

  const summary = fake.acls.created.map((a) => `${a.resourceType}:${a.resourceName}:${a.resourcePatternType}:${a.operation}:${a.principal}`);
  // kafkajs 常數：TOPIC=2 GROUP=3；LITERAL=3 PREFIXED=4；READ=3 DESCRIBE=8
  assert.deepEqual(summary.sort(), [
    '2:orders:3:3:User:alice',
    '2:orders:3:8:User:alice',
    '3:alice-:4:3:User:alice',
  ].sort());
  assert.ok(fake.acls.created.every((a) => a.permissionType === 3), '全部是 ALLOW');
  assert.equal(grant.groupPrefix, 'alice-');
  assert.ok(!JSON.stringify(grant).includes('Abcdef123456'), '回傳內容不含密碼');
});

test('kafka removeUser 刪除 ACL 與 SCRAM 憑證', async () => {
  const fake = fakeKafka();
  await createKafkaProvisioner({ ...fake, topic: 'orders' }).removeUser('alice');

  assert.equal(fake.acls.deleted.length, 3);
  assert.deepEqual(fake.configCalls.at(-1), ['--alter', '--entity-type', 'users', '--entity-name', 'alice', '--delete-config', 'SCRAM-SHA-512']);
});

test('redact：kafka-configs 失敗時會把參數（含密碼）印進錯誤訊息，必須遮蔽', () => {
  const raw = "Error while executing config command with args '--add-config SCRAM-SHA-512=[password=Abcdef123456]'";
  const cleaned = redact(raw, ['Abcdef123456']);
  assert.ok(!cleaned.includes('Abcdef123456'));
  assert.match(cleaned, /password=\*\*\*/);
});

// ---------- Redis ----------

function fakeRedisAdmin({ existing = [] } = {}) {
  const commands = [];
  return {
    commands,
    sendCommand: async (args) => {
      commands.push(args);
      if (args[0] === 'ACL' && args[1] === 'GETUSER') return existing.includes(args[2]) ? ['flags', ['on']] : null;
      return 'OK';
    },
  };
}

test('redis userExists 依 ACL GETUSER 判斷', async () => {
  const p = createRedisProvisioner(fakeRedisAdmin({ existing: ['bob'] }), { prefix: 'myapp' });
  assert.equal(await p.userExists('bob'), true);
  assert.equal(await p.userExists('alice'), false);
});

test('redis createUser：唯讀、只限 myapp:*，並 ACL SAVE 持久化', async () => {
  const client = fakeRedisAdmin();
  const grant = await createRedisProvisioner(client, { prefix: 'myapp' }).createUser('alice', 'Abcdef123456');

  const [setuser, save] = client.commands;
  assert.deepEqual(setuser.slice(0, 3), ['ACL', 'SETUSER', 'alice']);
  const rules = setuser.slice(3);
  assert.equal(rules[0], 'reset', '先 reset，避免殘留舊權限');
  assert.ok(rules.includes('~myapp:*'));
  assert.ok(rules.includes('-@all'));
  assert.ok(rules.includes('>Abcdef123456'));
  for (const writeCommand of ['+set', '+del', '+@write', '+@all', '~*', '+eval', '+keys', '+scan']) {
    assert.ok(!rules.includes(writeCommand), `不應授予 ${writeCommand}`);
  }
  assert.deepEqual(save, ['ACL', 'SAVE']);
  assert.ok(!JSON.stringify(grant).includes('Abcdef123456'));
});

test('redis removeUser 刪除帳號並持久化', async () => {
  const client = fakeRedisAdmin();
  await createRedisProvisioner(client, { prefix: 'myapp' }).removeUser('alice');
  assert.deepEqual(client.commands, [['ACL', 'DELUSER', 'alice'], ['ACL', 'SAVE']]);
});
