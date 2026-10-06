import { test } from 'node:test';
import assert from 'node:assert/strict';
import { validateRequest } from '../src/validate.js';

const good = { username: 'alice', password: 'Abcdef123456', targets: ['kafka', 'redis'] };

test('合法申請通過，targets 去重', () => {
  const result = validateRequest({ ...good, targets: ['redis', 'kafka', 'redis'] });
  assert.equal(result.ok, true);
  assert.deepEqual(result.value, { username: 'alice', password: 'Abcdef123456', targets: ['kafka', 'redis'] });
});

test('body 不是物件時拒絕', () => {
  assert.equal(validateRequest(null).ok, false);
  assert.equal(validateRequest('alice').ok, false);
  assert.equal(validateRequest([]).ok, false);
});

test('帳號格式：小寫英文開頭，3–32 字，只允許 a-z 0-9 _ -', () => {
  for (const username of ['ab', 'Alice', '1alice', 'al ice', 'a:b', 'a'.repeat(33), '', undefined]) {
    assert.equal(validateRequest({ ...good, username }).ok, false, `應拒絕 ${username}`);
  }
  assert.equal(validateRequest({ ...good, username: 'team-a_01' }).ok, true);
});

test('保留帳號（系統內建帳號）不能申請，避免被重設密碼', () => {
  for (const username of ['admin', 'producer', 'consumer', 'app', 'default']) {
    const result = validateRequest({ ...good, username });
    assert.equal(result.ok, false);
    assert.match(result.errors.join(), /保留/);
  }
});

test('密碼：12–64 碼英數字（會嵌進 SCRAM 設定字串，特殊字元會破壞語法）', () => {
  for (const password of ['short1', 'has space 12345', 'abc]def=12345', 'a'.repeat(65), 12345678901234]) {
    assert.equal(validateRequest({ ...good, password }).ok, false);
  }
});

test('targets 必須是 kafka / redis 的非空子集', () => {
  assert.equal(validateRequest({ ...good, targets: [] }).ok, false);
  assert.equal(validateRequest({ ...good, targets: ['mysql'] }).ok, false);
  assert.equal(validateRequest({ ...good, targets: 'kafka' }).ok, false);
});

test('錯誤訊息不包含密碼內容', () => {
  const result = validateRequest({ ...good, username: 'X', password: 'bad]Password!' });
  assert.equal(result.ok, false);
  assert.ok(!result.errors.join().includes('bad]Password!'));
});
