import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRateLimiter } from '../src/rate-limit.js';

test('同一來源在時間窗內超過上限即拒絕，時間窗過後恢復', () => {
  let t = 0;
  const allow = createRateLimiter({ limit: 2, windowMs: 1000, now: () => t });

  assert.equal(allow('1.1.1.1'), true);
  assert.equal(allow('1.1.1.1'), true);
  assert.equal(allow('1.1.1.1'), false);
  assert.equal(allow('2.2.2.2'), true, '不同來源各自計數');

  t = 1000;
  assert.equal(allow('1.1.1.1'), true);
});
