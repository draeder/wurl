import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parseClock } from '../src/chess.js';

test('--clock: minutes+increment, or none', () => {
  assert.deepEqual(parseClock('3+2'), { minutes: 3, increment: 2 });
  assert.deepEqual(parseClock('10'), { minutes: 10, increment: 0 });
  assert.deepEqual(parseClock('0'), { minutes: 0, increment: 0 });
  assert.throws(() => parseClock('fast'));
});
