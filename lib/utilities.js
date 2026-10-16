'use strict';

const crypto = require('node:crypto');
const { RedisCommandError } = require('./errors');

function isPlainObject(value) {
  if (value === null || typeof value !== 'object') return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

const snakeCase = (name) => name.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);

// Deeply converts plain-object keys to strings; envelopes keep Ruby/Python snake_case option names.
function stringifyKeys(value) {
  if (Array.isArray(value)) return value.map(stringifyKeys);
  if (isPlainObject(value)) {
    return Object.fromEntries(Object.entries(value).map(([key, nested]) => [String(key), stringifyKeys(nested)]));
  }
  return value;
}

const dumpJson = (value) => JSON.stringify(value);
const realtimeMilliseconds = () => Date.now();
// randomUUID draws from a pre-filled entropy cache, which is much cheaper than one randomBytes call per claim.
const randomHex = (bytes = 16) => (bytes === 16
  ? crypto.randomUUID().replaceAll('-', '')
  : crypto.randomBytes(bytes).toString('hex'));
const sha1 = (script) => crypto.createHash('sha1').update(script, 'utf8').digest('hex');
const nowSeconds = () => Date.now() / 1000;
const sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

// EVALSHA with an EVAL fallback when the server does not know the script yet.
async function runScript(pool, script, sha, keys, ...args) {
  try {
    return await pool.call('EVALSHA', sha, keys.length, ...keys, ...args);
  } catch (error) {
    if (!(error instanceof RedisCommandError) || !String(error.message).includes('NOSCRIPT')) throw error;
    return pool.call('EVAL', script, keys.length, ...keys, ...args);
  }
}

async function scan(pool, pattern) {
  let cursor = '0';
  const found = [];
  do {
    let batch;
    [cursor, batch] = await pool.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', 100);
    found.push(...batch);
  } while (cursor !== '0');
  return found;
}

function pairs(flat) {
  const list = flat || [];
  const result = [];
  for (let index = 0; index + 1 < list.length; index += 2) result.push([list[index], list[index + 1]]);
  return result;
}

module.exports = {
  isPlainObject,
  snakeCase,
  stringifyKeys,
  dumpJson,
  realtimeMilliseconds,
  randomHex,
  sha1,
  nowSeconds,
  sleep,
  runScript,
  scan,
  pairs,
};
