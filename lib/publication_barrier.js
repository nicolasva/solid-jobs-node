'use strict';

const keyspace = require('./keyspace');
const { randomHex } = require('./utilities');

const TIMEOUT_MS = 30_000;
const FIELD = '_solid_jobs_publication';
const RELEASE = `
local released = 0
for index, key in ipairs(KEYS) do
  if redis.call("get", key) == ARGV[index] then
    released = released + redis.call("del", key)
  end
end
return released
`;

function prepare({ publication, owner } = {}) {
  const id = publication || randomHex(16);
  return { key: keyspace.publication(id), publication: id, owner: owner || randomHex(16) };
}

function mark(pipeline, entry) {
  pipeline.call('SET', entry.key, entry.owner, 'PX', TIMEOUT_MS);
}

async function release(pool, entries) {
  const list = [...entries];
  if (list.length === 0) return 0;
  return pool.call('EVAL', RELEASE, list.length, ...list.map((e) => e.key), ...list.map((e) => e.owner));
}

function strip(envelope) {
  delete envelope[FIELD];
  return envelope;
}

module.exports = { TIMEOUT_MS, FIELD, prepare, mark, release, strip };
