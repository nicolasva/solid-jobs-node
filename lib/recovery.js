'use strict';

const os = require('node:os');
const keyspace = require('./keyspace');
const publicationBarrier = require('./publication_barrier');
const { RedisError } = require('./errors');
const { emit } = require('./instrumentation');
const { randomHex, scan } = require('./utilities');

const CLAIMED_PATTERN = `${keyspace.PREFIX}:node:*:claimed:*`;
const HEARTBEAT_TTL = 60;
const RESTORE_ONE = `
local payload = redis.call("rpop", KEYS[1])
if payload then
  local envelope = cjson.decode(payload)
  local publication = envelope["${publicationBarrier.FIELD}"]
  if not publication then
    publication = ARGV[3]
    envelope["${publicationBarrier.FIELD}"] = publication
    payload = cjson.encode(envelope)
  end
  redis.call("set", ARGV[1] .. publication, ARGV[4], "PX", ARGV[2])
  redis.call("rpush", "${keyspace.PREFIX}:channel:" .. envelope["channel"], payload)
  return {payload, publication, ARGV[4]}
end
`;

function processAlive(pid) {
  try {
    process.kill(pid, 0);
  } catch (error) {
    return error.code !== 'ESRCH';
  }
  return true;
}

async function alive(pool, identity) {
  const [hostname, ...rest] = identity.split(':');
  const pid = rest[0] || '';
  if (hostname === os.hostname() && /^\d+$/.test(pid) && !processAlive(Number(pid))) return false;
  const beat = await pool.call('HGET', keyspace.node(identity), 'beat');
  return Boolean(beat) && Date.now() / 1000 - Number(beat) < HEARTBEAT_TTL;
}

async function releasePublication(config, pool, publication, owner) {
  const entry = publicationBarrier.prepare({ publication, owner });
  try {
    await publicationBarrier.release(pool, [entry]);
  } catch (error) {
    if (!(error instanceof RedisError)) throw error;
    config.logger.warn(`SolidJobs recovery barrier release failed; waiting for expiry: ${error.name}: ${error.message}`);
  }
}

// Requeues claims of dead nodes into their channels. `deadIdentities` forces specific nodes (shutdown fencing).
async function call(config, deadIdentities = []) {
  const pool = await config.connect();
  let recovered = 0;
  for (const key of await scan(pool, CLAIMED_PATTERN)) {
    const nodeKey = key.split(':claimed:')[0];
    const identity = nodeKey.slice(`${keyspace.PREFIX}:node:`.length);
    if (!deadIdentities.includes(identity) && (await alive(pool, identity))) continue;
    for (;;) {
      const restored = await pool.call('EVAL', RESTORE_ONE, 1, key, keyspace.PUBLICATION_PREFIX,
        publicationBarrier.TIMEOUT_MS, randomHex(16), randomHex(16));
      if (!restored) break;
      const [payload, publication, owner] = restored;
      recovered += 1;
      emit(config, 'recovered', payload);
      await releasePublication(config, pool, publication, owner);
    }
    await pool.call('DEL', key);
    await pool.call('DEL', keyspace.claims(identity));
  }
  return recovered;
}

module.exports = { call, alive, RESTORE_ONE, HEARTBEAT_TTL };
