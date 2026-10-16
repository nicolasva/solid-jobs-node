'use strict';

const keyspace = require('./keyspace');
const { RedisConnectionError } = require('./errors');
const { Publisher } = require('./publisher');
const recovery = require('./recovery');
const { runScript, sha1, sleep } = require('./utilities');

const COLLECTIONS = [keyspace.RETRIES, keyspace.PLANNED];
const POP_DUE = `
local jobs = redis.call("zrange", KEYS[1], "-inf", ARGV[1], "byscore", "limit", 0, 1)
if jobs[1] then
  redis.call("zrem", KEYS[1], jobs[1])
  return jobs[1]
end
`;
const POP_DUE_SHA = sha1(POP_DUE);

// Promotes due planned/retrying tasks and recovers claims of dead nodes.
class Timer {
  constructor(config) {
    this.config = config;
    this.publisher = new Publisher(config);
  }

  async enqueueDue(now = Date.now() / 1000) {
    const pool = await this.config.connect();
    let count = 0;
    for (const collection of COLLECTIONS) {
      for (;;) {
        const raw = await runScript(pool, POP_DUE, POP_DUE_SHA, [collection], now);
        if (!raw) break;
        await this.publisher.publish(JSON.parse(raw));
        count += 1;
      }
    }
    return count;
  }

  async run(stop) {
    try {
      while (!stop()) {
        try {
          await recovery.call(this.config);
          await this.enqueueDue();
        } catch (error) {
          if (!(error instanceof RedisConnectionError)) throw error;
          this.config.logger.warn(`Redis scheduler poll failed, retrying: ${error.name}: ${error.message}`);
        }
        await this.interruptibleSleep(this.randomizedInterval(), stop);
      }
    } finally {
      this.config.close();
    }
  }

  randomizedInterval() {
    return Number(this.config.pollIntervalAverage) * (0.5 + Math.random()) * 1000;
  }

  async interruptibleSleep(duration, stop) {
    const deadline = performance.now() + duration;
    while (!stop() && performance.now() < deadline) await sleep(50);
  }
}

module.exports = { Timer, POP_DUE };
