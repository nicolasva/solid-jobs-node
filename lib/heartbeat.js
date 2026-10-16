'use strict';

const os = require('node:os');
const keyspace = require('./keyspace');

const INTERVAL_MS = 5000;
const TTL = 60;

// Publishes node liveness, busy executors and processed/failed counters.
class Heartbeat {
  constructor(config, identity, startedAt, concurrency) {
    this.config = config;
    this.identity = identity;
    this.startedAt = startedAt;
    this.concurrency = concurrency;
  }

  async beat({ quiet = false, work = {}, processed = 0, failed = 0 } = {}) {
    const now = Date.now() / 1000;
    const info = {
      hostname: os.hostname(), pid: process.pid, started_at: this.startedAt,
      channels: this.config.channels, labels: [], identity: this.identity,
    };
    const nodeKey = keyspace.node(this.identity);
    const workKey = keyspace.nodeWork(this.identity);
    const pool = await this.config.connect();
    const busy = await pool.call('HLEN', workKey);
    const entries = Object.entries(work);
    await pool.pipelined((pipeline) => {
      if (processed > 0) pipeline.call('INCRBY', keyspace.PROCESSED, processed);
      if (failed > 0) pipeline.call('INCRBY', keyspace.FAILED, failed);
      pipeline.call('SADD', keyspace.NODES, this.identity);
      pipeline.call('HSET', nodeKey, 'info', JSON.stringify(info), 'beat', now, 'busy', busy,
        'quiet', quiet ? 'true' : 'false', 'concurrency', this.concurrency);
      pipeline.call('EXPIRE', nodeKey, TTL);
      pipeline.call('DEL', workKey);
      if (entries.length > 0) pipeline.call('HSET', workKey, ...entries.flat());
      pipeline.call('EXPIRE', workKey, TTL);
    });
    return now;
  }

  async cleanup() {
    try {
      const pool = await this.config.connect();
      await pool.pipelined((pipeline) => {
        pipeline.call('SREM', keyspace.NODES, this.identity);
        pipeline.call('DEL', keyspace.node(this.identity));
        pipeline.call('DEL', keyspace.nodeWork(this.identity));
      });
    } finally {
      this.config.close();
    }
  }
}

module.exports = { Heartbeat, INTERVAL_MS, TTL };
