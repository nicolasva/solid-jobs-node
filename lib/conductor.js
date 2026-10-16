'use strict';

const path = require('node:path');
const { Worker } = require('node:worker_threads');
const keyspace = require('./keyspace');
const settings = require('./settings');
const { Blueprint } = require('./blueprint');
const { SolidJobsError, RedisError } = require('./errors');
const { Heartbeat, INTERVAL_MS } = require('./heartbeat');
const recovery = require('./recovery');
const { Timer } = require('./timer');
const { sleep } = require('./utilities');
const { WarmIPCGateway } = require('./warm_ipc');

const STOP_GRACE = 5.0;
const STOP_RESENDS = 3;
const STARTUP_TIMEOUT_MS = 30_000;
const RESTART_BACKOFF_MS = [100, 500, 2000, 5000];
const WORKER_FILE = path.join(__dirname, 'worker.js');

/**
 * Runs N executor worker threads, a scheduler and a heartbeat for one node.
 *
 * Every Redis operation goes through the Ruby solid-redis sidecar over a 0600 Unix socket; each worker thread owns
 * one connection to it. A worker that dies (uncaught exception, process.exit in task code) is respawned with the
 * same processor id, which re-adopts the claim it left behind: the job is replayed, never lost.
 */
class Conductor {
  constructor(config, { stopGrace = STOP_GRACE } = {}) {
    this.config = config || settings.config();
    this.stopGrace = stopGrace;
    this.identity = this.config.identity;
    this.startedAt = Date.now() / 1000;
    this.reset();
  }

  reset() {
    this.started = false;
    this.slots = [];
    this.background = [];
    this.stopBackground = false;
    this.stopHeartbeat = false;
    this.isQuiet = false;
    this.stopping = false;
    this.work = new Map();
    this.processed = 0;
    this.failed = 0;
    this.restarts = 0;
    this.heartbeat = null;
    this.gateway = null;
  }

  running() {
    return this.started;
  }

  async start() {
    if (this.started) throw new SolidJobsError('Conductor is already running');
    const { config } = this;
    try {
      const pool = await config.connect();
      if (config.warmIpc) this.gateway = new WarmIPCGateway(pool);
      const snapshot = config.snapshot();
      await recovery.call(config);
      await this.startHeartbeat(snapshot);
      const ready = [];
      for (let processorId = 0; processorId < config.concurrency; processorId += 1) ready.push(this.startWorker(snapshot, processorId));
      this.startScheduler(snapshot);
      await this.awaitReady(ready);
    } catch (error) {
      await this.abort();
      throw error;
    }
    this.started = true;
    await config.fire('startup', this);
    return this;
  }

  async quiet() {
    if (!this.started) return this;
    for (const slot of this.slots) slot.worker?.postMessage('quiet');
    this.isQuiet = true;
    await this.config.fire('quiet', this);
    return this;
  }

  async stop() {
    if (!this.started) return [];
    this.stopping = true;
    for (const slot of this.slots) slot.worker?.postMessage('stop');
    this.stopBackground = true;
    const deadlineMs = (this.config.shutdownTimeout + this.stopGrace) * 1000;
    const results = await Promise.all(this.slots.map((slot) => this.awaitWorker(slot, deadlineMs)));
    this.stopHeartbeat = true;
    await Promise.race([Promise.all(this.background), sleep(deadlineMs)]);
    await this.teardown();
    await this.config.fire('shutdown', this);
    return results;
  }

  async remoteSignal() {
    const pool = await this.config.connect();
    return pool.call('RPOP', keyspace.nodeSignals(this.identity));
  }

  stats() {
    return { processed: this.processed, failed: this.failed, restarts: this.restarts, busy: this.work.size };
  }

  // Resolves once the worker has reported ready (or rejects when it fails to boot).
  startWorker(snapshot, processorId, slot = null) {
    const target = slot || { processorId, worker: null, exited: null, outcome: null, restartCount: 0 };
    if (!slot) this.slots.push(target);
    const worker = new Worker(WORKER_FILE, { workerData: { snapshot, processorId }, stdout: false, stderr: false });
    target.worker = worker;
    if (this.gateway) this.gateway.attach(worker);
    target.outcome = null;
    target.exited = new Promise((resolve) => {
      worker.once('exit', (code) => resolve(code));
    });

    const booted = new Promise((resolve, reject) => {
      worker.on('message', (message) => {
        if (message.type === 'ready') resolve();
        else if (message.type === 'failed') reject(new SolidJobsError(`SolidJobs worker ${processorId} failed to boot: ${message.detail}`));
        else this.handleMessage(message);
      });
      worker.once('error', (error) => {
        this.config.logger.error(`SolidJobs worker ${processorId} crashed: ${error && error.stack ? error.stack : error}`);
        target.outcome = { status: 'crashed', error };
        reject(error);
      });
    });
    booted.catch(() => {});

    worker.once('exit', () => {
      this.work.delete(String(processorId));
      if (target.worker !== worker) return;
      const unexpected = !this.stopping && this.started;
      if (!target.outcome) target.outcome = { status: unexpected ? 'crashed' : 'stopped' };
      if (unexpected) this.scheduleRestart(snapshot, target);
    });
    return booted;
  }

  scheduleRestart(snapshot, slot) {
    const delay = RESTART_BACKOFF_MS[Math.min(slot.restartCount, RESTART_BACKOFF_MS.length - 1)];
    slot.restartCount += 1;
    this.restarts += 1;
    this.config.logger.warn(`SolidJobs restarting worker ${slot.processorId} in ${delay}ms`);
    setTimeout(() => {
      if (this.stopping || !this.started) return;
      const booted = this.startWorker(snapshot, slot.processorId, slot);
      booted.then(() => {
        slot.restartCount = 0;
        if (this.isQuiet) slot.worker?.postMessage('quiet');
      }, () => {});
    }, delay).unref();
  }

  handleMessage(message) {
    if (message.type === 'work') this.work.set(String(message.processorId), message.work);
    else if (message.type === 'done') this.work.delete(String(message.processorId));
    else if (message.type === 'stats') {
      this.processed += message.processed;
      this.failed += message.failed;
    }
  }

  async startHeartbeat(snapshot) {
    const local = this.localBlueprint(snapshot);
    const heartbeat = new Heartbeat(local, this.identity, this.startedAt, this.config.concurrency);
    // Publish liveness before workers/scheduler start, so recovery never treats this node as dead.
    await heartbeat.beat({ quiet: this.isQuiet });
    const beat = async () => {
      const work = Object.fromEntries(this.work);
      const { processed, failed } = this;
      this.processed = 0;
      this.failed = 0;
      try {
        await heartbeat.beat({ quiet: this.isQuiet, work, processed, failed });
      } catch (error) {
        this.processed += processed;
        this.failed += failed;
        if (!(error instanceof RedisError)) throw error;
        local.logger.warn(`Redis heartbeat failed, retrying: ${error.name}: ${error.message}`);
      }
    };
    this.background.push((async () => {
      try {
        let nextBeat = performance.now() + INTERVAL_MS;
        while (!this.stopHeartbeat) {
          await sleep(50);
          if (performance.now() >= nextBeat) {
            await beat();
            nextBeat = performance.now() + INTERVAL_MS;
          }
        }
        try {
          await beat();
          await heartbeat.cleanup();
        } catch (error) {
          if (!(error instanceof RedisError)) throw error;
          local.logger.warn(`Redis heartbeat cleanup failed: ${error.name}: ${error.message}`);
        }
      } finally {
        local.close();
      }
    })().catch((error) => this.config.logger.error(`heartbeat stopped: ${error && error.stack}`)));
  }

  // In warm IPC mode every component reuses the single sidecar connection.
  localBlueprint(snapshot) {
    const local = Blueprint.fromSnapshot(snapshot);
    if (this.gateway) {
      local.pool = this.config.pool;
      local.sharedPool = true;
    }
    return local;
  }

  startScheduler(snapshot) {
    const timer = new Timer(this.localBlueprint(snapshot));
    this.background.push(timer.run(() => this.stopBackground)
      .catch((error) => this.config.logger.error(`scheduler stopped: ${error && error.stack}`)));
  }

  async awaitReady(ready) {
    let timer;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new SolidJobsError(`SolidJobs workers did not start within ${STARTUP_TIMEOUT_MS / 1000}s`)), STARTUP_TIMEOUT_MS);
    });
    try {
      await Promise.race([Promise.all(ready), timeout]);
    } finally {
      clearTimeout(timer);
    }
  }

  async awaitWorker(slot, deadlineMs) {
    let resends = 0;
    for (;;) {
      const worker = slot.worker;
      const settled = await Promise.race([slot.exited.then(() => true), sleep(deadlineMs).then(() => false)]);
      if (settled || worker !== slot.worker) break;
      if (resends >= STOP_RESENDS) {
        this.config.logger.error(`SolidJobs worker ${slot.processorId} did not stop; abandoning it`);
        await worker.terminate();
        await this.fenceAbandoned();
        return { status: 'abandoned' };
      }
      resends += 1;
      this.config.logger.warn(`SolidJobs worker ${slot.processorId} did not stop within ${deadlineMs / 1000}s, re-sending stop (${resends}/${STOP_RESENDS})`);
      worker.postMessage('stop');
    }
    return { status: slot.outcome?.status || 'stopped' };
  }

  // Requeues claims of workers we could not stop; their late ack is rejected by the claim token.
  async fenceAbandoned() {
    try {
      await recovery.call(this.config, [this.identity]);
    } catch (error) {
      this.config.logger.warn(`Could not requeue abandoned claims: ${error.message}`);
    }
  }

  async teardown() {
    this.config.close();
    this.reset();
  }

  async abort() {
    this.stopping = true;
    this.stopBackground = true;
    this.stopHeartbeat = true;
    await Promise.all(this.slots.map((slot) => slot.worker?.terminate().catch(() => {})));
    await Promise.race([Promise.all(this.background), sleep(2000)]);
    await this.teardown();
  }
}

module.exports = { Conductor, STOP_GRACE };
