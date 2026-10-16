'use strict';

const Claim = require('./claim');
const { RedisConnectionError } = require('./errors');
const { Executor } = require('./executor');
const { FailurePolicy } = require('./failure_policy');
const { emit, workerAttributes } = require('./instrumentation');
const { sleep } = require('./utilities');

// One executor loop: claim, execute, acknowledge. Runs inside a worker thread (or the main thread in tests).
class Engine {
  /**
   * @param control  `{ drain(): string[] }` returning pending commands ("quiet", "stop")
   * @param report   `(message) => void` forwarding work/done/stats to the heartbeat owner
   */
  constructor(config, { identity = null, processorId = null, report = null } = {}) {
    this.config = config;
    this.claims = new Claim.Claim(config, { identity, processorId });
    // Second slot: a full executor identity of its own (own claimed list, claim entry and fencing tokens), so
    // recovery and fencing are unchanged. At most one extra job is held, and only with reliable fetch.
    this.claimsAlt = config.prefetch && this.claims.claimedKey
      ? new Claim.Claim(config, { identity, processorId: `${processorId}b` }) : null;
    this.executor = new Executor(config);
    this.identity = identity;
    this.processorId = processorId;
    this.report = report;
    this.quiet = false;
    this.stopping = false;
    this.doneDeferred = false;
  }

  async run(control) {
    let processed = 0;
    let failed = 0;
    let slot = this.claims;
    let ahead = null;
    try {
      for (;;) {
        this.receiveCommands(control);
        if (this.stopping) break;
        if (this.quiet) {
          await this.flushAck();
          this.flushDone();
          await sleep(50);
          continue;
        }
        let claim;
        if (ahead) {
          claim = await ahead.promise;
          slot = ahead.slot;
          ahead = null;
        } else {
          claim = await this.retrieve(slot);
        }
        if (!claim) {
          this.flushDone();
          // Idle: settle the other slot's deferred ack and poll it next, so it re-adopts any claim it left behind.
          if (this.claimsAlt) {
            await this.flushAck();
            slot = this.otherSlot(slot);
          }
          continue;
        }
        this.receiveCommands(control);
        if (this.stopping || this.quiet) {
          await this.requeue(claim);
          if (this.stopping) break;
          continue;
        }
        const other = this.otherSlot(slot);
        if (other) {
          ahead = { slot: other, promise: this.retrieve(other) };
          ahead.promise.catch(() => {});
          // A synchronous job would block the thread before the reservation is even written; one loop turn lets it go out.
          await new Promise((resolve) => setImmediate(resolve));
        }
        const [ok, bad] = await this.process(claim);
        processed += ok;
        failed += bad;
        if (other) slot = other;
      }
    } finally {
      if (ahead) {
        const leftover = await ahead.promise.catch(() => null);
        if (leftover) await this.requeue(leftover);
      }
      await this.flushAck();
      this.flushDone();
      this.send({ type: 'stats', processed, failed });
      this.config.close();
    }
    return { processed, failed };
  }

  otherSlot(slot) {
    if (!this.claimsAlt) return null;
    return slot === this.claims ? this.claimsAlt : this.claims;
  }

  // Resolves to [processed, failed] deltas.
  async process(claim) {
    const { envelope } = claim;
    this.registerWork(claim, envelope);
    const attributes = this.telemetryAttributes(claim);
    try {
      emit(this.config, 'started', envelope, attributes);
      const started = performance.now();
      try {
        await this.executor.execute(envelope);
      } catch (error) {
        const duration = (performance.now() - started) / 1000;
        emit(this.config, 'failed', envelope, {
          ...attributes, duration, error_class: error?.constructor?.name, error_message: String(error?.message ?? error),
        });
        await FailurePolicy.call({ config: this.config, payload: envelope, error, telemetryAttributes: attributes });
        await this.complete(claim);
        return [0, 1];
      }
      const duration = (performance.now() - started) / 1000;
      emit(this.config, 'completed', envelope, { ...attributes, duration });
      return [(await this.complete(claim)) ? 1 : 0, 0];
    } finally {
      this.clearWork();
    }
  }

  async complete(claim) {
    try {
      await claim.claims.deferAck(claim, (acknowledged) => this.acknowledged(claim, acknowledged));
    } catch (error) {
      if (!(error instanceof RedisConnectionError)) throw error;
      this.completionFailed(error);
      return false;
    }
    return true;
  }

  async flushAck() {
    for (const slot of [this.claims, this.claimsAlt]) {
      if (!slot) continue;
      try {
        await slot.flush();
      } catch (error) {
        if (!(error instanceof RedisConnectionError)) throw error;
        this.completionFailed(error);
      }
    }
  }

  completionFailed(error) {
    this.claims.connectionFailed();
    if (this.claimsAlt) this.claimsAlt.connectionFailed();
    this.config.logger.warn(`Redis completion failed; claimed task will be replayed: ${error.name}: ${error.message}`);
  }

  acknowledged(claim, acknowledged) {
    if (!acknowledged) {
      this.config.logger.warn(`Claim fencing rejected stale completion: ${claim.claimToken}`);
      return;
    }
    emit(this.config, 'acknowledged', claim.envelope, this.telemetryAttributes(claim));
  }

  async requeue(claim) {
    try {
      if (!(await claim.requeue())) this.config.logger.warn(`Claim fencing rejected stale requeue: ${claim.claimToken}`);
    } catch (error) {
      if (!(error instanceof RedisConnectionError)) throw error;
      this.claims.connectionFailed();
    }
  }

  async retrieve(slot = this.claims) {
    try {
      return await slot.next();
    } catch (error) {
      if (!(error instanceof RedisConnectionError)) throw error;
      slot.connectionFailed();
      this.config.logger.warn(`Redis claim failed, retrying: ${error.name}: ${error.message}`);
      await sleep(100);
      return null;
    }
  }

  registerWork(claim, envelope) {
    if (this.identity === null) return;
    const work = JSON.stringify({
      job_id: envelope.id, channel: claim.channel, envelope, started_at: Date.now() / 1000,
      claim_token: claim.claimToken, attempt: claim.attempt,
    });
    this.doneDeferred = false;
    this.send({ type: 'work', processorId: this.processorId, work, jobId: envelope.id });
  }

  // The 'done' notice is deferred: when the next job starts right away its 'work' message replaces the entry, which
  // saves one main-thread message per job. It is flushed whenever the worker goes idle.
  clearWork() {
    if (this.identity !== null) this.doneDeferred = true;
  }

  flushDone() {
    if (!this.doneDeferred) return;
    this.doneDeferred = false;
    this.send({ type: 'done', processorId: this.processorId });
  }

  send(message) {
    if (!this.report) return;
    try {
      this.report(message);
    } catch (error) {
      this.config.logger.debug(`heartbeat message dropped: ${error.message}`);
    }
  }

  receiveCommands(control) {
    for (const command of control.drain()) {
      if (command === 'quiet') this.quiet = true;
      else if (command === 'stop') this.stopping = true;
    }
  }

  telemetryAttributes(claim) {
    return { reservation_id: claim.claimToken, attempt: claim.attempt, ...workerAttributes(this.processorId) };
  }
}

module.exports = { Engine };
