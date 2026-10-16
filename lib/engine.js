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
    this.executor = new Executor(config);
    this.identity = identity;
    this.processorId = processorId;
    this.report = report;
    this.quiet = false;
    this.stopping = false;
  }

  async run(control) {
    let processed = 0;
    let failed = 0;
    try {
      for (;;) {
        this.receiveCommands(control);
        if (this.stopping) break;
        if (this.quiet) {
          await this.flushAck();
          await sleep(50);
          continue;
        }
        const claim = await this.retrieve();
        if (!claim) continue;
        this.receiveCommands(control);
        if (this.stopping || this.quiet) {
          await this.requeue(claim);
          if (this.stopping) break;
          continue;
        }
        const [ok, bad] = await this.process(claim);
        processed += ok;
        failed += bad;
      }
    } finally {
      await this.flushAck();
      this.send({ type: 'stats', processed, failed });
      this.config.close();
    }
    return { processed, failed };
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
      await this.claims.deferAck(claim, (acknowledged) => this.acknowledged(claim, acknowledged));
    } catch (error) {
      if (!(error instanceof RedisConnectionError)) throw error;
      this.completionFailed(error);
      return false;
    }
    return true;
  }

  async flushAck() {
    try {
      await this.claims.flush();
    } catch (error) {
      if (!(error instanceof RedisConnectionError)) throw error;
      this.completionFailed(error);
    }
  }

  completionFailed(error) {
    this.claims.connectionFailed();
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

  async retrieve() {
    try {
      return await this.claims.next();
    } catch (error) {
      if (!(error instanceof RedisConnectionError)) throw error;
      this.claims.connectionFailed();
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
    this.send({ type: 'work', processorId: this.processorId, work, jobId: envelope.id });
  }

  clearWork() {
    if (this.identity !== null) this.send({ type: 'done', processorId: this.processorId });
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
