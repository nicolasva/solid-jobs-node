'use strict';

const keyspace = require('./keyspace');
const { emit } = require('./instrumentation');
const { dumpJson, realtimeMilliseconds } = require('./utilities');

function resolveTask(payload) {
  try {
    return require('./task').resolveTask(payload.task);
  } catch {
    return null;
  }
}

const errorClass = (error) => (error && error.constructor && error.constructor.name) || 'Error';
const errorMessage = (error) => (error instanceof Error ? error.message : String(error));

// Decides retry / drop / archive for a failed envelope. Resolves to the next attempt time (seconds) or null.
class FailurePolicy {
  constructor({ config, payload, error, telemetryAttributes = {} }) {
    this.config = config;
    this.payload = payload;
    this.error = error;
    this.telemetry = telemetryAttributes;
  }

  static call(options) {
    return new FailurePolicy(options).run();
  }

  async run() {
    const payload = { ...this.payload };
    const now = Date.now() / 1000;
    const nowMs = realtimeMilliseconds();
    const failureCount = Number(payload.failure_count || 0) + 1;
    payload.exception_message = errorMessage(this.error);
    payload.exception_type = errorClass(this.error);
    payload.first_failed_ms ??= nowMs;
    payload.last_failed_ms = nowMs;
    payload.failure_count = failureCount;
    if (payload.retry_channel) payload.channel = payload.retry_channel;

    const pool = await this.config.connect();
    if (this.retryAllowed(payload, failureCount, now)) {
      const decision = await this.retryDecision(payload, failureCount);
      if (decision === 'drop') {
        this.emitDead(payload);
        return null;
      }
      if (decision === 'archive') return this.archive(payload, now);
      const nextAttempt = now + (decision !== null ? decision : this.retryDelay(failureCount));
      await pool.call('ZADD', keyspace.RETRIES, nextAttempt, dumpJson(payload));
      emit(this.config, 'retry_scheduled', payload, {
        ...this.telemetry, error_class: errorClass(this.error), error_message: errorMessage(this.error),
      });
      this.config.logger.info(`retry scheduled for ${payload.id}`);
      return nextAttempt;
    }

    await this.invokeFinalFailure(payload);
    await this.archive(payload, now);
    this.config.logger.info(`task discarded: ${payload.id}`);
    return null;
  }

  retryAllowed(payload, failureCount, now) {
    if (payload.retry_within !== undefined && payload.retry_within !== null) {
      return now - Number(payload.first_failed_ms) / 1000 < Number(payload.retry_within);
    }
    return failureCount <= Number(payload.max_failures ?? 0);
  }

  retryDelay(failureCount) {
    const ceiling = Math.min(Number(this.config.retryBaseDelay) * 2 ** (failureCount - 1), Number(this.config.retryMaxDelay));
    return ceiling / 2 + (Math.random() * ceiling) / 2;
  }

  async retryDecision(payload, failureCount) {
    const task = resolveTask(payload);
    if (!task) return null;
    const value = await task.retryDelay(failureCount, this.error, payload);
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    return value === 'drop' || value === 'archive' ? value : null;
  }

  async invokeFinalFailure(payload) {
    const task = resolveTask(payload);
    if (task) await task.afterFinalFailure(payload, this.error);
  }

  async archive(payload, timestamp) {
    const pool = await this.config.connect();
    const replies = await pool.pipelined((pipeline) => {
      pipeline.call('ZADD', keyspace.DISCARDED, timestamp, dumpJson(payload));
      pipeline.call('ZREMRANGEBYSCORE', keyspace.DISCARDED, '-inf', timestamp - this.config.discardedRetention);
      pipeline.call('ZREMRANGEBYRANK', keyspace.DISCARDED, 0, -(this.config.discardedLimit + 1));
    });
    this.emitDead(payload);
    return replies;
  }

  emitDead(payload) {
    emit(this.config, 'dead', payload, {
      ...this.telemetry, error_class: errorClass(this.error), error_message: errorMessage(this.error),
    });
  }
}

module.exports = { FailurePolicy };
