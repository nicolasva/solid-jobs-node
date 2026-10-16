'use strict';

const { ExecutionHalt } = require('./errors');

const EVENTS = {
  enqueued: 'job.enqueued',
  reserved: 'job.reserved',
  journaled: 'job.journaled',
  started: 'job.started',
  completed: 'job.completed',
  failed: 'job.failed',
  retry_scheduled: 'job.retry_scheduled',
  acknowledged: 'job.acknowledged',
  recovered: 'job.recovered',
  dead: 'job.dead',
};

class NullInstrumenter {
  instrument() {
    return null;
  }
}

// Publishes a lifecycle event to `config.instrumenter`; telemetry failures never break job processing.
function emit(config, event, envelope, attributes = {}) {
  try {
    const job = typeof envelope === 'string' ? JSON.parse(envelope) : envelope;
    const payload = {
      job_id: job.id,
      queue: job.channel,
      job_class: job.task,
      node_id: config.identity,
      ...attributes,
    };
    const clean = Object.fromEntries(Object.entries(payload).filter(([, value]) => value !== null && value !== undefined));
    const result = config.instrumenter.instrument(EVENTS[event], clean);
    if (result && typeof result.catch === 'function') {
      result.catch((error) => config.logger.warn(`SolidJobs telemetry failed: ${error.name}: ${error.message}`));
    }
    return true;
  } catch (error) {
    if (error instanceof ExecutionHalt) throw error;
    config.logger.warn(`SolidJobs telemetry failed: ${error.name}: ${error.message}`);
    return false;
  }
}

const workerAttributes = (processorId) => (processorId === null || processorId === undefined
  ? {} : { worker_id: processorId, thread_id: processorId });

module.exports = { EVENTS, NullInstrumenter, emit, workerAttributes };
