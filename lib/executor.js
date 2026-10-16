'use strict';

const settings = require('./settings');
const { InvalidJobError } = require('./errors');
const { resolveTask } = require('./task');

class Executor {
  constructor(config) {
    this.config = config || settings.config();
  }

  async execute(envelope) {
    const job = typeof envelope === 'string' ? JSON.parse(envelope) : envelope;
    if (!job || typeof job !== 'object' || Array.isArray(job)) throw new InvalidJobError('Task envelope must be an object');
    if (typeof job.task !== 'string' || !job.task) throw new InvalidJobError('Task must be present');
    if (!Array.isArray(job.arguments)) throw new InvalidJobError('Task arguments must be an array');
    if (typeof job.channel !== 'string' || !job.channel) throw new InvalidJobError('Task channel must be present');

    const Klass = resolveTask(job.task);
    const task = new Klass();
    task.taskId = job.id;
    const run = () => task.executeTask(...job.arguments);
    const interceptors = this.config.executeInterceptors;
    if (interceptors.empty()) return run();
    return interceptors.call({ task, envelope: job }, run);
  }
}

module.exports = { Executor };
