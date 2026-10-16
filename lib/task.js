'use strict';

const Lab = require('./lab');
const settings = require('./settings');
const { InvalidJobError } = require('./errors');
const { stringifyKeys, snakeCase } = require('./utilities');

const REGISTRY = settings.STATE.tasks;
const OPTION_STATICS = ['channel', 'maxFailures', 'retryWithin', 'retryChannel'];
const DEFAULT_CHUNK_SIZE = 1000;

const taskNameOf = (cls) => (Object.hasOwn(cls, 'taskName') && cls.taskName ? cls.taskName : cls.name);

function register(cls) {
  const name = taskNameOf(cls);
  if (!name) throw new InvalidJobError('Task classes must be named (or define static taskName)');
  const existing = REGISTRY.get(name);
  if (existing && existing !== cls) {
    throw new InvalidJobError(`Two different tasks are registered as ${name}; set a distinct static taskName`);
  }
  REGISTRY.set(name, cls);
  return name;
}

function isTaskClass(value) {
  return typeof value === 'function' && value.prototype instanceof Task; // eslint-disable-line no-use-before-define
}

// Registers every Task subclass exported by a module (worker threads call this after `require`).
function registerExports(moduleExports, seen = new Set(), depth = 0) {
  if (!moduleExports || seen.has(moduleExports) || depth > 2) return [];
  seen.add(moduleExports);
  const found = [];
  if (isTaskClass(moduleExports)) {
    register(moduleExports);
    found.push(moduleExports);
  }
  if (typeof moduleExports === 'object' || typeof moduleExports === 'function') {
    for (const value of Object.values(moduleExports)) found.push(...registerExports(value, seen, depth + 1));
  }
  return found;
}

function resolveTask(name) {
  const cls = REGISTRY.get(name);
  if (!cls) throw new InvalidJobError(`Unknown task ${JSON.stringify(name)}: it must be exported by a --require'd file`);
  return cls;
}

/**
 * Base class for SolidJobs tasks.
 *
 *   class RecalculateAccount extends Task {
 *     static channel = 'critical';
 *     static maxFailures = 10;
 *     async executeTask(accountId) { ... }
 *   }
 *   await RecalculateAccount.enqueue(42);
 *
 * Tasks are looked up by `static taskName` (default: the class name) in the files given with `--require`.
 */
class Task {
  constructor() {
    this.taskId = null;
  }

  async executeTask() {
    throw new Error(`${this.constructor.name} must implement executeTask`);
  }

  get logger() {
    return settings.config().logger;
  }

  // Seconds to wait, 'drop', 'archive' or null/undefined for the default exponential backoff.
  static async retryDelay() {
    return null;
  }

  static async afterFinalFailure() {
    return null;
  }

  static currentTaskOptions() {
    const merged = { ...settings.config().defaultTaskOptions };
    for (const name of OPTION_STATICS) {
      if (this[name] !== undefined && this[name] !== null) merged[snakeCase(name)] = this[name];
    }
    const chain = [];
    for (let klass = this; klass && klass !== Function.prototype; klass = Object.getPrototypeOf(klass)) chain.unshift(klass);
    for (const klass of chain) {
      if (Object.hasOwn(klass, 'taskOptions')) Object.assign(merged, stringifyKeys(klass.taskOptions));
    }
    return merged;
  }

  static async enqueue(...args) {
    return this.publish({ task: this, arguments: args });
  }

  static async enqueueMany(argumentSets, { chunkSize = DEFAULT_CHUNK_SIZE } = {}) {
    register(this);
    const { Publisher } = require('./publisher');
    return new Publisher().publishMany({ ...this.currentTaskOptions(), task: this, arguments: argumentSets, chunk_size: chunkSize });
  }

  static async enqueueAfter(delaySeconds, ...args) {
    return this.scheduleAt(Date.now() / 1000 + Number(delaySeconds), ...args);
  }

  static async scheduleAt(when, ...args) {
    const timestamp = when instanceof Date ? when.getTime() / 1000 : Number(when);
    return this.publish({ task: this, arguments: args, run_at: timestamp });
  }

  // Runs the task inline (no Redis), through the same executor as a worker.
  static async execute(...args) {
    register(this);
    return Lab.execute({ ...this.currentTaskOptions(), task: this, arguments: args });
  }

  static withOptions(options) {
    return new Submission(this, options);
  }

  static captured() {
    return Lab.capturedFor(this);
  }

  static clearCaptured() {
    this.captured().length = 0;
  }

  static async executeNext() {
    register(this);
    return Lab.executeNext(this);
  }

  static async publish(envelope) {
    register(this);
    const { Publisher } = require('./publisher');
    return new Publisher().publish({ ...this.currentTaskOptions(), ...envelope });
  }
}

class Submission {
  constructor(task, options) {
    this.task = task;
    this.options = stringifyKeys(Object.fromEntries(Object.entries(options).map(([k, v]) => [snakeCase(k), v])));
  }

  async enqueue(...args) {
    const options = { ...this.options };
    const inline = options.execute;
    const { delay, run_at: runAt } = options;
    delete options.execute;
    delete options.delay;
    delete options.run_at;
    if (inline) return this.task.execute(...args);
    if (delay !== undefined && delay !== null) return this.enqueueAfter(delay, ...args);
    if (runAt !== undefined && runAt !== null) return this.scheduleAt(runAt, ...args);
    return this.publish(args, options);
  }

  async enqueueAfter(delaySeconds, ...args) {
    return this.scheduleAt(Date.now() / 1000 + Number(delaySeconds), ...args);
  }

  async scheduleAt(when, ...args) {
    const timestamp = when instanceof Date ? when.getTime() / 1000 : Number(when);
    const options = { ...this.options };
    delete options.delay;
    delete options.run_at;
    delete options.execute;
    return this.publish(args, options, { run_at: timestamp });
  }

  async publish(args, options, extra = {}) {
    return this.task.publish({ ...options, ...extra, task: this.task, arguments: args });
  }
}

module.exports = { Task, Submission, taskNameOf, register, registerExports, resolveTask, isTaskClass, DEFAULT_CHUNK_SIZE };
