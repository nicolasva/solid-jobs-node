'use strict';

// Testing helpers: capture published tasks instead of persisting them, or run them inline at enqueue time.
const { NoCapturedTask } = require('./errors');

const state = { mode: null, captured: new Map() };

const mode = () => state.mode;

function capturedFor(taskName) {
  const name = typeof taskName === 'function' ? require('./task').taskNameOf(taskName) : taskName;
  if (!state.captured.has(name)) state.captured.set(name, []);
  return state.captured.get(name);
}

async function withMode(next, fn) {
  const previous = state.mode;
  state.mode = next;
  try {
    return await fn();
  } finally {
    state.mode = previous;
  }
}

const capture = (fn) => withMode('capture', fn);
const inline = (fn) => withMode('execute', fn);

async function execute(envelope) {
  const { Executor } = require('./executor');
  const settings = require('./settings');
  const { InvalidJobError } = require('./errors');
  const { taskNameOf } = require('./task');
  const item = { ...envelope };
  if (typeof item.task === 'function') item.task = taskNameOf(item.task);
  if (!item.id) item.id = `lab-${Date.now()}`;
  if (typeof item.task !== 'string') throw new InvalidJobError('Task must be present');
  // Round trip through JSON so inline execution sees exactly what a worker would.
  const wire = JSON.parse(JSON.stringify(item));
  return new Executor(settings.config()).execute(wire);
}

function executeNext(task) {
  const list = capturedFor(task);
  if (list.length === 0) throw new NoCapturedTask(`No captured task for ${typeof task === 'function' ? task.name : task}`);
  return execute(list.shift());
}

function reset() {
  state.mode = null;
  state.captured.clear();
}

module.exports = { mode, capturedFor, capture, inline, execute, executeNext, reset, state };
