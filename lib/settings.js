'use strict';

// Per-thread (main thread or worker thread) current configuration. State lives on globalThis so several copies of
// the package inside one thread still share the same configuration and task registry.
const STATE = (globalThis[Symbol.for('solid-jobs-node')] ??= { config: null, tasks: new Map() });

function config() {
  if (!STATE.config) {
    const { Blueprint } = require('./blueprint');
    STATE.config = new Blueprint();
  }
  return STATE.config;
}

function useConfig(blueprint) {
  STATE.config = blueprint;
  return blueprint;
}

function reset() {
  const previous = STATE.config;
  STATE.config = null;
  if (previous) previous.close();
}

// configure({ redisUrl, concurrency, ... }) or configure((config) => { ... })
function configure(options) {
  const current = config();
  if (typeof options === 'function') options(current);
  else Object.assign(current, options);
  return current;
}

module.exports = { STATE, config, useConfig, reset, configure };
