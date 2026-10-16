'use strict';

// Entry point of every worker thread: one isolated V8 context running one Engine loop.
const { parentPort, workerData } = require('node:worker_threads');

const { Blueprint } = require('./blueprint');
const { Engine } = require('./engine');
const settings = require('./settings');
const { registerExports } = require('./task');
const { ProxyPool } = require('./warm_ipc');

async function main() {
  const { snapshot, processorId } = workerData;
  const config = settings.useConfig(Blueprint.fromSnapshot(snapshot));
  const inbox = [];
  let proxy = null;
  if (snapshot.warmIpc) {
    proxy = new ProxyPool(parentPort);
    config.pool = proxy;
  }
  parentPort.on('message', (message) => {
    if (!(proxy && proxy.receive(message))) inbox.push(message);
  });
  try {
    // User files may call configure() (callbacks, interceptors) and export the Task classes to run.
    for (const entry of snapshot.requires) registerExports(require(entry));
    await config.connect();
  } catch (error) {
    parentPort.postMessage({ type: 'failed', processorId, detail: error && error.stack ? error.stack : String(error) });
    return;
  }

  const control = { drain: () => inbox.splice(0) };
  const engine = new Engine(config, {
    identity: snapshot.identity,
    processorId,
    report: (message) => parentPort.postMessage(message),
  });
  parentPort.postMessage({ type: 'ready', processorId });
  const result = await engine.run(control);
  parentPort.postMessage({ type: 'stopped', processorId, ...result });
}

main().then(() => process.exit(0), (error) => {
  process.stderr.write(`solid-jobs worker crashed: ${error && error.stack ? error.stack : error}\n`);
  process.exit(1);
});
