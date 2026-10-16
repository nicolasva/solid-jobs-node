'use strict';

// Command line runner: `solid-jobs-node -r ./myapp/jobs.js -c 4 --channel critical,3 --channel default`
const fs = require('node:fs');
const path = require('node:path');
const { parseArgs } = require('node:util');
const settings = require('./settings');
const { Conductor } = require('./conductor');
const { VERSION } = require('./version');
const { sleep } = require('./utilities');

const SIGNALS = ['SIGINT', 'SIGTERM', 'SIGTSTP', 'SIGTTIN', 'SIGINFO'];
const HELP = `Usage: solid-jobs-node [options]

  -r, --require <file|module>   task definitions, loaded in every worker thread (repeatable)
  -C, --config <file>           JSON (or .js/.cjs exporting an object) configuration file
  -c, --concurrency <n>         number of worker threads
      --channel <name[,weight]> channel to consume (repeatable)
  -t, --timeout <seconds>       graceful shutdown timeout
      --redis-url <url>         Redis URL used by the solid-redis sidecar
      --warm-ipc                multiplex all Redis traffic over one persistent sidecar connection
  -v, --version                 print the version
  -h, --help                    print this help
`;

function parseChannel(value) {
  const [name, weight] = String(value).split(',');
  return weight ? [name, Number(weight)] : name;
}

function loadFile(file) {
  const resolved = path.resolve(file);
  if (/\.c?js$/.test(resolved)) return require(resolved);
  return JSON.parse(fs.readFileSync(resolved, 'utf8'));
}

function parse(argv) {
  return parseArgs({
    args: argv,
    allowPositionals: false,
    options: {
      require: { type: 'string', short: 'r', multiple: true },
      config: { type: 'string', short: 'C' },
      concurrency: { type: 'string', short: 'c' },
      channel: { type: 'string', multiple: true },
      timeout: { type: 'string', short: 't' },
      'redis-url': { type: 'string' },
      'warm-ipc': { type: 'boolean' },
      version: { type: 'boolean', short: 'v' },
      help: { type: 'boolean', short: 'h' },
    },
  }).values;
}

// Applies CLI options (taking precedence) over the config file onto the current configuration.
function applyOptions(config, options) {
  const values = options.config ? loadFile(options.config) : {};
  const url = options['redis-url'] || values.redisUrl || values.redis_url || values.redis?.url;
  if (url) config.redisUrl = url;
  const concurrency = options.concurrency ? Number(options.concurrency) : values.concurrency;
  if (concurrency) {
    if (!Number.isInteger(concurrency) || concurrency < 1) throw new RangeError('concurrency must be a positive integer');
    config.concurrency = concurrency;
  }
  const channels = options.channel?.length ? options.channel.map(parseChannel) : (values.channels || []).map(parseChannel);
  if (channels.length > 0) config.channels = channels;
  const timeout = options.timeout ? Number(options.timeout) : values.timeout;
  if (timeout) config.shutdownTimeout = timeout;
  if (options['warm-ipc'] || values.warmIpc) config.warmIpc = true;
  config.requires = [...(values.require || []), ...(options.require || [])];
  return config;
}

async function loadRequirements(config) {
  const { resolveRequire } = require('./blueprint');
  const { registerExports } = require('./task');
  for (const entry of config.requires) registerExports(require(resolveRequire(entry)));
}

async function main(argv = process.argv.slice(2)) {
  const options = parse(argv);
  if (options.help) {
    process.stdout.write(HELP);
    return 0;
  }
  if (options.version) {
    process.stdout.write(`solid-jobs-node ${VERSION}\n`);
    return 0;
  }
  const config = applyOptions(settings.config(), options);
  config.logger = require('./blueprint').createLogger(process.env.SOLID_JOBS_LOG_LEVEL || 'info');
  await loadRequirements(config);

  const server = new Conductor(config);
  const received = [];
  const handlers = new Map();
  for (const name of SIGNALS) {
    const handler = () => received.push(name);
    try {
      process.on(name, handler);
      handlers.set(name, handler);
    } catch { /* signal unsupported on this platform */ }
  }
  try {
    await server.start();
    config.logger.info(`solid-jobs-node ${VERSION} started: ${config.concurrency} workers, channels ${config.channels.join(',')}`);
    await waitForShutdown(server, received);
  } finally {
    for (const [name, handler] of handlers) process.removeListener(name, handler);
    await server.stop();
  }
  return 0;
}

async function waitForShutdown(server, received) {
  for (;;) {
    for (let waited = 0; waited < 500; waited += 50) {
      if (received.length > 0) break;
      await sleep(50);
    }
    while (received.length > 0) {
      if (await handle(server, received.shift())) return;
    }
    let remote = null;
    try {
      remote = await server.remoteSignal();
    } catch (error) {
      server.config.logger.warn(`remote signal poll failed: ${error.message}`);
    }
    if (remote && await handle(server, `SIG${remote}`)) return;
  }
}

async function handle(server, name) {
  if (name === 'SIGINT' || name === 'SIGTERM') return true;
  if (name === 'SIGTSTP') await server.quiet();
  else if (name === 'SIGTTIN' || name === 'SIGINFO') server.config.logger.info(`status ${JSON.stringify(server.stats())}`);
  return false;
}

module.exports = { main, parse, applyOptions, parseChannel, handle };
