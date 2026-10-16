'use strict';

const PREFIX = 'solid_jobs';

module.exports = {
  PREFIX,
  CHANNELS: `${PREFIX}:channels`,
  PAUSED_CHANNELS: `${PREFIX}:paused_channels`,
  PLANNED: `${PREFIX}:planned`,
  RETRIES: `${PREFIX}:retries`,
  DISCARDED: `${PREFIX}:discarded`,
  NODES: `${PREFIX}:nodes`,
  ATTEMPTS: `${PREFIX}:attempts`,
  PUBLICATION_PREFIX: `${PREFIX}:publication:`,
  PROCESSED: `${PREFIX}:metrics:processed`,
  FAILED: `${PREFIX}:metrics:failed`,
  CHANNEL_PREFIX: `${PREFIX}:channel:`,
  channel: (name) => `${PREFIX}:channel:${name}`,
  publication: (digest) => `${PREFIX}:publication:${digest}`,
  node: (identity) => `${PREFIX}:node:${identity}`,
  nodeWork: (identity) => `${PREFIX}:node:${identity}:work`,
  nodeSignals: (identity) => `${PREFIX}:node:${identity}:signals`,
  claims: (identity) => `${PREFIX}:node:${identity}:claims`,
  claimed: (identity, executorId) => `${PREFIX}:node:${identity}:claimed:${executorId}`,
};
