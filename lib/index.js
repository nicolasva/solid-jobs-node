'use strict';

const settings = require('./settings');
const catalog = require('./catalog');
const errors = require('./errors');
const { Blueprint } = require('./blueprint');
const { Conductor } = require('./conductor');
const { Engine } = require('./engine');
const { Executor } = require('./executor');
const { FailurePolicy } = require('./failure_policy');
const { InterceptorRegistry } = require('./interceptors');
const { IntegrityCheck } = require('./integrity_check');
const { Publisher } = require('./publisher');
const { Task } = require('./task');
const { Timer } = require('./timer');
const { VERSION } = require('./version');
const Lab = require('./lab');
const recovery = require('./recovery');
const gateway = require('./gateway');

module.exports = {
  VERSION,
  Task,
  Blueprint,
  Conductor,
  Engine,
  Executor,
  FailurePolicy,
  InterceptorRegistry,
  IntegrityCheck,
  Publisher,
  Timer,
  Lab,
  recovery,
  gateway,
  settings,
  configure: settings.configure,
  config: settings.config,
  ...catalog,
  ...errors,
};
