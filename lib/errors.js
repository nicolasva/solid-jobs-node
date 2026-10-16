'use strict';

class SolidJobsError extends Error {
  constructor(message, options) {
    super(message, options);
    this.name = this.constructor.name;
  }
}

class InvalidJobError extends SolidJobsError {}
class InvalidArgumentError extends InvalidJobError {}
class ConfigurationError extends SolidJobsError {}
class ExecutionHalt extends SolidJobsError {}
class NoCapturedTask extends SolidJobsError {}
class RedisError extends SolidJobsError {}
class RedisConnectionError extends RedisError {}
class RedisCommandError extends RedisError {}
// The solid-redis sidecar could not be started or has died.
class GatewayError extends RedisConnectionError {}

module.exports = {
  SolidJobsError,
  InvalidJobError,
  InvalidArgumentError,
  ConfigurationError,
  ExecutionHalt,
  NoCapturedTask,
  RedisError,
  RedisConnectionError,
  RedisCommandError,
  GatewayError,
};
