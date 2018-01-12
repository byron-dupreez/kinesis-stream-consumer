'use strict';

/**
 * Unit tests for aws-stream-consumer-core/stream-consumer.js#generateHandlerFunction
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const streamConsumer = require('../kinesis-consumer');
const sequencingRequired = false;

const kinesisProcessing = require('../kinesis-processing');

const TaskDef = require('task-utils/task-defs');

const kinesisCache = require('aws-core-utils/kinesis-cache');
const dynamoDBDocClientCache = require('aws-core-utils/dynamodb-doc-client-cache');

const Promises = require('core-functions/promises');

const strings = require('core-functions/strings');
const stringify = strings.stringify;

const logging = require('logging-utils');
const LogLevel = logging.LogLevel;

const samples = require('./samples');

const dynamoDBMocking = require('aws-core-test-utils/dynamodb-mocking');
const mockDynamoDBDocClient = dynamoDBMocking.mockDynamoDBDocClient;

function setRegionStageAndDeleteCachedInstances(region, stage) {
  // Set up region
  process.env.AWS_REGION = region;
  // Set up stage
  process.env.STAGE = stage;
  // Remove any cached entries before configuring
  deleteCachedInstances();
  return region;
}

function deleteCachedInstances() {
  const region = process.env.AWS_REGION;
  kinesisCache.deleteKinesis(region);
  dynamoDBDocClientCache.deleteDynamoDBDocClient(region);
}

function sampleKinesisEvent(streamName, partitionKey, data, omitEventSourceARN) {
  const region = process.env.AWS_REGION;
  const eventSourceArn = omitEventSourceARN ? undefined : samples.sampleKinesisEventSourceArn(region, streamName);
  return samples.sampleKinesisEventWithSampleRecord(undefined, undefined, partitionKey, data, eventSourceArn, region);
}

function sampleAwsContext(functionVersion, functionAlias, maxTimeInMillis) {
  const region = process.env.AWS_REGION;
  const functionName = 'sampleFunctionName';
  const invokedFunctionArn = samples.sampleInvokedFunctionArn(region, functionName, functionAlias);
  return samples.sampleAwsContext(functionName, functionVersion, invokedFunctionArn, maxTimeInMillis);
}

function dummyKinesis(t, prefix, error) {
  return {
    putRecord(request) {
      return {
        promise() {
          return new Promise((resolve, reject) => {
            t.pass(`${prefix} simulated putRecord to Kinesis with request (${stringify(request)})`);
            if (error)
              reject(error);
            else
              resolve({});
          })
        }
      }
    }
  };
}

let messageNumber = 0;

function sampleMessage(i) {
  ++messageNumber;
  return {
    id: messageNumber,
    msg: `Sample Message ${i}`,
    date: new Date().toISOString(),
    loc: {
      lat: 123.456 + i,
      lon: -67.890 - i
    },
    tags: [`Tag A${i}`, `Tag B${i}`]
  };
}

function sampleExecuteOneAsync(ms, mustRejectWithError, callback) {
  // noinspection JSUnusedLocalSymbols
  function executeOneAsync(message, batch, context) {
    context.info(`${executeOneAsync.name} started processing message (${stringify(message)})`);
    return Promises.delay(ms)
      .then(
        () => {
          if (typeof callback === 'function') {
            callback(message, context);
          }
          if (!mustRejectWithError) {
            context.info(`${executeOneAsync.name} completed message (${stringify(message)})`);
            return message;
          } else {
            context.error(`${executeOneAsync.name} failed intentionally on message (${stringify(message)}) with error (${mustRejectWithError})`,
              mustRejectWithError);
            throw mustRejectWithError;
          }
        },
        err => {
          context.error(`${executeOneAsync.name} hit UNEXPECTED error`, err);
          if (typeof callback === 'function') {
            callback(message, context);
          }
          if (!mustRejectWithError) {
            context.info(`${executeOneAsync.name} "completed" message (${stringify(message)})`);
            return message;
          } else {
            context.error(`${executeOneAsync.name} "failed" intentionally on message (${stringify(message)})`, mustRejectWithError);
            throw mustRejectWithError;
          }
        }
      );
  }

  return executeOneAsync;
}

function sampleExecuteAllAsync(ms, mustRejectWithError, callback) {
  // noinspection JSUnusedLocalSymbols
  function executeAllAsync(messages, batch, context) {
    context.info(`${executeAllAsync.name} started processing messages ${stringify(messages)}`);
    return Promises.delay(ms)
      .then(
        () => {
          if (typeof callback === 'function') {
            callback(message, context);
          }
          if (!mustRejectWithError) {
            context.info(`${executeAllAsync.name} completed messages ${stringify(messages)}`);
            return messages;
          } else {
            context.error(`${executeAllAsync.name} failed intentionally on messages ${stringify(messages)}`, mustRejectWithError);
            throw mustRejectWithError;
          }
        },
        err => {
          context.error(`${executeAllAsync.name} hit UNEXPECTED error`, err);
          if (typeof callback === 'function') {
            callback(message, context);
          }
          if (!mustRejectWithError) {
            context.info(`${executeAllAsync.name} "completed" messages ${stringify(messages)}`);
            return messages;
          } else {
            context.error(`${executeAllAsync.name} "failed" intentionally on messages ${stringify(messages)}`, mustRejectWithError);
            throw mustRejectWithError;
          }
        }
      );
  }

  return executeAllAsync;
}

// =====================================================================================================================
// generateHandlerFunction simulating successful result
// =====================================================================================================================

test('generateHandlerFunction simulating successful result', t => {
  // Setup the task definitions
  function defineProcessOneTaskDefs() {
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    return [taskDef1];
  }

  function defineProcessAllTaskDefs() {
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    return [taskDef2];
  }

  // Create a context and configure it with a dummy Kinesis instance
  function createContext() {
    const context = {};
    const kinesisError = new Error('Planned Kinesis Error');
    context.kinesis = dummyKinesis(t, 'Test generateHandlerFunction', kinesisError);
    context.dynamoDBDocClient = mockDynamoDBDocClient(t, 'Test A', 5, {
      get: undefined,
      put: {result: {}},
      update: {result: {}}
    });
    return context;
  }

  try {
    // Set up environment for testing
    setRegionStageAndDeleteCachedInstances('us-west-2', 'dev99');

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Create a sample AWS Lambda handler function
    const defaultOptions = kinesisProcessing.loadKinesisDefaultOptions();
    defaultOptions.streamProcessingOptions.sequencingRequired = sequencingRequired;

    const opts = {
      logRequestResponseAtLogLevel: LogLevel.INFO,
      failureMsg: 'Failed to process test stream event',
      successMsg: 'Processed test stream event'
    };

    const handler = streamConsumer.generateHandlerFunction(createContext, undefined, defaultOptions,
      defineProcessOneTaskDefs, defineProcessAllTaskDefs, opts);

    // Wrap the callback-based AWS Lambda handler function as a Promise returning function purely for testing purposes
    const handlerWithPromise = Promises.wrap(handler);

    // Invoke the handler function
    handlerWithPromise(event, awsContext)
      .then(result => {
        t.pass(`handler should have passed`);
        t.ok(result, `result must be defined`);
        // Clean up environment
        setRegionStageAndDeleteCachedInstances(undefined, undefined);
        t.end();
      })
      .catch(err => {
        t.fail(`handler should not have failed - ${err}`);
        // Clean up environment
        setRegionStageAndDeleteCachedInstances(undefined, undefined);
        t.end();
      });

  } catch (err) {
    t.fail(`handler should not have failed in try-catch - ${err}`);
    // Clean up environment
    setRegionStageAndDeleteCachedInstances(undefined, undefined);
    t.end();
  }
});

// =====================================================================================================================
// generateHandlerFunction simulating failure
// =====================================================================================================================

test('generateHandlerFunction simulating failure', t => {
  const taskError = new Error('Planned task failure');
  const kinesisError = new Error('Planned Kinesis Error');

  // Setup the task definitions
  function defineProcessOneTaskDefs() {
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, taskError));
    return [taskDef1];
  }

  function defineProcessAllTaskDefs() {
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    return [taskDef2];
  }

  // Create a context and configure it with a dummy Kinesis instance
  function createContext() {
    const context = {};
    context.kinesis = dummyKinesis(t, 'Test generateHandlerFunction', kinesisError);
    context.dynamoDBDocClient = mockDynamoDBDocClient(t, 'Test A', 5, {
      get: undefined,
      put: {result: {}},
      update: {result: {}}
    });
    return context;
  }

  try {
    // Set up environment for testing
    setRegionStageAndDeleteCachedInstances('us-west-2', 'dev99');

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Create a sample AWS Lambda handler function
    const defaultOptions = kinesisProcessing.loadKinesisDefaultOptions();
    defaultOptions.streamProcessingOptions.sequencingRequired = sequencingRequired;

    const opts = {
      logRequestResponseAtLogLevel: LogLevel.DEBUG,
      failureMsg: 'Failed to process test stream event',
      successMsg: 'Processed test stream event'
    };

    const handler = streamConsumer.generateHandlerFunction(createContext, undefined, defaultOptions,
      defineProcessOneTaskDefs, defineProcessAllTaskDefs, opts);

    // Wrap the callback-based AWS Lambda handler function as a Promise returning function purely for testing purposes
    const handlerWithPromise = Promises.wrap(handler);

    // Invoke the handler function
    handlerWithPromise(event, awsContext)
      .then(response => {
        t.fail(`handler should NOT have passed with response ${stringify(response)}`);
        // Clean up environment
        setRegionStageAndDeleteCachedInstances(undefined, undefined);
        t.end();
      })
      .catch(err => {
        t.pass(`handler should have failed - ${err}`);
        const expectedErrorMessagePrefix = 'Planned task failure';
        t.ok(err.message.startsWith(expectedErrorMessagePrefix), `err must start with "${expectedErrorMessagePrefix}"`);
        // t.equal(err, kinesisError, `err must be "${kinesisError}"`);
        // Clean up environment
        setRegionStageAndDeleteCachedInstances(undefined, undefined);
        t.end();
      });

  } catch (err) {
    t.fail(`handler should not have failed in try-catch - ${err}`);
    // Clean up environment
    setRegionStageAndDeleteCachedInstances(undefined, undefined);
    t.end();
  }
});

// =====================================================================================================================
// generateHandlerFunction simulating failure during configuration with missing stage
// =====================================================================================================================

test('generateHandlerFunction simulating failure during configuration with missing stage', t => {
  const taskError = new Error('Planned task failure');
  const kinesisError = new Error('Planned Kinesis Error');

  // Setup the task definitions
  function defineProcessOneTaskDefs() {
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, taskError));
    return [taskDef1];
  }

  function defineProcessAllTaskDefs() {
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    return [taskDef2];
  }

  // Create a context and configure it with a dummy Kinesis instance
  function createContext() {
    const context = {};
    context.kinesis = dummyKinesis(t, 'Test generateHandlerFunction', kinesisError);
    context.dynamoDBDocClient = mockDynamoDBDocClient(t, 'Test A', 5, {
      get: undefined,
      put: {result: {}},
      update: {result: {}}
    });
    return context;
  }

  try {
    // Set up environment for testing without stage to trigger missing stage error
    setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    // Generate a sample AWS event
    const streamName = 'TestStream';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', '1.0.1', maxTimeInMillis);

    // Create a context and configure it with a dummy Kinesis instance
    const context = {};
    const kinesisError = new Error('Planned Kinesis Error');
    context.kinesis = dummyKinesis(t, 'Test generateHandlerFunction', kinesisError);

    // Create a sample AWS Lambda handler function
    const defaultOptions = kinesisProcessing.loadKinesisDefaultOptions();
    defaultOptions.streamProcessingOptions.sequencingRequired = sequencingRequired;

    const opts = {
      logRequestResponseAtLogLevel: LogLevel.DEBUG,
      failureMsg: 'Failed to process test stream event',
      successMsg: 'Processed test stream event'
    };

    const handler = streamConsumer.generateHandlerFunction(createContext, undefined, () => defaultOptions,
      defineProcessOneTaskDefs, defineProcessAllTaskDefs, opts);

    // Wrap the callback-based AWS Lambda handler function as a Promise returning function purely for testing purposes
    const handlerWithPromise = Promises.wrap(handler);

    // Invoke the handler function
    handlerWithPromise(event, awsContext)
      .then(response => {
        t.fail(`handler should NOT have passed with response ${stringify(response)}`);
        // Clean up environment
        setRegionStageAndDeleteCachedInstances(undefined, undefined);
        t.end();
      })
      .catch(err => {
        t.pass(`handler should have failed - ${err}`);
        const expectedErrorMessagePrefix = 'Failed to resolve stage from event';
        t.ok(err.message.startsWith(expectedErrorMessagePrefix), `err must start with "${expectedErrorMessagePrefix}"`);
        // Clean up environment
        setRegionStageAndDeleteCachedInstances(undefined, undefined);
        t.end();
      });

  } catch (err) {
    t.fail(`handler should not have failed in try-catch - ${err}`);
    // Clean up environment
    setRegionStageAndDeleteCachedInstances(undefined, undefined);
    t.end();
  }
});