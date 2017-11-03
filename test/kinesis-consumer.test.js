'use strict';

/**
 * Unit tests for kinesis-stream-consumer/kinesis-consumer.js
 * @author Byron du Preez
 */

const test = require('tape');

// Leave integrationTestingMode committed as false - ONLY TEMPORARILY set it to true to run in integration testing mode
// instead of normal unit testing mode
const integrationTestingMode = false;
const sequencingRequired = true;
const sequencingPerKey = true;
console.log(`***** RUNNING IN ${integrationTestingMode ? 'INTEGRATION' : 'UNIT'} TESTING MODE ***** (with sequencingRequired: ${sequencingRequired})`);

// The test subject
const consumer = require('../kinesis-consumer');
const prefix = 'kinesis-consumer';
// const streamConsumer = require('aws-stream-consumer-core/stream-consumer');

const kinesisProcessing = require('../kinesis-processing');
const Settings = require('aws-stream-consumer-core/settings');

const tracking = require('aws-stream-consumer-core/tracking');
const toCountString = tracking.toCountString;

// const Batch = require('aws-stream-consumer-core/batch');

// const Settings = require('aws-stream-consumer-core/settings');
// const StreamType = Settings.StreamType;

const dynamoDBMocking = require('aws-core-test-utils/dynamodb-mocking');
const mockDynamoDBDocClient = dynamoDBMocking.mockDynamoDBDocClient;

const taskUtils = require('task-utils');
const TaskDef = require('task-utils/task-defs');
// const Task = require('task-utils/tasks');
// const TaskFactory = require('task-utils/task-factory');

const core = require('task-utils/core');
const StateType = core.StateType;
// const ReturnMode = core.ReturnMode;

const taskStates = require('task-utils/task-states');
// const TaskState = taskStates.TaskState;
// const Unstarted = taskStates.Unstarted;
// const Started = taskStates.Started;
const CompletedState = taskStates.CompletedState;
const Completed = taskStates.Completed;
// const Succeeded = taskStates.Succeeded;
// const FailedState = taskStates.FailedState;
const Failed = taskStates.Failed;
// const TimedOutState = taskStates.TimedOutState;
const TimedOut = taskStates.TimedOut;
// const RejectedState = taskStates.RejectedState;
const Rejected = taskStates.Rejected;
const Discarded = taskStates.Discarded;
const Abandoned = taskStates.Abandoned;

// const stages = require('aws-core-utils/stages');
const kinesisCache = require('aws-core-utils/kinesis-cache');
const dynamoDBDocClientCache = require('aws-core-utils/dynamodb-doc-client-cache');

const Promises = require('core-functions/promises');

const strings = require('core-functions/strings');
const stringify = strings.stringify;

const Numbers = require('core-functions/numbers');

const errors = require('core-functions/errors');
const TimeoutError = errors.TimeoutError;

const copying = require('core-functions/copying');
const copy = copying.copy;
const deep = copying.defaultCopyOpts.deep;
const merging = require('core-functions/merging');
const merge = merging.merge;

const logging = require('logging-utils');

const samples = require('./samples');
const sampleKinesisEventSourceArn = samples.sampleKinesisEventSourceArn;
const sampleKinesisRecord2 = samples.sampleKinesisRecord2;
const sampleKinesisEventWithRecords = samples.sampleKinesisEventWithRecords;

// const incompletePrefix = 'Triggering a replay of the entire batch';

const streamName = 'TEST_Stream_DEV';

// const taskFactory = new TaskFactory({logger: console}, {returnMode: ReturnMode.PROMISE});

// function sampleKinesisEvent(streamName, partitionKey, data, omitEventSourceARN) {
//   const region = process.env.AWS_REGION;
//   const eventSourceArn = omitEventSourceARN ? undefined : samples.sampleKinesisEventSourceArn(region, streamName);
//   return samples.sampleKinesisEventWithSampleRecord(undefined, undefined, partitionKey, data, eventSourceArn, region);
// }

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

function sampleAwsContext(functionVersion, functionAlias, maxTimeInMillis) {
  const region = process.env.AWS_REGION;
  const functionName = 'sampleFunctionName';
  const invokedFunctionArn = samples.sampleInvokedFunctionArn(region, functionName, functionAlias);
  return samples.sampleAwsContext(functionName, functionVersion, invokedFunctionArn, maxTimeInMillis);
}

// Simulate ideal conditions - everything meant to be configured beforehand has been configured
function createContext(idPropertyNames, keyPropertyNames, seqNoPropertyNames, event, awsContext) {
  const defaultOptions = kinesisProcessing.loadKinesisDefaultOptions();
  defaultOptions.streamProcessingOptions.sequencingRequired = sequencingRequired;
  defaultOptions.streamProcessingOptions.sequencingPerKey = sequencingPerKey;

  const options = {
    loggingOptions: {
      logLevel: logging.LogLevel.TRACE
    },
    streamProcessingOptions: {
      consumerIdSuffix: undefined,
      maxNumberOfAttempts: 3,
      idPropertyNames: idPropertyNames ? idPropertyNames : ['id1', 'id2'],
      keyPropertyNames: keyPropertyNames ? keyPropertyNames : ['k1', 'k2'],
      seqNoPropertyNames: seqNoPropertyNames ? seqNoPropertyNames : ['n1', 'n2', 'n3', 'n4'],
      batchStateTableName: 'TEST_StreamConsumerBatchState',
      deadRecordQueueName: 'TEST_DRQ',
      deadMessageQueueName: 'TEST_DMQ'
    }
  };
  if (integrationTestingMode) {
    options.kinesisOptions = {maxRetries: 0};
    options.dynamoDBDocClientOptions = {maxRetries: 0};
  }

  merge(defaultOptions, options, merging.defaultMergeOpts.deepNoReplace);

  if (!integrationTestingMode) {
    options.kinesisOptions = undefined;
    options.dynamoDBDocClientOptions = undefined;
  }

  return consumer.configureStreamConsumer({}, undefined, options, event, awsContext);
}

// function configureKinesisAndDynamoDBDocClient(t, context, prefix, kinesisError, loadError, loadData, loadCallback, saveError, dynamoDelay) {
function configureKinesisAndDynamoDBDocClient(t, context, prefix, kinesisError, dynamoDelay, mockResponseSourcesByMethodName) {
  if (!integrationTestingMode) {
    context.kinesis = dummyKinesis(t, prefix, kinesisError);

    // context.dynamoDBDocClient = dummyDynamoDBDocClient(t, prefix, loadError, loadData, saveError, dynamoDelay, loadCallback, context);
    context.dynamoDBDocClient = mockDynamoDBDocClient(t, prefix, dynamoDelay, mockResponseSourcesByMethodName);
  }
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

function sampleExecuteOneAsync(delayMs, mustRejectWithError, callback) {
  function executeOneAsync(message, batch, context) {
    const state = batch.states.get(message);
    const messageId = state.id ? state.id : state.eventID;
    console.log(`*** ${executeOneAsync.name} started processing message (${messageId})`);
    return Promises.delay(delayMs).then(
      () => {
        if (typeof callback === 'function') {
          callback(message, batch, context);
        }
        if (!mustRejectWithError) {
          console.log(`*** ${executeOneAsync.name} completed message (${messageId})`);
          return message;
        } else {
          const error = new Error('Failing process one task (1)');
          console.error(`*** ${executeOneAsync.name} failed intentionally on message (${messageId})`, error); //mustRejectWithError);
          throw error; //mustRejectWithError;
        }
      },
      err => {
        console.error(`*** ${executeOneAsync.name} hit UNEXPECTED error on message (${messageId})`, err);
        if (typeof callback === 'function') {
          callback(message, batch, context);
        }
        if (!mustRejectWithError) {
          console.log(`*** ${executeOneAsync.name} "completed" message (${messageId})`);
          return message;
        } else {
          const error = new Error('Failing process one task (2)');
          console.error(`*** ${executeOneAsync.name} "failed" intentionally on message (${messageId})`, error, '- Unexpected error', err); //mustRejectWithError);
          throw error; // mustRejectWithError;
        }
      }
    );
  }

  return executeOneAsync;
}

function sampleExecuteAllAsync(delayMs, mustRejectWithError, callback) {

  function executeAllAsync(batch, incompleteMessages, context) {
    const m = batch.messages.length;
    const ms = toCountString(m, 'message');
    const i = incompleteMessages ? incompleteMessages.length : 0;
    const isOfMs = `${i} incomplete of ${ms}`;

    console.log(`*** ${executeAllAsync.name} started processing ${isOfMs}`);
    return Promises.delay(delayMs).then(
      () => {
        if (typeof callback === 'function') {
          callback(batch, incompleteMessages, context);
        }
        if (!mustRejectWithError) {
          console.log(`*** ${executeAllAsync.name} completed ${isOfMs}`);
          return incompleteMessages;
        } else {
          const error = new Error('Failing process all task (1)');
          console.error(`*** ${executeAllAsync.name} failed intentionally on ${isOfMs}`, error); //mustRejectWithError);
          throw error; // mustRejectWithError;
        }
      },
      err => {
        console.error(`*** ${executeAllAsync.name} hit UNEXPECTED error`, err);
        if (typeof callback === 'function') {
          callback(batch, incompleteMessages, context);
        }
        if (!mustRejectWithError) {
          console.log(`*** ${executeAllAsync.name} "completed" ${isOfMs} messages`);
          return incompleteMessages;
        } else {
          const error = new Error('Failing process all task (2)');
          console.error(`*** ${executeAllAsync.name} "failed" intentionally on ${isOfMs} messages`, error, '- Unexpected error', err); //mustRejectWithError);
          throw error; // mustRejectWithError;
        }
      }
    );
  }

  return executeAllAsync;
}

// =====================================================================================================================
// processStreamEvent
// =====================================================================================================================

function checkMessagesTasksStates(t, batch, messages, oneStateTypeOrTypes, allStateTypeOrTypes) {
  for (let i = 0; i < messages.length; ++i) {
    checkMessageTasksStates(t, messages[i], batch, oneStateTypeOrTypes, allStateTypeOrTypes)
  }
  if (allStateTypeOrTypes) {
    const processAllTasksByName = batch.getProcessAllTasks();
    const processAllTasks = taskUtils.getTasksAndSubTasks(processAllTasksByName);
    if (Array.isArray(allStateTypeOrTypes)) {
      t.ok(processAllTasks.every(t => allStateTypeOrTypes.some(state => t.state instanceof state)), `batch (${batch.shardOrEventID}) every process all task state ${JSON.stringify(processAllTasks.map(t => t.state.name || t.state.type))} must be instance of ${JSON.stringify(allStateTypeOrTypes.map(s => s.name))}`);
    } else {
      t.ok(processAllTasks.every(t => t.state instanceof allStateTypeOrTypes), `batch (${batch.shardOrEventID}) every process all task state ${JSON.stringify(processAllTasks.map(t => t.state.name || t.state.type))} must be instance of ${allStateTypeOrTypes.name}`);
    }
  }
}

function checkMessageTasksStates(t, message, batch, oneStateTypeOrTypes, allStateTypeOrTypes) {
  const state = batch.states.get(message);
  if (oneStateTypeOrTypes) {
    const processOneTasks = taskUtils.getTasksAndSubTasks(state.ones);
    if (Array.isArray(oneStateTypeOrTypes)) {
      t.ok(processOneTasks.every(t => oneStateTypeOrTypes.some(state => t.state instanceof state)), `message (${state.msgDesc}) every process one task state ${JSON.stringify(processOneTasks.map(t => t.state.name || t.state.type))} must be instance of ${JSON.stringify(oneStateTypeOrTypes.map(s => s.name))}`);
    } else {
      t.ok(processOneTasks.every(t => t.state instanceof oneStateTypeOrTypes), `message (${state.msgDesc}) every process one task state ${JSON.stringify(processOneTasks.map(t => t.state.name || t.state.type))} must be instance of ${oneStateTypeOrTypes.name}`);
    }
  }
  if (allStateTypeOrTypes) {
    const processAllTasks = taskUtils.getTasksAndSubTasks(state.alls);
    if (Array.isArray(allStateTypeOrTypes)) {
      t.ok(processAllTasks.every(t => allStateTypeOrTypes.some(state => t.state instanceof state)), `message (${state.msgDesc}) every process all task ${JSON.stringify(processAllTasks.map(t => t.state.name || t.state.type))} state must be instance of ${JSON.stringify(allStateTypeOrTypes.map(s => s.name))}`);
    } else {
      t.ok(processAllTasks.every(t => t.state instanceof allStateTypeOrTypes), `message (${state.msgDesc}) every process all task state ${JSON.stringify(processAllTasks.map(t => t.state.name || t.state.type))} must be instance of ${allStateTypeOrTypes.name}`);
    }
  }
}

function checkUnusableRecordsTasksStates(t, batch, states, attempts) {
  const records = batch.unusableRecords;
  for (let i = 0; i < records.length; ++i) {
    const state = Array.isArray(states) ? states[i] : states;
    const attempt = Array.isArray(attempts) ? attempts[i] : attempts;
    checkUnusableRecordTasksStates(t, records[i], batch, state, attempt)
  }
}

function checkUnusableRecordTasksStates(t, record, batch, state, attempt) {
  if (state) {
    const discardOneTasksByName = batch.getDiscardOneTasks(record);
    const discardOneTasks = taskUtils.getTasksAndSubTasks(discardOneTasksByName);
    // const recordState = batch.states.get(record);
    const prefix = `unusable record (${record.eventID})`;
    for (let i = 0; i < discardOneTasks.length; ++i) {
      const task = discardOneTasks[i];
      t.ok(task.state instanceof state, `${prefix} ${task.name} state must be an instance of ${state.name}`);
      t.equal(task.attempts, attempt, `${prefix} ${task.name} attempts must be ${attempt}`);

    }
  }
}

function checkBatchTaskStates(t, batchState, tasksByNameName, taskName, names, tasksFullyFinalised, states) {
  const tasksByName = batchState[tasksByNameName];
  for (let i = 0; i < names.length; ++i) {
    const name = names[i];

    const task = name ? taskUtils.getSubTask(tasksByName, taskName, name.split('.')) : tasksByName[taskName];

    const prefix = `batch ${taskName}${name ? ` ${name}` : ''}`;

    if (Array.isArray(states[i])) {
      const ss = states[i];
      let stateIndex = -1;
      const instanceOfState = ss.reduce((acc, state, s) => {
        const isInstance = task.state instanceof state;
        if (isInstance) stateIndex = s;
        return acc || isInstance;
      }, false);

      const fullyFinalisedOneOrMore = tasksFullyFinalised[i];
      const fullyFinalised = Array.isArray(fullyFinalisedOneOrMore) ?
        fullyFinalisedOneOrMore[stateIndex] : fullyFinalisedOneOrMore;

      t.ok(instanceOfState, `${prefix} state must be an instanceof ${JSON.stringify(ss)} (actual: ${task.state.name})`);
      t.equal(task.isFullyFinalised(), fullyFinalised, `${prefix} isFullyFinalised must be ${fullyFinalised}`);

    } else {
      const state = states[i];
      t.ok(task.state instanceof state, `${prefix} state must be ${state.name}${state.name !== task.state.name ? ` (actual: ${task.state.name})` : ''}`);
      const fullyFinalised = tasksFullyFinalised[i];
      t.equal(task.isFullyFinalised(), fullyFinalised, `${prefix} isFullyFinalised must be ${fullyFinalised}`);
    }
  }
}

function checkBatchInitiatingTaskStates(t, batchState, tasksFullyFinalised, states) {
  const names = ['', 'extractAndSequenceMessages', 'loadBatchState', 'loadBatchState.loadBatchStateFromDynamoDB', 'reviveTasks'];
  checkBatchTaskStates(t, batchState, 'initiating', 'initiateBatch', names, tasksFullyFinalised, states);
}

function checkBatchProcessingTaskStates(t, batchState, tasksFullyFinalised, states) {
  const names = ['', 'executeAllProcessOneTasks', 'executeAllProcessAllTasks', 'discardUnusableRecords'];
  checkBatchTaskStates(t, batchState, 'processing', 'processBatch', names, tasksFullyFinalised, states);
}

function checkBatchFinalisingTaskStates(t, batchState, tasksFullyFinalised, states) {
  const names = ['', 'discardAnyRejectedMessages', 'saveBatchState', 'saveBatchState.saveBatchStateToDynamoDB'];
  checkBatchTaskStates(t, batchState, 'finalising', 'finaliseBatch', names, tasksFullyFinalised, states);
}

function pad(number, digits) {
  return Numbers.zeroPadLeft(`${number}`, digits);
}

// function generateMessages(s, n, opts) {
//   const k = opts && opts.uniqueKeys && !Number.isNaN(Number(opts.uniqueKeys)) ? Math.max(Math.min(Number(opts.uniqueKeys), n), 1) : Math.max(n, 1);
//   const messages = new Array(n);
//
//   const ss = pad(s, 2);
//   for (let i = 0; i < n; ++i) {
//     const ii = pad(i, 2);
//     messages[i] = samples.sampleMsg(`ID-${ss}`, `70${ii}`, 'ABC', 10 + (i % k),
//       1, 100, '10000000000000000000001', `2017-01-17T23:59:59.0${ii}Z`);
//   }
//   return messages;
// }

function generateEvent(s, n, region, opts, partitionKey) {
  const b = opts && opts.badApples && !Number.isNaN(Number(opts.badApples)) ? Number(opts.badApples) : 0;
  const u = opts && opts.unusables && !Number.isNaN(Number(opts.unusables)) ? Number(opts.unusables) : 0;
  const k = opts && opts.uniqueKeys && !Number.isNaN(Number(opts.uniqueKeys)) ? Math.max(Math.min(Number(opts.uniqueKeys), n), 1) : Math.max(n, 1);
  const records = new Array(n + u + b);

  const eventSourceARN = sampleKinesisEventSourceArn(region, streamName);
  const ss = pad(s, 2);
  for (let i = 0; i < n; ++i) {
    const ii = pad(i, 2);
    const shardId = `shardId-0000000000${ss}`;
    const seqNo = `495451152434909850182800677149731445821800625932442009${ii}`;
    // const msg = samples.sampleMsg(`ID-${ss}`, `70${ii}`, 'ABC', 10 + (i % k), 1, 100, '10000000000000000000001', `2017-01-17T23:59:59.0${ii}Z`);
    // records[i] = samples.sampleKinesisRecord(shardId, seqNo, partitionKey, msg, eventSourceARN, 'eu-west-1');
    records[i] = sampleKinesisRecord2(shardId, seqNo, eventSourceARN, `ID-${ss}`, `70${ii}`, 'ABC', 10 + (i % k),
      1, 100, '10000000000000000000001', `2017-01-17T23:59:59.0${ii}Z`, undefined, partitionKey);
  }

  for (let i = n; i < n + u; ++i) {
    const ii = pad(i, 2);
    const shardId = `shardId-2222222222${ss}`;
    const seqNo = `495451152434909850182800677149731445821800625932442009${ii}`;
    // const msg = samples.sampleMsg(`ID-${ss}`, `70${ii}`, 'ABC', 10 + (i % k), 1, 100, '10000000000000000000001', `2017-01-17T23:59:59.0${ii}Z`);
    // records[i] = samples.sampleKinesisRecord(shardId, seqNo, partitionKey, msg, eventSourceARN, 'eu-west-1');
    records[i] = sampleKinesisRecord2(shardId, seqNo, eventSourceARN, `ID-${ss}`, `70${ii}`, 'ABC', 10 + (i % k),
      1, 100, '10000000000000000000001', `2017-01-17T23:59:59.0${ii}Z`, undefined, partitionKey);
    records[i].kinesis.data = `BAD-DATA-${ss}-${ii}`; // break the data
  }

  for (let i = n + u; i < n + u + b; ++i) {
    const ii = pad(i, 2);
    records[i] = {bad: `apple-${ss}-${ii}`};
  }

  records.reverse(); // Start them in the worst sequence possible

  return sampleKinesisEventWithRecords(records);
}

// =====================================================================================================================
// processStreamEvent with successful message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that succeeds all tasks', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000001';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 1, `item.messageStates.length must be 1`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const event = generateEvent(1, n, region, {unusables: u, uniqueKeys: n});

    // Generate a sample AWS context
    const maxTimeInMillis = 1200000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);

      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, 5, {
        get: [],
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          t.equal(batch.firstMessagesToProcess.length, n, `processStreamEvent batch must have ${n} first messages to process`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
          t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

          checkMessagesTasksStates(t, batch, messages, CompletedState, CompletedState, context);
          for (let i = 0; i < messages.length; ++i) {
            const messageState = batch.states.get(messages[i]);
            t.equal(messageState.ones.Task1.attempts, 1, `message [${i}] (${messageState.id}) Task1 attempts must be 1`);
            t.equal(messageState.alls.Task2.attempts, 1, `message [${i}] (${messageState.id}) Task2 attempts must be 1`);
          }

          const batchState = batch.states.get(batch);
          t.equal(batchState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${JSON.stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${JSON.stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that succeeds all tasks (despite broken Kinesis, i.e. no unusable/rejected/incomplete)', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000002';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 1, `item.messageStates.length must be 1`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const event = generateEvent(2, n, region, {unusables: u, uniqueKeys: n});

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);

      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, new Error('Disabling Kinesis'), 5, {
        get: [],
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          t.equal(batch.firstMessagesToProcess.length, n, `processStreamEvent batch must have ${n} first messages to process`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
          t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

          checkMessagesTasksStates(t, batch, messages, CompletedState, CompletedState, context);
          t.equal(batch.states.get(messages[0]).ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          for (let i = 0; i < messages.length; ++i) {
            const messageState = batch.states.get(messages[i]);
            t.equal(messageState.ones.Task1.attempts, 1, `message [${i}] (${messageState.id}) Task1 attempts must be 1`);
            t.equal(messageState.alls.Task2.attempts, 1, `message [${i}] (${messageState.id}) Task2 attempts must be 1`);
          }

          const batchState = batch.states.get(batch);
          t.equal(batchState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${JSON.stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${JSON.stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 10 messages with 10 unique keys that succeed all tasks (despite broken Kinesis, i.e. no unusable/rejected/incomplete)', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000003';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 10, `item.messageStates.length must be 10`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 10;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = '7425bb79169cfe3977ea6eb1fb0ecd66';
    const event = generateEvent(3, n, region, {unusables: u, uniqueKeys: n}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 120000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4'], event, awsContext);

      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, new Error('Disabling Kinesis'), 5, {
        get: [],
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          t.equal(batch.firstMessagesToProcess.length, n, `processStreamEvent batch must have ${n} first messages to process`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
          t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

          checkMessagesTasksStates(t, batch, messages, CompletedState, CompletedState, context);
          for (let i = 0; i < messages.length; ++i) {
            const messageState = batch.states.get(messages[i]);
            t.equal(messageState.ones.Task1.attempts, 1, `message [${i}] (${messageState.id}) Task1 attempts must be 1`);
            t.equal(messageState.alls.Task2.attempts, 1, `message [${i}] (${messageState.id}) Task2 attempts must be 1`);
          }
          const batchState = batch.states.get(batch);
          t.equal(batchState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});


test('processStreamEvent with 10 messages with 10 unique keys that succeed all tasks AND with PRIOR state (despite broken Kinesis, i.e. no unusable/rejected/incomplete)', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000003';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 10, `item.messageStates.length must be 10`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 10;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = '7425bb79169cfe3977ea6eb1fb0ecd66';
    const event = generateEvent(3, n, region, {unusables: u, uniqueKeys: n}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 120000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4'], event, awsContext);

      const getResult = copy(require('./batch-10-state.json'), deep);
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, new Error('Disabling Kinesis'), 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          t.equal(batch.firstMessagesToProcess.length, n, `processStreamEvent batch must have ${n} first messages to process`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
          t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

          checkMessagesTasksStates(t, batch, messages, CompletedState, CompletedState, context);
          for (let i = 0; i < messages.length; ++i) {
            const messageState = batch.states.get(messages[i]);
            t.equal(messageState.ones.Task1.attempts, 1, `message [${i}] (${messageState.id}) Task1 attempts must be 1`);
            t.equal(messageState.alls.Task2.attempts, 1, `message [${i}] (${messageState.id}) Task2 attempts must be 1`);
          }
          const batchState = batch.states.get(batch);
          t.equal(batchState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 10 messages with 10 unique keys that succeed all tasks AND with COMPLETELY MISMATCHED PRIOR state (despite broken Kinesis, i.e. no unusable/rejected/incomplete)', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000003';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 10, `item.messageStates.length must be 10`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 10;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = undefined; // random
    const event = generateEvent(3, n, region, {unusables: u, uniqueKeys: n}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 120000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4'], event, awsContext);

      const getResult = copy(require('./batch-10-state.json'), deep);
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, new Error('Disabling Kinesis'), 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          t.equal(batch.firstMessagesToProcess.length, n, `processStreamEvent batch must have ${n} first messages to process`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
          t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

          checkMessagesTasksStates(t, batch, messages, CompletedState, CompletedState, context);
          for (let i = 0; i < messages.length; ++i) {
            const messageState = batch.states.get(messages[i]);
            t.equal(messageState.ones.Task1.attempts, 1, `message [${i}] (${messageState.id}) Task1 attempts must be 1`);
            t.equal(messageState.alls.Task2.attempts, 1, `message [${i}] (${messageState.id}) Task2 attempts must be 1`);
          }
          const batchState = batch.states.get(batch);
          t.equal(batchState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 10 messages with 2 unique keys that succeed all tasks (despite broken Kinesis, i.e. no unusable/rejected/incomplete)', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000003';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 10, `item.messageStates.length must be 10`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 10;
    const k = 2;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = undefined;
    const event = generateEvent(3, n, region, {unusables: u, uniqueKeys: k}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 120000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4'], event, awsContext);

      const getResult = undefined;
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, new Error('Disabling Kinesis'), 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          t.equal(batch.firstMessagesToProcess.length, k, `processStreamEvent batch must have ${k} first messages to process`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
          t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

          checkMessagesTasksStates(t, batch, messages, CompletedState, CompletedState, context);
          for (let i = 0; i < messages.length; ++i) {
            const messageState = batch.states.get(messages[i]);
            t.equal(messageState.ones.Task1.attempts, 1, `message [${i}] (${messageState.id}) Task1 attempts must be 1`);
            // t.equal(messageState.alls.Task2.attempts, 1, `message [${i}] (${messageState.id}) Task2 attempts must be 1`);
          }
          const batchState = batch.states.get(batch);
          t.equal(batchState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with unusable record(s)
// =====================================================================================================================

test('processStreamEvent with 1 unusable record', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000004';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 1, `item.messageStates.length must be 1`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 0;
    const u = 1;

    // Generate a sample AWS event
    const partitionKey = undefined;
    const event = generateEvent(4, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);

      const getResult = [];
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);


      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const messages = batch.messages;

          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, 0, `processStreamEvent batch must have ${0} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
          t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

          checkMessagesTasksStates(t, batch, messages, CompletedState, CompletedState, context);
          for (let i = 0; i < messages.length; ++i) {
            const messageState = batch.states.get(messages[i]);
            t.equal(messageState.ones.Task1.attempts, 1, `message [${i}] (${messageState.id}) Task1 attempts must be 1`);
            t.equal(messageState.alls.Task2.attempts, 1, `message [${i}] (${messageState.id}) Task2 attempts must be 1`);
          }

          checkUnusableRecordsTasksStates(t, batch, CompletedState, 1, context);

          const batchState = batch.states.get(batch);
          t.notOk(batchState.alls, `batch alls must be undefined`);
          // t.equal(batchState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${JSON.stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${JSON.stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 unusable record, but if cannot discard must fail', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-222222222205';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 0, `item.messageStates.length must be 0`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 1, `item.unusableRecordStates.length must be 1`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 0;
    const u = 1;

    // Generate a sample AWS event
    const partitionKey = undefined;
    const event = generateEvent(5, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const kinesisError = new Error('Disabling Kinesis');
      const context = createContext(undefined, undefined, undefined, event, awsContext);

      const getResult = undefined;
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, kinesisError, 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.fail(`processStreamEvent must NOT resolve with batch (${batch.shardOrEventID}) ${stringify(batch)}`);
          t.end();
        })
        .catch(err => {
            t.pass(`processStreamEvent must reject with error (${err})`);
            t.equal(err, kinesisError, `processStreamEvent error must be ${kinesisError}`);
            // t.ok(err.message.startsWith(incompletePrefix), `processStreamEvent error must start with '${incompletePrefix}'`);

            const batch = err.batch;
            const messages = batch.messages;
            const batchState = batch.states.get(batch);

            t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
            t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
            t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
            t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
            t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
            t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

            checkUnusableRecordsTasksStates(t, batch, Failed, 1, context);
            t.equal(batch.states.get(batch.unusableRecords[0]).discards.discardUnusableRecord.error, kinesisError, `discardUnusableRecord error must be ${kinesisError}`);

            t.notOk(batchState.alls, `batchState.alls must NOT be defined`);

            checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
            checkBatchProcessingTaskStates(t, batchState, [false, true, true, false], [Failed, Completed, Completed, Failed]);
            checkBatchFinalisingTaskStates(t, batchState, [false, true, true, true], [Failed, Completed, Completed, Completed]);

            t.end();
          }
        )
        .catch(err => {
          t.end(err);
        });

    } catch
      (err) {
      t.fail(`processStreamEvent should NOT have failed on try-catch (${err})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with failing processOne message(s) - must replay if maxNumberOfAttempts NOT reached
// =====================================================================================================================

test('processStreamEvent with 1 message that fails its processOne task must replay if maxNumberOfAttempts NOT reached', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000006';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 1, `item.messageStates.length must be 1`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = undefined;
    const event = generateEvent(6, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const processOneError = new Error('Failing process one task');
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, processOneError));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);

      const getResult = undefined;
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.fail(`processStreamEvent must NOT resolve with batch (${batch.shardOrEventID}) ${stringify(batch)}`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${err})`);
          // t.equal(err, processOneError, `processStreamEvent error must be ${processOneError}`);
          t.ok(err.message.startsWith(processOneError.message), `processStreamEvent error message must start with ${processOneError.message}`);
          // t.ok(err.message.startsWith(incompletePrefix), `processStreamEvent error must start with '${incompletePrefix}'`);

          const batch = err.batch;
          const messages = batch.messages;
          const batchState = batch.states.get(batch);

          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
          t.equal(batch.incompleteMessages.length, 1, `processStreamEvent batch must have ${1} incomplete message`);

          checkMessagesTasksStates(t, batch, messages, Failed, Completed, context);
          const msg0State = batch.states.get(messages[0]);
          t.equal(msg0State.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          // t.equal(msg0State.ones.Task1.error, processOneError, `Task1 error must be ${processOneError}`);
          t.ok(msg0State.ones.Task1.error.message.startsWith(processOneError.message), `Task1 error message must start with ${processOneError.message}`);
          t.equal(msg0State.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [false, false, true, true], [Failed, Failed, Completed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [false, true, true, true], [Failed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.end(err);
        });

    } catch
      (err) {
      t.fail(`processStreamEvent should NOT have failed on try-catch (${err})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with failing processOne message(s) - must NOT replay if maxNumberOfAttempts is reached
// =====================================================================================================================

test('processStreamEvent with 1 message that fails its processOne task must NOT replay if maxNumberOfAttempts is reached, ', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000007';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 0, `item.messageStates.length must be 0`);
    t.equal(item.rejectedMessageStates.length, 1, `item.rejectedMessageStates.length must be 1`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = undefined;
    const event = generateEvent(7, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const processOneError = new Error('Failing process one task');
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, processOneError));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);
      context.streamProcessing.maxNumberOfAttempts = 1; // Extreme case of ONLY 1 attempt allowed

      const getResult = undefined;
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);


      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve with batch (${batch.shardOrEventID})`);
          const messages = batch.messages;
          const rejectedMessages = batch.rejectedMessages;
          const batchState = batch.states.get(batch);

          t.equal(messages.length, 0, `processStreamEvent batch must have ${0} messages`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(rejectedMessages.length, 1, `processStreamEvent batch must have ${1} rejected message`);
          // t.equal(batch.undiscardedRejectedMessages.length, 1, `processStreamEvent batch must have ${1} undiscarded rejected message`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected message`);
          t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

          checkMessagesTasksStates(t, batch, rejectedMessages, Discarded, Completed, context);
          const msg0State = batch.states.get(rejectedMessages[0]);
          t.equal(msg0State.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          t.equal(msg0State.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [false, false, true, true], [Failed, Failed, Completed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent must NOT reject with error (${err})`);
          t.end();
        })
        .catch(err => {
          t.end(err);
        });

    } catch
      (err) {
      t.fail(`processStreamEvent should NOT have failed on try-catch (${err})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that fails its processOne task, must replay if Kinesis resubmit is disabled', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000008';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 1, `item.messageStates.length must be 1`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = undefined;
    const event = generateEvent(8, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const processOneError = new Error('Failing process one task');
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, processOneError));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);
      const kinesisError = new Error('Disabling Kinesis');

      const getResult = undefined;
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, kinesisError, 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.fail(`processStreamEvent must NOT resolve with batch (${batch.shardOrEventID}) ${stringify(batch)}`);
          t.end();
        })
        .catch(err => {
            t.pass(`processStreamEvent must reject with error (${err})`);
            // t.equal(err, processOneError, `processStreamEvent error must be ${processOneError}`);
            t.ok(err.message.startsWith(processOneError.message), `processStreamEvent error must start with '${processOneError.message}'`);
            // t.ok(err.message.startsWith(incompletePrefix), `processStreamEvent error (${err}) must start with '${incompletePrefix}'`);

            const batch = err.batch;
            const messages = batch.messages;
            const batchState = batch.states.get(batch);

            t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
            t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
            t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
            t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
            t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
            t.equal(batch.incompleteMessages.length, 1, `processStreamEvent batch must have ${1} incomplete message`);

            checkMessagesTasksStates(t, batch, messages, Failed, Completed, context);
            const msg0State = batch.states.get(messages[0]);
            t.equal(msg0State.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
            // t.equal(msg0State.ones.Task1.error, processOneError, `Task1 error must be ${processOneError}`);
            t.ok(msg0State.ones.Task1.error.message.startsWith(processOneError.message), `Task1 error message must start with ${processOneError.message}`);
            t.equal(msg0State.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

            checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
            checkBatchProcessingTaskStates(t, batchState, [false, false, true, true], [Failed, Failed, Completed, Completed]);
            checkBatchFinalisingTaskStates(t, batchState, [false, true, true, true], [Failed, Completed, Completed, Completed]);

            t.end();
          }
        )
        .catch(err => {
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with failing processAll message(s) - must replay if maxNumberOfAttempts NOT reached
// =====================================================================================================================

test('processStreamEvent with 1 message that fails its processAll task - must replay if maxNumberOfAttempts NOT reached', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000009';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 1, `item.messageStates.length must be 1`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const event = generateEvent(9, n, region, {unusables: u});

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const processAllError = new Error('Failing process all task');
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, processAllError));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);

      const getResult = undefined;
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.fail(`processStreamEvent must NOT resolve with batch (${batch.shardOrEventID}) ${stringify(batch)}`);
          t.end();
        })
        .catch(err => {
            t.pass(`processStreamEvent must reject with error (${err})`);
            // t.equal(err, processAllError, `processStreamEvent error must be ${processAllError}`);
            t.ok(err.message.startsWith(processAllError.message), `processStreamEvent error must start with '${processAllError.message}'`);
            // t.ok(err.message.startsWith(incompletePrefix), `processStreamEvent error must start with '${incompletePrefix}'`);

            const batch = err.batch;
            const messages = batch.messages;
            const batchState = batch.states.get(batch);

            t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
            t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
            t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
            t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
            t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
            t.equal(batch.incompleteMessages.length, 1, `processStreamEvent batch must have ${1} incomplete messages`);
            t.notOk(batch.isFullyFinalised(), `processStreamEvent batch must NOT be fully finalised`);

            checkMessagesTasksStates(t, batch, messages, CompletedState, Failed, context);
            const msg0State = batch.states.get(messages[0]);
            t.equal(msg0State.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
            // t.equal(msg0State.alls.Task2.attempts, 1, `Task2 attempts must be 1`);
            // t.equal(msg0State.alls.Task2.error, processAllError, `Task2 error must be ${processAllError}`);

            checkBatchTaskStates(t, batchState, 'alls', 'Task2', [''], [false], [Failed]);
            t.equal(batchState.alls.Task2.attempts, 1, `Batch Task2 attempts must be 1`);
            // t.equal(batchState.alls.Task2.error, processAllError, `Batch Task2 error must be ${processAllError}`);
            t.ok(msg0State.alls.Task2.error.message.startsWith(processAllError.message), `Batch Task2 error message must start with ${processAllError.message}`);

            t.equal(batch.states.get(batch).alls.Task2.attempts, 1, `Batch Task2 attempts must be 1`);

            checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
            checkBatchProcessingTaskStates(t, batchState, [false, true, false, true], [Failed, Completed, Failed, Completed]);
            checkBatchFinalisingTaskStates(t, batchState, [false, true, true, true], [Failed, Completed, Completed, Completed]);

            t.end();
          }
        )
        .catch(err => {
          t.end(err);
        });

    } catch
      (err) {
      t.fail(`processStreamEvent should NOT have failed on try-catch (${err})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with failing processAll message(s) - must NOT replay if maxNumberOfAttempts is reached
// =====================================================================================================================

test('processStreamEvent with 1 message that fails its processAll task must NOT replay if maxNumberOfAttempts is reached, ', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000010';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 0, `item.messageStates.length must be 0`);
    t.equal(item.rejectedMessageStates.length, 1, `item.rejectedMessageStates.length must be 1`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const event = generateEvent(10, n, region, {unusables: u});

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const processAllError = new Error('Failing process all task');
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, processAllError));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);
      context.streamProcessing.maxNumberOfAttempts = 1; // Extreme case of ONLY 1 attempt allowed

      const getResult = undefined;
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve with batch (${batch.shardOrEventID})`);
          const messages = batch.messages;
          const rejectedMessages = batch.rejectedMessages;
          const batchState = batch.states.get(batch);

          t.equal(messages.length, 0, `processStreamEvent batch must have ${0} messages`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 1, `processStreamEvent batch must have ${1} rejected message`);
          // t.equal(batch.undiscardedRejectedMessages.length, 1, `processStreamEvent batch must have ${1} undiscarded rejected message`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected message`);
          t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

          checkMessagesTasksStates(t, batch, rejectedMessages, Completed, Discarded, context);
          const msg0State = batch.states.get(rejectedMessages[0]);
          t.equal(msg0State.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          t.equal(msg0State.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          checkBatchTaskStates(t, batchState, 'alls', 'Task2', [''], [true], [Discarded]);
          t.equal(batchState.alls.Task2.attempts, 1, `Batch Task2 attempts must be 1`);
          t.equal(batchState.alls.Task2.error, undefined, `Batch Task2 error must be ${undefined}`);
          // t.equal(batchState.alls.Task2.error, processAllError, `Batch Task2 error must be ${processAllError}`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [false, true, false, true], [Failed, Completed, Failed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent must NOT reject with error (${err})`);
          t.end();
        })
        .catch(err => {
          t.end(err);
        });

    } catch
      (err) {
      t.fail(`processStreamEvent should NOT have failed on try-catch (${err})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that fails its processAll task, must replay if Kinesis resubmit is disabled', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000011';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 1, `item.messageStates.length must be 1`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = undefined;
    const event = generateEvent(11, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const processAllError = new Error('Failing process all task');
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, processAllError));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);
      const kinesisError = new Error('Disabling Kinesis');

      const getResult = undefined;
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, kinesisError, 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.fail(`processStreamEvent must NOT resolve with batch (${batch.shardOrEventID}) ${stringify(batch)}`);
          t.end();
        })
        .catch(err => {
            t.pass(`processStreamEvent must reject with error (${err})`);
            // t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`); // TODO NB when !sequencingRequired then perhaps the fatalError?
            // t.ok(err.message.startsWith(incompletePrefix), `processStreamEvent error must start with '${incompletePrefix}'`);
            t.ok(err.message.startsWith(processAllError.message), `processStreamEvent error must start with '${processAllError.message}'`);
            // t.equal(err, processAllError, `processStreamEvent error must be ${processAllError}`);

            const batch = err.batch;
            const messages = batch.messages;
            const batchState = batch.states.get(batch);

            t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
            t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
            t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
            t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
            t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
            t.equal(batch.incompleteMessages.length, 1, `processStreamEvent batch must have ${1} incomplete message`);

            checkMessagesTasksStates(t, batch, messages, Completed, Failed);
            const msg0State = batch.states.get(messages[0]);
            t.equal(msg0State.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
            t.equal(msg0State.alls.Task2.attempts, 1, `Task2 attempts must be 1`);
            // t.equal(msg0State.alls.Task2.error, processAllError, `Task2 error must be ${processAllError}`);
            t.ok(msg0State.alls.Task2.error.message.startsWith(processAllError.message), `Task2 error must start with ${processAllError.message}`);

            checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
            checkBatchProcessingTaskStates(t, batchState, [false, true, false, true], [Failed, Completed, Failed, Completed]);
            checkBatchFinalisingTaskStates(t, batchState, [false, true, true, true], [Failed, Completed, Completed, Completed]);

            t.end();
          }
        )
        .catch(err => {
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with successful message(s) with inactive tasks, must discard abandoned message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that succeeds, but has 1 abandoned task - must discard rejected message', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000012';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 0, `item.messageStates.length must be 0`);
    t.equal(item.rejectedMessageStates.length, 1, `item.rejectedMessageStates.length must be 1`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = '8425bb79169cfe3977ea6eb1fb0ecd66';
    const event = generateEvent(12, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    // const taskDefX = TaskDef.defineTask('TaskX', execute1);
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = []; //[taskDefX];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);

      const getResult = copy(require('./batch-1-task-x1-state-1.json'), deep);
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve with batch (${batch.shardOrEventID})`);
          const messages = batch.messages;
          const rejectedMessages = batch.rejectedMessages;
          const states = batch.states;
          const batchState = states.get(batch);

          t.equal(messages.length, 0, `processStreamEvent batch must have ${0} messages`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 1, `processStreamEvent batch must have ${1} rejected message`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected message`);
          t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

          checkMessagesTasksStates(t, batch, rejectedMessages, Abandoned, Completed);
          const msgState = states.get(rejectedMessages[0]);
          t.equal(msgState.ones.TaskX.attempts, 100, `TaskX attempts must be 100`);
          t.equal(msgState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          checkBatchTaskStates(t, batchState, 'alls', 'Task2', [''], [true], [Completed]);
          t.equal(batchState.alls.Task2.attempts, 1, `Batch Task2 attempts must be 1`);
          t.equal(batchState.alls.Task2.error, undefined, `Batch Task2 error must be ${undefined}`);
          // t.equal(batchState.alls.Task2.error, processAllError, `Batch Task2 error must be ${processAllError}`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent must NOT reject with error (${err})`);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that succeeds, but has 1 abandoned task - must fail if cannot discard rejected message', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000012';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 0, `item.messageStates.length must be 0`);
    t.equal(item.rejectedMessageStates.length, 1, `item.rejectedMessageStates.length must be 1`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = '8425bb79169cfe3977ea6eb1fb0ecd66';
    const event = generateEvent(12, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    //const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined)); //.then(msg => taskUtils.getTask(batch.states.get(msg).ones, 'Task1')
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = []; //[taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);

      const kinesisError = new Error('Disabling Kinesis');
      const getResult = copy(require('./batch-1-task-x2-state-1.json'), deep);
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, kinesisError, 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.fail(`processStreamEvent must NOT resolve with batch (${batch.shardOrEventID}) ${stringify(batch)}`);
          t.end();
        })
        .catch(err => {
            t.pass(`processStreamEvent must reject with error (${err})`);
            // t.equal(err, kinesisError, `processStreamEvent error must be ${kinesisError}`);
            t.ok(err.message.startsWith(kinesisError.message), `processStreamEvent error must start with ${kinesisError.message}`);
            // t.ok(err.message.startsWith(incompletePrefix), `processStreamEvent error must start with '${incompletePrefix}'`);

            const batch = err.batch;
            const messages = batch.messages;
            const rejectedMessages = batch.rejectedMessages;
            const batchState = batch.states.get(batch);

            t.equal(messages.length, 0, `processStreamEvent batch must have ${0} messages`);
            t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
            t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
            t.equal(batch.rejectedMessages.length, 1, `processStreamEvent batch must have ${1} rejected messages`);
            t.equal(batch.undiscardedRejectedMessages.length, 1, `processStreamEvent batch must have ${1} undiscarded rejected messages`);
            t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

            checkMessagesTasksStates(t, batch, rejectedMessages, Abandoned, Completed);
            const msg0State = batch.states.get(rejectedMessages[0]);
            t.equal(msg0State.ones.TaskX.attempts, 66, `TaskX attempts must be 66`);
            t.equal(msg0State.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

            checkBatchTaskStates(t, batchState, 'alls', 'Task2', [''], [true], [Completed]);
            t.equal(batchState.alls.Task2.attempts, 1, `Batch Task2 attempts must be 1`);
            t.equal(batchState.alls.Task2.error, undefined, `Batch Task2 error must be ${undefined}`);
            // t.equal(batchState.alls.Task2.error, processAllError, `Batch Task2 error must be ${processAllError}`);

            checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
            checkBatchProcessingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);
            checkBatchFinalisingTaskStates(t, batchState, [false, false, true, true], [Failed, Failed, Completed, Completed]);

            t.end();
          }
        )
        .catch(err => {
          t.end(err);
        });

    } catch
      (err) {
      t.fail(`processStreamEvent should NOT have failed on try-catch (${err})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 0 messages, 1 rejected message with 1 abandoned task - must discard previously undiscarded rejected message', t => {
  function validatePutParams(t, params) {
    t.fail(`processStreamEvent must NOT call DynamoDB put with ${stringify(params)}`);
  }

  function validateUpdateParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000012';
    const eav = params.ExpressionAttributeValues;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(params.Key.streamConsumerId, expectedStreamConsumerId, `params.Key.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(params.Key.shardOrEventID, expectedShardOrEventId, `params.Key.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(eav[':messageStates'].length, 0, `:messageStates.length must be 0`);
    t.equal(eav[':rejectedMessageStates'].length, 1, `:rejectedMessageStates.length must be 1`);
    t.equal(eav[':unusableRecordStates'].length, 0, `:unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = '8425bb79169cfe3977ea6eb1fb0ecd66';
    const event = generateEvent(12, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    // const taskDefX = TaskDef.defineTask('TaskX', execute1);
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = []; //[taskDefX];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);

      const getResult = copy(require('./batch-1-task-x2-state-2.json'), deep);
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams},
        update: {result: putResult, validateArgs: validateUpdateParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve with batch (${batch.shardOrEventID})`);
          const messages = batch.messages;
          const rejectedMessages = batch.rejectedMessages;
          const states = batch.states;
          const batchState = states.get(batch);

          t.equal(messages.length, 0, `processStreamEvent batch must have ${0} messages`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 1, `processStreamEvent batch must have ${1} rejected message`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected message`);
          t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

          checkMessagesTasksStates(t, batch, rejectedMessages, Abandoned, Completed);
          const msgState = states.get(rejectedMessages[0]);
          t.equal(msgState.ones.TaskX.attempts, 66, `TaskX attempts must be 66`);
          t.equal(msgState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.equal(batchState.alls, undefined, `Batch alls must be undefined`);
          // checkBatchTaskStates(t, batchState, 'alls', 'Task2', [''], [true], [Completed]);
          // t.equal(batchState.alls.Task2.attempts, 1, `Batch Task2 attempts must be 1`);
          // t.equal(batchState.alls.Task2.error, undefined, `Batch Task2 error must be ${undefined}`);
          // t.equal(batchState.alls.Task2.error, processAllError, `Batch Task2 error must be ${processAllError}`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent must NOT reject with error (${err})`);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with rejecting message(s), must discard rejected message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that rejects - must discard rejected message', t => {
  function validateUpdateParams(t, params) {
    t.fail(`processStreamEvent must NOT call DynamoDB update with ${stringify(params)}`);
  }

  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000013';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 0, `item.messageStates.length must be 0`);
    t.equal(item.rejectedMessageStates.length, 1, `item.rejectedMessageStates.length must be 1`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = '9425bb79169cfe3977ea6eb1fb0ecd66';
    const event = generateEvent(13, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const rejectError = new Error('Rejecting message');
    const executeOneAsync = sampleExecuteOneAsync(5, undefined, (msg, batch, context) => {
      // trigger a rejection from inside
      context.log(`*** Triggering an internal reject`);
      batch.states.get(msg).ones.Task1.reject('Forcing reject', rejectError, true);
    });
    const taskDef1 = TaskDef.defineTask('Task1', executeOneAsync);
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);

      const getResult = undefined;
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams},
        update: {result: undefined, validateArgs: validateUpdateParams}
      });

      // Process the event
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve with batch (${batch.shardOrEventID})`);
          const messages = batch.messages;
          const rejectedMessages = batch.rejectedMessages;
          const states = batch.states;
          const batchState = states.get(batch);

          t.equal(messages.length, 0, `processStreamEvent batch must have ${0} messages`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 1, `processStreamEvent batch must have ${1} rejected message`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected message`);
          t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

          checkMessagesTasksStates(t, batch, rejectedMessages, Rejected, Completed, context);
          const msgState = states.get(rejectedMessages[0]);
          t.equal(msgState.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          t.equal(msgState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          checkBatchTaskStates(t, batchState, 'alls', 'Task2', [''], [true], [Completed]);
          t.equal(batchState.alls.Task2.attempts, 1, `Batch Task2 attempts must be 1`);
          t.equal(batchState.alls.Task2.error, undefined, `Batch Task2 error must be ${undefined}`);
          // t.equal(batchState.alls.Task2.error, processAllError, `Batch Task2 error must be ${processAllError}`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent must NOT reject with error (${err})`);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }
  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that rejects, but cannot discard rejected message must fail', t => {
  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000014';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 0, `item.messageStates.length must be 0`);
    t.equal(item.rejectedMessageStates.length, 1, `item.rejectedMessageStates.length must be 1`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = '9025bb79169cfe3977ea6eb1fb0ecd66';
    const event = generateEvent(14, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const rejectError = new Error('Rejecting message');
    const executeOneAsync = sampleExecuteOneAsync(5, undefined, (msg, batch, context) => {
      // trigger a rejection from inside
      context.log(`*** Triggering an internal reject`);
      batch.states.get(msg).ones.Task1.reject('Forcing reject', rejectError, true);
    });
    const taskDef1 = TaskDef.defineTask('Task1', executeOneAsync);
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);

      const kinesisError = new Error('Disabling Kinesis');
      const getResult = undefined;
      const putResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, kinesisError, 5, {
        get: {result: getResult},
        put: {result: putResult, validateArgs: validatePutParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.fail(`processStreamEvent must NOT resolve with batch (${batch.shardOrEventID}) ${stringify(batch)}`);
          t.end();
        })
        .catch(err => {
            t.pass(`processStreamEvent must reject with error (${err})`);
            // t.equal(err, kinesisError, `processStreamEvent error must be ${kinesisError}`);
            t.ok(err.message.startsWith(kinesisError.message), `processStreamEvent error must start with ${kinesisError.message}`);
            // t.ok(err.message.startsWith(incompletePrefix), `processStreamEvent error must start with '${incompletePrefix}'`);

            const batch = err.batch;
            const messages = batch.messages;
            const rejectedMessages = batch.rejectedMessages;
            const batchState = batch.states.get(batch);

            t.equal(messages.length, 0, `processStreamEvent batch must have ${0} messages`);
            t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
            t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
            t.equal(batch.rejectedMessages.length, 1, `processStreamEvent batch must have ${1} rejected messages`);
            t.equal(batch.undiscardedRejectedMessages.length, 1, `processStreamEvent batch must have ${1} undiscarded rejected messages`);
            t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

            checkMessagesTasksStates(t, batch, rejectedMessages, Rejected, Completed);
            const msg0State = batch.states.get(rejectedMessages[0]);
            t.equal(msg0State.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
            t.equal(msg0State.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

            checkBatchTaskStates(t, batchState, 'alls', 'Task2', [''], [true], [Completed]);
            t.equal(batchState.alls.Task2.attempts, 1, `Batch Task2 attempts must be 1`);
            t.equal(batchState.alls.Task2.error, undefined, `Batch Task2 error must be ${undefined}`);
            // t.equal(batchState.alls.Task2.error, processAllError, `Batch Task2 error must be ${processAllError}`);

            checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
            checkBatchProcessingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);
            checkBatchFinalisingTaskStates(t, batchState, [false, false, true, true], [Failed, Failed, Completed, Completed]);

            t.end();
          }
        )
        .catch(err => {
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with message(s) exceeding max number of attempts on all tasks, must discard Discarded message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that exceeds max number of attempts on all its tasks - must discard Discarded message', t => {
  function validatePutParams(t, params) {
    t.fail(`processStreamEvent must NOT call DynamoDB put with ${stringify(params)}`);
  }

  function validateUpdateParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000013';
    const eav = params.ExpressionAttributeValues;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(params.Key.streamConsumerId, expectedStreamConsumerId, `params.Key.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(params.Key.shardOrEventID, expectedShardOrEventId, `params.Key.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(eav[':messageStates'].length, 0, `:messageStates.length must be 0`);
    t.equal(eav[':rejectedMessageStates'].length, 1, `:rejectedMessageStates.length must be 1`);
    t.equal(eav[':unusableRecordStates'].length, 0, `:unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = '9425bb79169cfe3977ea6eb1fb0ecd66';
    const event = generateEvent(13, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, new Error('Previously failed Task1')));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, new Error('Previously failed Task2')));
    // const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, new Error('Final failure')));
    // const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, new Error('Final failure')));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);
      context.streamProcessing.maxNumberOfAttempts = 7;
      const maxNumberOfAttempts = Settings.getMaxNumberOfAttempts(context);
      t.equal(maxNumberOfAttempts, 7, `maxNumberOfAttempts must be ${7}`);

      // Simulate a previous run, which has taken both tasks to 6 (i.e. one before max)
      const getResult = copy(require('./batch-1-tasks-failed-state-1.json'), deep);
      const updateResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, 5, {
        get: {result: getResult},
        put: {result: undefined, validateArgs: validatePutParams},
        update: {result: updateResult, validateArgs: validateUpdateParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const messages = batch.messages;
          t.equal(messages.length, 0, `processStreamEvent batch must have ${0} messages`);
          t.equal(batch.firstMessagesToProcess.length, 0, `processStreamEvent batch must have ${0} first messages to process`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 1, `processStreamEvent batch must have ${1} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
          t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

          checkMessagesTasksStates(t, batch, messages, Discarded, Discarded, context);
          for (let i = 0; i < messages.length; ++i) {
            const messageState = batch.states.get(messages[i]);
            t.equal(messageState.ones.Task1.attempts, 7, `message [${i}] (${messageState.id}) Task1 attempts must be 7`);
            t.equal(messageState.alls.Task2.attempts, 7, `message [${i}] (${messageState.id}) Task2 attempts must be 7`);
          }

          const batchState = batch.states.get(batch);
          t.equal(batchState.alls.Task2.attempts, 7, `Task2 attempts must be 7`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [false, false, false, true], [Failed, Failed, Failed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [true, true, true, true], [Completed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});


test('processStreamEvent with 1 message that exceeds max number of attempts on all its tasks, but cannot discard message must fail', t => {
  function validatePutParams(t, params) {
    t.fail(`processStreamEvent must NOT call DynamoDB put with ${stringify(params)}`);
  }

  function validateUpdateParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000013';
    const eav = params.ExpressionAttributeValues;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(params.Key.streamConsumerId, expectedStreamConsumerId, `params.Key.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(params.Key.shardOrEventID, expectedShardOrEventId, `params.Key.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(eav[':messageStates'].length, 0, `:messageStates.length must be 0`);
    t.equal(eav[':rejectedMessageStates'].length, 1, `:rejectedMessageStates.length must be 1`);
    t.equal(eav[':unusableRecordStates'].length, 0, `:unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = '9425bb79169cfe3977ea6eb1fb0ecd66';
    const event = generateEvent(13, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, new Error('Previously failed Task1')));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, new Error('Previously failed Task2')));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);
      context.streamProcessing.maxNumberOfAttempts = 7;
      const maxNumberOfAttempts = Settings.getMaxNumberOfAttempts(context);
      t.equal(maxNumberOfAttempts, 7, `maxNumberOfAttempts must be ${7}`);

      // Simulate a previous run, which has taken both tasks to 6 (i.e. one before max)
      const getResult = copy(require('./batch-1-tasks-failed-state-1.json'), deep);
      const updateResult = {};
      const kinesisError = new Error('Disabling Kinesis');
      configureKinesisAndDynamoDBDocClient(t, context, prefix, kinesisError, 5, {
        get: {result: getResult},
        put: {result: undefined, validateArgs: validatePutParams},
        update: {result: updateResult, validateArgs: validateUpdateParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.fail(`processStreamEvent must NOT resolve with batch (${batch.shardOrEventID}) ${stringify(batch)}`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${err})`);
          t.ok(err.message.startsWith(kinesisError.message), `processStreamEvent error must start with ${kinesisError.message}`);

          const batch = err.batch;
          const messages = batch.messages;
          const rejectedMessages = batch.rejectedMessages;
          const batchState = batch.states.get(batch);

          t.equal(messages.length, 0, `processStreamEvent batch must have ${0} messages`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 1, `processStreamEvent batch must have ${1} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 1, `processStreamEvent batch must have ${1} undiscarded rejected messages`);
          t.equal(batch.incompleteMessages.length, 0, `processStreamEvent batch must have ${0} incomplete messages`);

          checkMessagesTasksStates(t, batch, rejectedMessages, Discarded, Discarded);
          const msg0State = batch.states.get(rejectedMessages[0]);
          t.equal(msg0State.ones.Task1.attempts, 7, `Task1 attempts must be 7`);
          t.equal(msg0State.alls.Task2.attempts, 7, `Task2 attempts must be 7`);

          checkBatchTaskStates(t, batchState, 'alls', 'Task2', [''], [true], [Discarded]);
          t.equal(batchState.alls.Task2.attempts, 7, `Batch Task2 attempts must be 7`);
          t.equal(batchState.alls.Task2.error, undefined, `Batch Task2 error must be ${undefined}`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [false, false, false, true], [Failed, Failed, Failed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [false, false, true, true], [Failed, Failed, Completed, Completed]);

          t.end();
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that only exceeds max number of attempts on 1 of its 2 its tasks, must NOT discard message yet', t => {
  function validatePutParams(t, params) {
    t.fail(`processStreamEvent must NOT call DynamoDB put with ${stringify(params)}`);
  }

  function validateUpdateParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000013';
    const eav = params.ExpressionAttributeValues;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(params.Key.streamConsumerId, expectedStreamConsumerId, `params.Key.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(params.Key.shardOrEventID, expectedShardOrEventId, `params.Key.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(eav[':messageStates'].length, 1, `:messageStates.length must be 1`);
    t.equal(eav[':rejectedMessageStates'].length, 0, `:rejectedMessageStates.length must be 0`);
    t.equal(eav[':unusableRecordStates'].length, 0, `:unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = '9425bb79169cfe3977ea6eb1fb0ecd66';
    const event = generateEvent(13, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, new Error('Previously failed Task1')));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, new Error('Previously failed Task2')));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);
      context.streamProcessing.maxNumberOfAttempts = 8;
      const maxNumberOfAttempts = Settings.getMaxNumberOfAttempts(context);
      t.equal(maxNumberOfAttempts, 8, `maxNumberOfAttempts must be ${8}`);

      // Simulate a previous run, which has taken both tasks to 6 (i.e. one before max)
      const getResult = copy(require('./batch-1-tasks-failed-state-2.json'), deep);
      const updateResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, 5, {
        get: {result: getResult},
        put: {result: undefined, validateArgs: validatePutParams},
        update: {result: updateResult, validateArgs: validateUpdateParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.fail(`processStreamEvent must NOT resolve with batch (${batch.shardOrEventID}) ${stringify(batch)}`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${err})`);
          const expectedErrorMsg = 'Failing process one task (1)';
          t.ok(err.message.startsWith(expectedErrorMsg), `processStreamEvent error must start with ${expectedErrorMsg}`);

          const batch = err.batch;
          const messages = batch.messages;
          // const rejectedMessages = batch.rejectedMessages;
          const batchState = batch.states.get(batch);

          t.equal(messages.length, 1, `processStreamEvent batch must have ${1} messages`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
          t.equal(batch.incompleteMessages.length, 1, `processStreamEvent batch must have ${1} incomplete messages`);

          checkMessagesTasksStates(t, batch, messages, Failed, Discarded);
          const msg0State = batch.states.get(messages[0]);
          t.equal(msg0State.ones.Task1.attempts, 7, `Task1 attempts must be 7`);
          t.equal(msg0State.alls.Task2.attempts, 8, `Task2 attempts must be 8`);

          checkBatchTaskStates(t, batchState, 'alls', 'Task2', [''], [true], [Discarded]);
          t.equal(batchState.alls.Task2.attempts, 8, `Batch Task2 attempts must be 8`);
          t.equal(batchState.alls.Task2.error, undefined, `Batch Task2 error must be ${undefined}`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [false, false, false, true], [Failed, Failed, Failed, Completed]);
          checkBatchFinalisingTaskStates(t, batchState, [false, true, true, true], [Failed, Completed, Completed, Completed]);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with 1 message and triggered processing timeout promise, must resubmit incomplete message
// =====================================================================================================================

test('processStreamEvent with 1 message and triggered processing timeout promise, must resubmit incomplete message', t => {
  function validateUpdateParams(t, params) {
    t.fail(`processStreamEvent must NOT call DynamoDB update with ${stringify(params)}`);
  }

  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000016';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 1, `item.messageStates.length must be 1`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = '1025bb79169cfe3977ea6eb1fb0ecd66';
    const event = generateEvent(16, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 300;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(15, new Error('Previously failed Task1')));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(15, new Error('Previously failed Task2')));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);

      // Force a processing phase timeout at 1% of 300 ms, i.e. after 3 ms, i.e. before process one & all tasks can complete
      context.streamProcessing.timeoutAtPercentageOfRemainingTime = 0.01;

      // Simulate a previous run, which has taken both tasks to 6 (i.e. one before max)
      const getResult = undefined; //copy(require('./batch-1-tasks-failed-state-3.json'), deep);
      const updateResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, 5, {
        get: {result: getResult},
        put: {result: undefined, validateArgs: validatePutParams},
        update: {result: updateResult, validateArgs: validateUpdateParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.fail(`processStreamEvent must NOT resolve with batch (${batch.shardOrEventID}) ${stringify(batch)}`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${err})`);

          const batch = err.batch;
          const messages = batch.messages;
          const batchState = batch.states.get(batch);

          t.equal(messages.length, 1, `processStreamEvent batch must have ${1} messages`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
          t.equal(batch.incompleteMessages.length, 1, `processStreamEvent batch must have ${1} incomplete messages`);

          t.ok(err instanceof TimeoutError, `processStreamEvent error must be an instance of TimeoutError`);
          const expectedErrorMsg = 'Ran out of time to complete processBatch';
          const expectedErrorMsgPlanB = 'Ran out of time to complete finaliseBatch';

          const msgState = batch.states.get(messages[0]);

          const msgTask1 = msgState.ones.Task1;
          const msgTask2 = msgState.alls.Task2;
          const batchTask2 = batchState.alls.Task2;

          const batchTask2ErrorMsg = batchTask2.error.message;

          checkMessagesTasksStates(t, batch, messages, TimedOut, TimedOut);
          checkBatchTaskStates(t, batchState, 'alls', 'Task2', [''], [false], [TimedOut]);

          // All attempts MUST be reset back to zero, since timed out
          t.equal(msgTask1.attempts, 0, `Task1 attempts must be 0`);
          t.equal(msgTask2.attempts, 0, `Task2 attempts must be 0`);
          t.equal(batchTask2.attempts, 0, `Batch Task2 attempts must be 0`);

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);
          checkBatchProcessingTaskStates(t, batchState, [false, false, false, true], [TimedOut, TimedOut, TimedOut, Completed]);

          if (batchState.finalising.finaliseBatch.state.type !== StateType.TimedOut) {
            t.ok(err.message.startsWith(expectedErrorMsg), `processStreamEvent error (${err}) must start with ${expectedErrorMsg}`);

            t.ok(batchTask2ErrorMsg.startsWith(expectedErrorMsg),
              `Batch Task2 error (${batchTask2.error}) must start with ${expectedErrorMsg}`);

            checkBatchFinalisingTaskStates(t, batchState, [false, true, true, true], [Failed, Completed, Completed, Completed]);

            t.end();

          } else {
            // Sometimes due to timing differences, the finaliseBatch will also time out regardless of the settings above
            t.ok(err.message.startsWith(expectedErrorMsg) || err.message.startsWith(expectedErrorMsgPlanB),
              `*** PLAN B ***: processStreamEvent error (${err}) must start with ${expectedErrorMsg} or ${expectedErrorMsgPlanB}`);

            t.ok(batchTask2ErrorMsg.startsWith(expectedErrorMsg) || batchTask2ErrorMsg.startsWith(expectedErrorMsgPlanB),
              `*** PLAN B ***: Batch Task2 error (${batchTask2.error}) must start with ${expectedErrorMsg} or ${expectedErrorMsgPlanB}`);

            checkBatchFinalisingTaskStates(t, batchState, [[false, false], [true, false], [true, false], [true, false]], [[Failed, TimedOut], [Completed, TimedOut], [Completed, TimedOut], [Completed, TimedOut]]);

            t.end();
          }
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with 1 message and triggered finalising timeout promise, must resubmit incomplete message
// =====================================================================================================================

test('processStreamEvent with 1 message and triggered finalising timeout promise, must resubmit incomplete message', t => {
  function validateUpdateParams(t, params) {
    t.fail(`processStreamEvent must NOT call DynamoDB update with ${stringify(params)}`);
  }

  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|TEST_Stream_DEV|sampleFunctionName:dev';
    const expectedShardOrEventId = 'S|shardId-000000000017';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 1, `item.messageStates.length must be 1`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;
    const u = 0;

    // Generate a sample AWS event
    const partitionKey = '1025bb79169cfe3977ea6eb1fb0ecd66';
    const event = generateEvent(17, n, region, {unusables: u}, partitionKey);

    // Generate a sample AWS context
    const maxTimeInMillis = 40;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, new Error('Previously failed Task1')));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, new Error('Previously failed Task2')));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);

      // The processing phase should complete at a bit after 20 ms (10 ms to "load" + 10 ms to run tasks), but if not
      // force a processing timeout at 99% of 40 ms (i.e. at ~39 ms), which only leaves between 1 and 20 ms for the
      // finalising phase, which includes a 10 ms "save", to wrap up, which seems to be insufficient enough to always
      // trigger a time out in the finalising phase
      context.streamProcessing.timeoutAtPercentageOfRemainingTime = 0.99;

      // Simulate a previous run, which has taken both tasks to 6 (i.e. one before max)
      const getResult = undefined;
      const updateResult = {};
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, 20, {
        get: {result: getResult},
        put: {result: undefined, validateArgs: validatePutParams},
        update: {result: updateResult, validateArgs: validateUpdateParams}
      });

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.fail(`processStreamEvent must NOT resolve with batch (${batch.shardOrEventID}) ${stringify(batch)}`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${err})`);

          const batch = err.batch;
          const messages = batch.messages;
          const batchState = batch.states.get(batch);

          t.equal(messages.length, 1, `processStreamEvent batch must have ${1} messages`);
          t.equal(batch.unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          t.equal(batch.undiscardedUnusableRecords.length, u, `processStreamEvent batch must have ${u} undiscarded unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);
          t.equal(batch.incompleteMessages.length, 1, `processStreamEvent batch must have ${1} incomplete messages`);

          t.ok(err instanceof TimeoutError, `processStreamEvent error must be an instance of TimeoutError`);
          const expectedErrorMsg = 'Ran out of time to complete finaliseBatch';
          const expectedErrorMsgPlanB = 'Ran out of time to complete processBatch';

          const msgState = batch.states.get(messages[0]);

          const msgTask1 = msgState.ones.Task1;
          const msgTask2 = msgState.alls.Task2;
          const batchTask2 = batchState.alls.Task2;

          const batchTask2ErrorMsg = batchTask2.error.message;

          checkBatchInitiatingTaskStates(t, batchState, [true, true, true, true, true], [Completed, Completed, Completed, Completed, Completed]);

          if (batchState.processing.processBatch.state.type !== StateType.TimedOut) {
            t.ok(err.message.startsWith(expectedErrorMsg), `processStreamEvent error must start with ${expectedErrorMsg}`);

            checkMessagesTasksStates(t, batch, messages, Failed, Failed);
            checkBatchTaskStates(t, batchState, 'alls', 'Task2', [''], [false], [Failed]);

            // All attempts MUST be ONE, because processing was frozen & cannot be reset back to zero by finalise timeout
            t.equal(msgTask1.attempts, 1, `Task1 attempts (${msgTask1.attempts}) must be 1`);
            t.equal(msgTask2.attempts, 1, `Task2 attempts (${msgTask2.attempts}) must be 1`);
            t.equal(batchTask2.attempts, 1, `Batch Task2 attempts (${batchTask2.attempts}) must be 1`);

            const expectedErrorMsg3 = 'Failing process all task (1)';
            t.ok(batchTask2ErrorMsg.startsWith(expectedErrorMsg3),
              `Batch Task2 error must start with ${expectedErrorMsg3}`);

            checkBatchProcessingTaskStates(t, batchState, [false, false, false, true], [Failed, Failed, Failed, Completed]);
            checkBatchFinalisingTaskStates(t, batchState, [false, true, false, false], [TimedOut, Completed, TimedOut, TimedOut]);

            t.end();

          } else {
            // Sometimes due to timing differences, the processBatch will also time out regardless of the settings above
            t.ok(err.message.startsWith(expectedErrorMsg) || err.message.startsWith(expectedErrorMsgPlanB),
              `*** PLAN B ***: processStreamEvent error (${err}) must start with ${expectedErrorMsg} or ${expectedErrorMsgPlanB}`);

            checkMessagesTasksStates(t, batch, messages, [TimedOut, Failed], [TimedOut, Failed]);
            checkBatchTaskStates(t, batchState, 'alls', 'Task2', [''], [false, false], [TimedOut, Failed]);

            if (msgTask1.state.type === StateType.Failed) {
              t.equal(msgTask1.attempts, 1, `*** PLAN B ***: Task1 attempts (${msgTask1.attempts}) must be 1`);
            } else {
              // All attempts will most likely be reset back to zero, since timed out
              t.ok(msgTask1.attempts === 0 || msgTask1.attempts === 1, `*** PLAN B ***: Task1 attempts (${msgTask1.attempts}) must be 0 or 1`);
            }

            if (msgTask2.state.type === StateType.Failed) {
              t.equal(msgTask2.attempts, 1, `*** PLAN B ***: Task2 attempts (${msgTask2.attempts}) must be 1`);
            } else {
              // All attempts will most likely be reset back to zero, since timed out
              t.ok(msgTask2.attempts === 0 || msgTask2.attempts === 1, `*** PLAN B ***: Task2 attempts (${msgTask2.attempts}) must be 0 or 1`);
            }

            if (batchTask2.state.type === StateType.Failed) {
              t.equal(batchTask2.attempts, 1, `*** PLAN B ***: Batch Task2 attempts (${batchTask2.attempts}) must be 1`);
            } else {
              t.ok(batchTask2.attempts === 0 || batchTask2.attempts === 1, `*** PLAN B ***: Batch Task2 attempts (${batchTask2.attempts}) must be 0 or 1`);
            }

            const expectedErrorMsg4 = 'Failing process all task (1)';
            t.ok(batchTask2ErrorMsg.startsWith(expectedErrorMsg4) || batchTask2ErrorMsg.startsWith(expectedErrorMsgPlanB),
              `*** PLAN B ***: Batch Task2 error (${batchTask2.error}) must start with ${expectedErrorMsg4} or ${expectedErrorMsgPlanB}`);

            checkBatchProcessingTaskStates(t, batchState, [[false, false], [false, false], [false, false], [true, false]], [[Failed, TimedOut], [Failed, TimedOut], [Failed, TimedOut], [Completed, TimedOut]]);
            checkBatchFinalisingTaskStates(t, batchState, [false, [true, false], [true, false], [true, false]], [TimedOut, [Completed, TimedOut], [Completed, TimedOut], [Completed, TimedOut]]);

            t.end();
          }
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});
