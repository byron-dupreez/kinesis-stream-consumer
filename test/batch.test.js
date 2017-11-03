'use strict';

/**
 * Unit tests for aws-stream-consumer-core/sequencing.js
 * @author Byron du Preez
 */

const test = require('tape');

const sequencingRequired = true;
const sequencingPerKey = false;

const awsRegion = 'us-west-2';

// The test subject
const Batch = require('aws-stream-consumer-core/batch');

const contexts = require('aws-core-utils/contexts');
const regions = require('aws-core-utils/regions');

const samples = require('./samples');
const sampleKinesisMessageAndRecord = samples.sampleKinesisMessageAndRecord;

const identify = require('../kinesis-identify');
const generateKinesisMD5s = identify.generateKinesisMD5s;
const resolveKinesisEventIdAndSeqNos = identify.resolveKinesisEventIdAndSeqNos;
const resolveKinesisMessageIdsAndSeqNos = identify.resolveKinesisMessageIdsAndSeqNos;

const tries = require('core-functions/tries');
const Success = tries.Success;

const strings = require('core-functions/strings');
const stringify = strings.stringify;

const taskUtils = require('task-utils');
const TaskDef = require('task-utils/task-defs');
const TaskFactory = require('task-utils/task-factory');
const Task = require('task-utils/tasks');
const taskStates = require('task-utils/task-states');
const ReturnMode = taskUtils.ReturnMode;

const eventSourceARN = samples.sampleKinesisEventSourceArn(awsRegion, 'TEST_Stream_DEV');

function noop() {
}

const processOpts = {returnMode: ReturnMode.PROMISE}; // was ReturnMode.SUCCESS_OR_FAILURE};

// Dummy process one & all functions
const processOne = (message, context) => message;
const processAll = (batch, context) => batch;

// Dummy discard unusable record function
const discardUnusableRecord = (unusableRecord, batch, context) => {
  const i = batch.unusableRecords.indexOf(unusableRecord);
  context.info(`Running discardUnusableRecord with unusable record[${i}]`);
  return Promise.resolve({index: i});
};

// Dummy discard rejected message function
const discardRejectedMessage = (rejectedMessage, batch, context) => {
  const i = batch.messages.indexOf(rejectedMessage);
  context.info(`Running discardRejectedMessage with message[${i}]`);
  return Promise.resolve({index: i});
};

function createContext(streamType, sequencingRequired, sequencingPerKey, idPropertyNames, keyPropertyNames, seqNoPropertyNames, event, awsContext) {
  // noinspection JSUnusedLocalSymbols
  const context = {
    streamProcessing: {
      streamType: streamType,
      sequencingRequired: !!sequencingRequired,
      sequencingPerKey: !!sequencingPerKey,
      consumerIdSuffix: undefined,
      idPropertyNames: idPropertyNames,
      keyPropertyNames: keyPropertyNames,
      seqNoPropertyNames: seqNoPropertyNames,
      generateMD5s: generateKinesisMD5s,
      resolveEventIdAndSeqNos: resolveKinesisEventIdAndSeqNos,
      resolveMessageIdsAndSeqNos: resolveKinesisMessageIdsAndSeqNos,
      discardUnusableRecord: discardUnusableRecord,
      discardRejectedMessage: discardRejectedMessage
    }
  };
  regions.setRegion(awsRegion);
  context.stage = 'dev';
  contexts.configureStandardContext(context, undefined, require('../default-kinesis-options.json'), event, awsContext, false);
  context.streamProcessing.consumerId = `my-function:${context.stage}`;
  context.taskFactory = new TaskFactory({logger: context, describeItem: undefined}, {returnMode: ReturnMode.NORMAL});
  return context;
}

// =====================================================================================================================
// new Batch
// =====================================================================================================================

test('new Batch & resolveBatchKey with no records', t => {
  const context = createContext('kinesis', sequencingRequired, sequencingPerKey, ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const batchKey = Batch.resolveBatchKey([], context);
  t.deepEqual(batchKey, undefined, `Batch.resolveBatchKey([rec1]) must be ${undefined}`);

  const batch = new Batch([], [], [], context);

  t.ok(batch, `batch must exist`);
  t.ok(batch.states, `batch.states must exist`);
  t.equal(batch.records.length, 0, `batch.records.length must be 0`);
  t.notOk(batch.isKeyValid(), `batch.isKeyValid() must be false`);
  t.equal(batch.key, undefined, `batch.key must be undefined`);
  t.equal(batch.keyString, Batch.MISSING_KEY_STRING, `batch.keyString must be ${Batch.MISSING_KEY_STRING}`);
  t.equal(batch.messages.length, 0, `batch.messages.length must be 0`);
  t.equal(batch.unusableRecords.length, 0, `batch.unusableRecords.length must be 0`);
  t.end();
});

test('new Batch & resolveBatchKey with 1 record', t => {
  const context = createContext('kinesis', sequencingRequired, sequencingPerKey, ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId1 = 'shardId-000000000000';
  const eventSeqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [, rec1] = sampleKinesisMessageAndRecord(shardId1, eventSeqNo1, eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);

  const expectedKey = {
    streamConsumerId: 'K|TEST_Stream_DEV|my-function:dev',
    shardOrEventID: 'S|shardId-000000000000'
  };

  const batchKey = Batch.resolveBatchKey([rec1], context);
  t.deepEqual(batchKey, expectedKey, `Batch.resolveBatchKey([rec1]) must be ${stringify(expectedKey)}`);

  const batch = new Batch([rec1], [], [], context);

  t.ok(batch, `batch must exist`);
  t.ok(batch.states, `batch.states must exist`);
  t.equal(batch.records.length, 1, `batch.records.length must be 1`);
  t.ok(batch.isKeyValid(), `batch.isKeyValid() must be true`);
  t.deepEqual(batch.key, expectedKey, `batch.key must be ${stringify(expectedKey)}`);
  const expectedKeyString = `(${expectedKey.streamConsumerId}, ${expectedKey.shardOrEventID})`;
  t.equal(batch.keyString, expectedKeyString, `batch.keyString must be ${expectedKeyString}`);
  t.equal(batch.messages.length, 0, `batch.messages.length must be 0`);
  t.equal(batch.unusableRecords.length, 0, `batch.unusableRecords.length must be 0`);

  const streamConsumerId = expectedKey.streamConsumerId;
  const streamName = streamConsumerId.substring(streamConsumerId.indexOf('|') + 1, streamConsumerId.lastIndexOf('|'));
  const consumerId = streamConsumerId.substring(streamConsumerId.lastIndexOf('|') + 1);
  const components = batch.key.components;
  t.equal(components.streamName, streamName, `components.streamName must be ${streamName}`);
  t.equal(components.consumerId, consumerId, `components.consumerId must be ${consumerId}`);
  t.equal(components.shardId, shardId1, `components.shardId must be ${shardId1}`);
  const eventID = `${shardId1}:${eventSeqNo1}`;
  t.equal(components.eventID, eventID, `components.eventID must be ${eventID}`);

  t.end();
});

test('new Batch & resolveBatchKey with 1 unusable record & 2 messages', t => {
  const context = createContext('kinesis', sequencingRequired, sequencingPerKey, ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);
  // First record is unusable
  const rec1 = {bad: 'apple'};

  const shardId2 = 'shardId-000000000002';
  const eventSeqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, eventSeqNo2, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  const shardId3 = 'shardId-000000000003';
  const eventSeqNo3 = '49545115243490985018280067714973144582180062593244200963';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, eventSeqNo3, eventSourceARN, '456', '789', 'DEF', 11, 4, 5, 6);

  const expectedKey = {
    streamConsumerId: 'K|TEST_Stream_DEV|my-function:dev',
    shardOrEventID: 'S|shardId-000000000002'
  };

  const batchKey1 = Batch.resolveBatchKey([rec1], context);
  t.deepEqual(batchKey1, undefined, `Batch.resolveBatchKey([rec1]) must be ${undefined}`);

  const batchKey2 = Batch.resolveBatchKey([rec2], context);
  t.deepEqual(batchKey2, expectedKey, `Batch.resolveBatchKey([rec2]) must be ${stringify(expectedKey)}`);

  const expectedKey3 = {
    streamConsumerId: 'K|TEST_Stream_DEV|my-function:dev',
    shardOrEventID: 'S|shardId-000000000003'
  };
  const batchKey3 = Batch.resolveBatchKey([rec3], context);
  t.deepEqual(batchKey3, expectedKey3, `Batch.resolveBatchKey([rec3]) must be ${stringify(expectedKey3)}`);

  const batch = new Batch([rec1, rec2, rec3], [], [], context);

  t.ok(batch, `batch must exist`);

  const states = batch.states;
  t.ok(batch.states, `batch.states must exist`);

  const uRec1 = batch.addUnusableRecord(rec1, undefined, 'Useless', context);
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  t.ok(states.get(uRec1), `states.get(uRec1) must exist`);
  t.ok(states.get(msg2), `states.get(msg2) must exist`);
  t.ok(states.get(msg3), `states.get(msg3) must exist`);

  t.equal(batch.records.length, 3, `batch.records.length must be 3`);
  t.ok(batch.isKeyValid(), `batch.isKeyValid() must be true`);
  t.deepEqual(batch.key, expectedKey, `batch.key must be ${stringify(expectedKey)}`);
  const expectedKeyString = `(${expectedKey.streamConsumerId}, ${expectedKey.shardOrEventID})`;
  t.equal(batch.keyString, expectedKeyString, `batch.keyString must be ${expectedKeyString}`);
  t.equal(batch.messages.length, 2, `batch.messages.length must be 2`);
  t.equal(batch.unusableRecords.length, 1, `batch.unusableRecords.length must be 1`);

  const streamConsumerId = expectedKey.streamConsumerId;
  const streamName = streamConsumerId.substring(streamConsumerId.indexOf('|') + 1, streamConsumerId.lastIndexOf('|'));
  const consumerId = streamConsumerId.substring(streamConsumerId.lastIndexOf('|') + 1);
  const components = batch.key.components;
  t.equal(components.streamName, streamName, `components.streamName must be ${streamName}`);
  t.equal(components.consumerId, consumerId, `components.consumerId must be ${consumerId}`);
  t.equal(components.shardId, shardId2, `components.shardId must be ${shardId2}`);
  const eventID2 = `${shardId2}:${eventSeqNo2}`;
  t.equal(components.eventID, eventID2, `components.eventID must be ${eventID2}`);

  t.end();
});

// =====================================================================================================================
// defineDiscardTasks
// =====================================================================================================================

test('defineDiscardTasks', t => {
  const context = createContext('kinesis', sequencingRequired, sequencingPerKey, ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);
  // First record is unusable
  const rec1 = {bad: 'apple'};

  const shardId2 = 'shardId-000000000002';
  const eventSeqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, eventSeqNo2, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  const shardId3 = 'shardId-000000000003';
  const eventSeqNo3 = '49545115243490985018280067714973144582180062593244200963';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, eventSeqNo3, eventSourceARN, '456', '789', 'DEF', 11, 4, 5, 6);

  const batch = new Batch([rec1, rec2, rec3], [], [], context);
  t.ok(batch, `batch must exist`);

  const states = batch.states;
  t.ok(batch.states, `batch.states must exist`);

  const uRec1 = batch.addUnusableRecord(rec1, undefined, 'Useless', context);
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  t.ok(states.get(uRec1), `states.get(uRec1) must exist`);
  t.ok(states.get(msg2), `states.get(msg2) must exist`);
  t.ok(states.get(msg3), `states.get(msg3) must exist`);

  t.equal(batch.records.length, 3, `batch.records.length must be 3`);
  const expectedKey = {
    streamConsumerId: 'K|TEST_Stream_DEV|my-function:dev',
    shardOrEventID: 'S|shardId-000000000002'
  };
  t.deepEqual(batch.key, expectedKey, `batch.key must be ${stringify(expectedKey)}`);
  const expectedKeyString = `(${expectedKey.streamConsumerId}, ${expectedKey.shardOrEventID})`;
  t.equal(batch.keyString, expectedKeyString, `batch.keyString must be ${expectedKeyString}`);
  t.equal(batch.messages.length, 2, `batch.messages.length must be 2`);
  t.equal(batch.unusableRecords.length, 1, `batch.unusableRecords.length must be 1`);

  // Clear out all of the discard functions from the context for testing
  context.streamProcessing.discardUnusableRecord = undefined;
  context.streamProcessing.discardRejectedMessage = undefined;

  // With no discard functions configured
  t.throws(() => batch.defineDiscardTasks(context), Error, `batch.taskDefs.defineDiscardTasks without any discard functions must throw an error`);

  // Define discard one unusable record task def only
  context.streamProcessing.discardUnusableRecord = noop;
  context.streamProcessing.discardRejectedMessage = undefined;

  t.throws(() => batch.defineDiscardTasks(context), Error, `batch.taskDefs.defineDiscardTasks with only discard one unusable record task def must throw an error`);

  // Define discard one rejected message task def only
  context.streamProcessing.discardUnusableRecord = undefined;
  context.streamProcessing.discardRejectedMessage = noop;

  t.throws(() => batch.defineDiscardTasks(context), Error, `batch.taskDefs.defineDiscardTasks with only discard one rejected message task def must throw an error`);

  // Define discard one unusable record task def & discard one rejected message task def
  context.streamProcessing.discardUnusableRecord = noop;
  context.streamProcessing.discardRejectedMessage = noop;

  batch.defineDiscardTasks(context);
  const taskDefs = batch.taskDefs;

  t.ok(taskDefs.discardUnusableRecordTaskDef, `batch.taskDefs.discardUnusableRecordTaskDef must be defined`);
  t.ok(taskDefs.discardRejectedMessageTaskDef, `batch.taskDefs.discardRejectedMessageTaskDef must be defined`);

  t.end();
});

// =====================================================================================================================
// reviveTasks with 2 messages and 1 unusable record
// =====================================================================================================================

test(`reviveTasks with 2 messages and 1 unusable record AND sequencingRequired ${sequencingRequired}`, t => {
  const context = createContext('kinesis', sequencingRequired, sequencingPerKey, ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  // First record is unusable
  const rec1 = {bad: 'apple'};

  const shardId2 = 'shardId-000000000002';
  const eventSeqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, eventSeqNo2, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  const shardId3 = 'shardId-000000000003';
  const eventSeqNo3 = '49545115243490985018280067714973144582180062593244200963';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, eventSeqNo3, eventSourceARN, '456', '789', 'DEF', 11, 4, 5, 6);

  const processOneTaskDef = TaskDef.defineTask('processOne', processOne);
  const processAllTaskDef = TaskDef.defineTask('processAll', processAll);

  const batch0 = new Batch([rec1, rec2, rec3], [processOneTaskDef], [processAllTaskDef], context);
  const states0 = batch0.states;
  const uRec1 = batch0.addUnusableRecord(rec1, undefined, 'Useless', context);
  batch0.addMessage(msg2, rec2, undefined, context);
  batch0.addMessage(msg3, rec3, undefined, context);

  batch0.reviveTasks(context);

  // console.log(`**** SEQ INIT Batch = ${stringify(batch0)}`);

  t.ok(states0.get(uRec1).discards.discardUnusableRecord instanceof Task, `states0.get(uRec1).discards.discardUnusableRecord must be an instanceof Task`);
  t.ok(states0.get(msg2).ones.processOne instanceof Task, `states0.get(msg2).ones.processOne must be an instanceof Task`);
  t.ok(states0.get(msg3).ones.processOne instanceof Task, `states0.get(msg3).ones.processOne must be an instanceof Task`);

  t.ok(states0.get(batch0).alls.processAll instanceof Task, `states0.get(batch0).alls.processAll must be an instanceof Task`);
  t.ok(states0.get(msg2).alls.processAll instanceof Task, `states0.get(msg2).alls.processAll must be an instanceof Task`);
  t.ok(states0.get(msg3).alls.processAll instanceof Task, `states0.get(msg3).alls.processAll must be an instanceof Task`);

  t.ok(typeof states0.get(uRec1).discards.discardUnusableRecord.execute === 'function', `states0.get(uRec1).discards.discardUnusableRecord.execute must be a function`);
  t.ok(typeof states0.get(msg2).ones.processOne.execute === 'function', `states0.get(msg2).ones.processOne.execute must be a function`);
  t.ok(typeof states0.get(msg3).ones.processOne.execute === 'function', `states0.get(msg3).ones.processOne.execute must be a function`);

  t.ok(typeof states0.get(batch0).alls.processAll.execute === 'function', `states0.get(batch0).alls.processAll.execute must be a function`);
  t.ok(typeof states0.get(msg2).alls.processAll.execute === 'function', `states0.get(msg2).alls.processAll.execute must be a function`);
  t.ok(typeof states0.get(msg3).alls.processAll.execute === 'function', `states0.get(msg3).alls.processAll.execute must be a function`);

  t.equal(states0.get(uRec1).discards.discardUnusableRecord.attempts, 0, `states0.get(uRec1).discards.discardUnusableRecord.attempts must be 0`);
  t.equal(states0.get(msg2).ones.processOne.attempts, 0, `states0.get(msg2).ones.processOne.attempts must be 0`);
  t.equal(states0.get(msg3).ones.processOne.attempts, 0, `states0.get(msg3).ones.processOne.attempts must be 0`);

  t.equal(states0.get(batch0).alls.processAll.attempts, 0, `states0.get(batch0).alls.processAll.attempts must be 0`);
  t.equal(states0.get(msg2).alls.processAll.attempts, 0, `states0.get(msg2).alls.processAll.attempts must be 0`);
  t.equal(states0.get(msg3).alls.processAll.attempts, 0, `states0.get(msg3).alls.processAll.attempts must be 0`);

  let expectedState = taskStates.instances.Unstarted;
  t.equal(states0.get(uRec1).discards.discardUnusableRecord.state, expectedState, `states0.get(uRec1).discards.discardUnusableRecord.state must be ${expectedState}`);
  t.equal(states0.get(msg2).ones.processOne.state, expectedState, `states0.get(msg2).ones.processOne.state must be ${expectedState}`);
  t.equal(states0.get(msg3).ones.processOne.state, expectedState, `states0.get(msg3).ones.processOne.state must be ${expectedState}`);

  t.equal(states0.get(batch0).alls.processAll.state, expectedState, `states0.get(batch0).alls.processAll.state must be ${expectedState}`);
  t.equal(states0.get(msg2).alls.processAll.state, expectedState, `states0.get(msg2).alls.processAll.state must be ${expectedState}`);
  t.equal(states0.get(msg3).alls.processAll.state, expectedState, `states0.get(msg3).alls.processAll.state must be ${expectedState}`);

  // const taskFactory = context.taskFactory;

  const uRec1DiscardOneTask = states0.get(uRec1).discards.discardUnusableRecord;
  const msg2ProcessOneTask = states0.get(msg2).ones.processOne;
  const msg3ProcessOneTask = states0.get(msg3).ones.processOne;
  const batchProcessAllTask = states0.get(batch0).alls.processAll;

  // Simulate execution of tasks
  uRec1DiscardOneTask.start();
  msg2ProcessOneTask.start();
  msg3ProcessOneTask.start();
  batchProcessAllTask.start();

  uRec1DiscardOneTask.fail(new Error('Planned uRec1 error'));
  msg2ProcessOneTask.succeed();
  msg3ProcessOneTask.fail(new Error('Planned msg3 error'));
  batchProcessAllTask.complete(); //.fail(new Error('Planned batch process all error'));

  // Simulate persistence & restore of tasks (tasks become simplified task-likes)
  states0.set(uRec1, JSON.parse(JSON.stringify(states0.get(uRec1))));
  states0.set(msg2, JSON.parse(JSON.stringify(states0.get(msg2))));
  states0.set(msg3, JSON.parse(JSON.stringify(states0.get(msg3))));
  states0.set(batch0, JSON.parse(JSON.stringify(states0.get(batch0))));

  t.notOk(states0.get(uRec1).discards.discardUnusableRecord instanceof Task, `states0.get(uRec1).discards.discardUnusableRecord must NOT be an instanceof Task`);
  t.notOk(states0.get(msg2).ones.processOne instanceof Task, `states0.get(msg2).ones.processOne must NOT be an instanceof Task`);
  t.notOk(states0.get(msg3).ones.processOne instanceof Task, `states0.get(msg3).ones.processOne must NOT be an instanceof Task`);

  t.notOk(states0.get(batch0).alls.processAll instanceof Task, `states0.get(batch0).alls.processAll must NOT be an instanceof Task`);
  t.notOk(states0.get(msg2).alls.processAll instanceof Task, `states0.get(msg2).alls.processAll must NOT be an instanceof Task`);
  t.notOk(states0.get(msg3).alls.processAll instanceof Task, `states0.get(msg3).alls.processAll must NOT be an instanceof Task`);

  t.notOk(states0.get(uRec1).discards.discardUnusableRecord.execute, `states0.get(uRec1).discards.discardUnusableRecord.execute must be undefined`);
  t.notOk(states0.get(msg2).ones.processOne.execute, `states0.get(msg2).ones.processOne.execute must be undefined`);
  t.notOk(states0.get(msg3).ones.processOne.execute, `states0.get(msg3).ones.processOne.execute must be undefined`);

  t.notOk(states0.get(batch0).alls.processAll.execute, `states0.get(batch0).alls.processAll.execute must be undefined`);
  t.notOk(states0.get(msg2).alls.processAll.execute, `states0.get(msg2).alls.processAll.execute must be undefined`);
  t.notOk(states0.get(msg3).alls.processAll.execute, `states0.get(msg3).alls.processAll.execute must be undefined`);

  t.equal(states0.get(uRec1).discards.discardUnusableRecord.attempts, 1, `states0.get(uRec1).discards.discardUnusableRecord.attempts must be 1`);
  t.equal(states0.get(msg2).ones.processOne.attempts, 1, `states0.get(msg2).ones.processOne.attempts must be 1`);
  t.equal(states0.get(msg3).ones.processOne.attempts, 1, `states0.get(msg3).ones.processOne.attempts must be 1`);

  t.equal(states0.get(batch0).alls.processAll.attempts, 1, `states0.get(batch0).alls.processAll.attempts must be 1`);
  t.equal(states0.get(msg2).alls.processAll.attempts, 1, `states0.get(msg2).alls.processAll.attempts must be 1`);
  t.equal(states0.get(msg3).alls.processAll.attempts, 1, `states0.get(msg3).alls.processAll.attempts must be 1`);

  const batch = new Batch([rec1, rec2, rec3], [processOneTaskDef], [processAllTaskDef], context);
  const states = batch.states;
  const uRec1a = batch.addUnusableRecord(rec1, undefined, 'Useless', context);
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  // Simulate load & restore of batch state too
  states.set(uRec1a, states0.get(uRec1));
  states.set(msg2, states0.get(msg2));
  states.set(msg3, states0.get(msg3));
  // states.set(batch, states0.get(batch0)); // Do NOT restore batch's state

  // console.log(`**** SEQ BEFORE Batch = ${stringify(batch)}`);

  batch.reviveTasks(context);

  // console.log(`**** SEQ AFTER Batch = ${stringify(batch)}`);

  t.ok(states.get(uRec1a).discards.discardUnusableRecord instanceof Task, `states.get(uRec1a).discards.discardUnusableRecord must be an instanceof Task`);
  t.ok(states.get(msg2).ones.processOne instanceof Task, `states.get(msg2).ones.processOne must be an instanceof Task`);
  t.ok(states.get(msg3).ones.processOne instanceof Task, `states.get(msg3).ones.processOne must be an instanceof Task`);

  t.ok(states.get(batch).alls.processAll instanceof Task, `states.get(batch).alls.processAll must be an instanceof Task`);
  t.ok(states.get(msg2).alls.processAll instanceof Task, `states.get(msg2).alls.processAll must be an instanceof Task`);
  t.ok(states.get(msg3).alls.processAll instanceof Task, `states.get(msg3).alls.processAll must be an instanceof Task`);

  t.ok(typeof states.get(uRec1a).discards.discardUnusableRecord.execute === 'function', `states.get(uRec1a).discards.discardUnusableRecord.execute must be a function`);
  t.ok(typeof states.get(msg2).ones.processOne.execute === 'function', `states.get(msg2).ones.processOne.execute must be a function`);
  t.ok(typeof states.get(msg3).ones.processOne.execute === 'function', `states.get(msg3).ones.processOne.execute must be a function`);

  t.ok(typeof states.get(batch).alls.processAll.execute === 'function', `states.get(batch).alls.processAll.execute must be a function`);
  t.ok(typeof states.get(msg2).alls.processAll.execute === 'function', `states.get(msg2).alls.processAll.execute must be a function`);
  t.ok(typeof states.get(msg3).alls.processAll.execute === 'function', `states.get(msg3).alls.processAll.execute must be a function`);

  t.equal(states.get(uRec1a).discards.discardUnusableRecord.attempts, 1, `states.get(uRec1).discards.discardUnusableRecord.attempts must be 1`);
  t.equal(states.get(msg2).ones.processOne.attempts, 1, `states.get(msg2).ones.processOne.attempts must be 1`);
  t.equal(states.get(msg3).ones.processOne.attempts, 1, `states.get(msg3).ones.processOne.attempts must be 1`);

  t.equal(states.get(batch).alls.processAll.attempts, 1, `states.get(batch).alls.processAll.attempts must be 1`);
  t.equal(states.get(msg2).alls.processAll.attempts, 1, `states.get(msg2).alls.processAll.attempts must be 1`);
  t.equal(states.get(msg3).alls.processAll.attempts, 1, `states.get(msg3).alls.processAll.attempts must be 1`);

  // Check states
  expectedState = taskStates.instances.Unstarted;
  t.deepEqual(states.get(uRec1a).discards.discardUnusableRecord.state, expectedState, `states.get(uRec1a).discards.discardUnusableRecord.state must be ${expectedState}`);
  t.deepEqual(states.get(msg3).ones.processOne.state, expectedState, `states.get(msg3).ones.processOne.state must be ${expectedState}`);

  expectedState = taskStates.instances.Succeeded;
  t.deepEqual(states.get(msg2).ones.processOne.state, expectedState, `states.get(msg2).ones.processOne.state must be ${expectedState}`);

  expectedState = taskStates.instances.Completed;
  t.deepEqual(states.get(msg2).alls.processAll.state, expectedState, `states.get(msg2).alls.processAll.state must be ${expectedState}`);
  t.deepEqual(states.get(msg3).alls.processAll.state, expectedState, `states.get(msg3).alls.processAll.state must be ${expectedState}`);
  t.deepEqual(states.get(batch).alls.processAll.state, expectedState, `states.get(batch).alls.processAll.state must be ${expectedState}`);

  t.end();
});

// =====================================================================================================================
// reviveTasks with 0 messages and 2 unusable records
// =====================================================================================================================

test(`reviveTasks with 0 messages and 2 unusable records AND sequencingRequired ${sequencingRequired}`, t => {
  const context = createContext('kinesis', sequencingRequired, sequencingPerKey, ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  // First record is unusable
  const rec1 = {bad: 'apple'};

  const shardId2 = 'shardId-000000000001';
  const eventSeqNo2 = '49545115243490985018280067714973144582180062593244200961';
  const [, rec2] = sampleKinesisMessageAndRecord(shardId2, eventSeqNo2, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  const processOneTaskDef = TaskDef.defineTask('processOne', processOne);
  const processAllTaskDef = TaskDef.defineTask('processAll', processAll);

  const batch0 = new Batch([rec1, rec2], [processOneTaskDef], [processAllTaskDef], context);
  const states0 = batch0.states;
  const uRec1 = batch0.addUnusableRecord(rec1, undefined, 'Useless', context);
  const uRec2 = batch0.addUnusableRecord(rec2, undefined, 'Terminated with prejudice', context);

  batch0.reviveTasks(context);

  // console.log(`**** SEQ INIT Batch = ${stringify(batch0)}`);

  t.ok(states0.get(uRec1).discards.discardUnusableRecord instanceof Task, `states0.get(uRec1).discards.discardUnusableRecord must be an instanceof Task`);
  t.ok(states0.get(uRec2).discards.discardUnusableRecord instanceof Task, `states0.get(uRec2).discards.discardUnusableRecord must be an instanceof Task`);
  // t.ok(states0.get(batch0).alls.processAll instanceof Task, `states0.get(batch0).alls.processAll must be an instanceof Task`);

  t.ok(typeof states0.get(uRec1).discards.discardUnusableRecord.execute === 'function', `states0.get(uRec1).discards.discardUnusableRecord.execute must be a function`);
  t.ok(typeof states0.get(uRec2).discards.discardUnusableRecord.execute === 'function', `states0.get(uRec2).discards.discardUnusableRecord.execute must be a function`);
  // t.ok(typeof states0.get(batch0).alls.processAll.execute === 'function', `states0.get(batch0).alls.processAll.execute must be a function`);

  t.equal(states0.get(uRec1).discards.discardUnusableRecord.attempts, 0, `states0.get(uRec1).discards.discardUnusableRecord.attempts must be 0`);
  t.equal(states0.get(uRec2).discards.discardUnusableRecord.attempts, 0, `states0.get(uRec2).discards.discardUnusableRecord.attempts must be 0`);
  // t.equal(states0.get(batch0).alls.processAll.attempts, 0, `states0.get(batch0).alls.processAll.attempts must be 0`);

  let expectedState = taskStates.instances.Unstarted;
  t.equal(states0.get(uRec1).discards.discardUnusableRecord.state, expectedState, `states0.get(uRec1).discards.discardUnusableRecord.state must be ${expectedState}`);
  t.equal(states0.get(uRec2).discards.discardUnusableRecord.state, expectedState, `states0.get(uRec2).discards.discardUnusableRecord.state must be ${expectedState}`);
  // t.equal(states0.get(batch0).alls.processAll.state, expectedState, `states0.get(batch0).alls.processAll.state must be ${expectedState}`);

  // const taskFactory = context.taskFactory;

  const uRec1DiscardOneTask = states0.get(uRec1).discards.discardUnusableRecord;
  const uRec2DiscardOneTask = states0.get(uRec2).discards.discardUnusableRecord;
  // const batchProcessAllTask = states0.get(batch0).alls.processAll;

  // Simulate execution of tasks
  uRec1DiscardOneTask.start();
  uRec2DiscardOneTask.start();
  // batchProcessAllTask.start();

  uRec1DiscardOneTask.fail(new Error('Planned uRec1 error 1'));
  uRec2DiscardOneTask.succeed();
  // batchProcessAllTask.fail(new Error('Planned batch process all error'));

  // Simulate persistence & restore of tasks (tasks become simplified task-likes)
  states0.set(uRec1, JSON.parse(JSON.stringify(states0.get(uRec1))));
  states0.set(uRec2, JSON.parse(JSON.stringify(states0.get(uRec2))));
  states0.set(batch0, JSON.parse(JSON.stringify(states0.get(batch0))));

  t.notOk(states0.get(uRec1).discards.discardUnusableRecord instanceof Task, `states0.get(uRec1).discards.discardUnusableRecord must NOT be an instanceof Task`);
  t.notOk(states0.get(uRec2).discards.discardUnusableRecord instanceof Task, `states0.get(uRec2).discards.discardUnusableRecord must NOT be an instanceof Task`);
  t.notOk(states0.get(batch0).alls, `states0.get(batch0).alls must be undefined`);

  t.notOk(states0.get(uRec1).discards.discardUnusableRecord.execute, `states0.get(uRec1).discards.discardUnusableRecord.execute must be undefined`);
  t.notOk(states0.get(uRec2).discards.discardUnusableRecord.execute, `states0.get(uRec2).discards.discardUnusableRecord.execute must be undefined`);
  // t.notOk(states0.get(batch0).alls.processAll.execute, `states0.get(batch0).alls.processAll.execute must be undefined`);

  t.equal(states0.get(uRec1).discards.discardUnusableRecord.attempts, 1, `states0.get(uRec1).discards.discardUnusableRecord.attempts must be 1`);
  t.equal(states0.get(uRec2).discards.discardUnusableRecord.attempts, 1, `states0.get(uRec2).discards.discardUnusableRecord.attempts must be 1`);
  // t.equal(states0.get(batch0).alls.processAll.attempts, 1, `states0.get(batch0).alls.processAll.attempts must be 1`);

  const batch = new Batch([rec1, rec2], [processOneTaskDef], [processAllTaskDef], context);
  const states = batch.states;
  const uRec1a = batch.addUnusableRecord(rec1, undefined, 'Useless', context);
  const uRec2a = batch.addUnusableRecord(rec2, undefined, 'Terminated with prejudice', context);

  // simulate load & restore of batch state too
  states.set(uRec1a, states0.get(uRec1));
  states.set(uRec2a, states0.get(uRec2));
  // states.set(batch, states0.get(batch0)); // Do NOT restore batch's state

  // console.log(`**** SEQ BEFORE Batch = ${stringify(batch)}`);

  batch.reviveTasks(context);

  // console.log(`**** SEQ AFTER Batch = ${stringify(batch)}`);

  t.ok(states.get(uRec1a).discards.discardUnusableRecord instanceof Task, `states.get(uRec1).discards.discardUnusableRecord must be an instanceof Task`);
  t.ok(states.get(uRec2a).discards.discardUnusableRecord instanceof Task, `states.get(uRec2).discards.discardUnusableRecord must be an instanceof Task`);
  t.notOk(states.get(batch).alls, `states.get(batch).alls must be undefined`);

  t.ok(typeof states.get(uRec1a).discards.discardUnusableRecord.execute === 'function', `states.get(uRec1).discards.discardUnusableRecord.execute must be a function`);
  t.ok(typeof states.get(uRec2a).discards.discardUnusableRecord.execute === 'function', `states.get(uRec2).discards.discardUnusableRecord.execute must be a function`);
  // t.ok(typeof states.get(batch).alls.processAll.execute === 'function', `states.get(batch).alls.processAll.execute must be a function`);

  t.equal(states.get(uRec1a).discards.discardUnusableRecord.attempts, 1, `states.get(uRec1).discards.discardUnusableRecord.attempts must be 1`);
  t.equal(states.get(uRec2a).discards.discardUnusableRecord.attempts, 1, `states.get(uRec2).discards.discardUnusableRecord.attempts must be 1`);
  // t.equal(states.get(batch).alls.processAll.attempts, 1, `states.get(batch).alls.processAll.attempts must be 1`);

  // Check states
  expectedState = taskStates.instances.Unstarted;
  t.deepEqual(states.get(uRec1a).discards.discardUnusableRecord.state, expectedState, `states.get(uRec1).discards.discardUnusableRecord.state must be ${expectedState}`);
  // t.deepEqual(states.get(batch).alls.processAll.state, expectedState, `states.get(batch).alls.processAll.state must be ${expectedState}`);

  expectedState = taskStates.instances.Succeeded;
  t.deepEqual(states.get(uRec2a).discards.discardUnusableRecord.state, expectedState, `states.get(uRec2).discards.discardUnusableRecord.state must be ${expectedState}`);

  t.end();
});

// =====================================================================================================================
// reviveTasks with 2 messages and 0 unusable records
// =====================================================================================================================

test('reviveTasks with 2 messages and 0 unusable records AND with sequencingRequired true', t => {
  const context = createContext('kinesis', sequencingRequired, sequencingPerKey, ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId2 = 'shardId-000000000002';
  const eventSeqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, eventSeqNo2, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  const shardId3 = 'shardId-000000000003';
  const eventSeqNo3 = '49545115243490985018280067714973144582180062593244200963';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, eventSeqNo3, eventSourceARN, '456', '789', 'DEF', 11, 4, 5, 6);

  const processOneTaskDef = TaskDef.defineTask('processOne', processOne);
  const processAllTaskDef = TaskDef.defineTask('processAll', processAll);

  const batch0 = new Batch([rec2, rec3], [processOneTaskDef], [processAllTaskDef], context);
  const states0 = batch0.states;
  batch0.addMessage(msg2, rec2, undefined, context);
  batch0.addMessage(msg3, rec3, undefined, context);

  batch0.reviveTasks(context);

  // console.log(`**** SEQ INIT Batch = ${stringify(batch0)}`);

  t.ok(states0.get(msg2).ones.processOne instanceof Task, `states0.get(msg2).ones.processOne must be an instanceof Task`);
  t.ok(states0.get(msg3).ones.processOne instanceof Task, `states0.get(msg3).ones.processOne must be an instanceof Task`);

  t.ok(states0.get(batch0).alls.processAll instanceof Task, `states0.get(batch0).alls.processAll must be an instanceof Task`);
  t.ok(states0.get(msg2).alls.processAll instanceof Task, `states0.get(msg2).alls.processAll must be an instanceof Task`);
  t.ok(states0.get(msg3).alls.processAll instanceof Task, `states0.get(msg3).alls.processAll must be an instanceof Task`);

  t.ok(typeof states0.get(msg2).ones.processOne.execute === 'function', `states0.get(msg2).ones.processOne.execute must be a function`);
  t.ok(typeof states0.get(msg3).ones.processOne.execute === 'function', `states0.get(msg3).ones.processOne.execute must be a function`);

  t.ok(typeof states0.get(batch0).alls.processAll.execute === 'function', `states0.get(batch0).alls.processAll.execute must be a function`);
  t.ok(typeof states0.get(msg2).alls.processAll.execute === 'function', `states0.get(msg2).alls.processAll.execute must be a function`);
  t.ok(typeof states0.get(msg3).alls.processAll.execute === 'function', `states0.get(msg3).alls.processAll.execute must be a function`);

  t.equal(states0.get(msg2).ones.processOne.attempts, 0, `states0.get(msg2).ones.processOne.attempts must be 0`);
  t.equal(states0.get(msg3).ones.processOne.attempts, 0, `states0.get(msg3).ones.processOne.attempts must be 0`);

  t.equal(states0.get(batch0).alls.processAll.attempts, 0, `states0.get(batch0).alls.processAll.attempts must be 0`);
  t.equal(states0.get(msg2).alls.processAll.attempts, 0, `states0.get(msg2).alls.processAll.attempts must be 0`);
  t.equal(states0.get(msg3).alls.processAll.attempts, 0, `states0.get(msg3).alls.processAll.attempts must be 0`);

  let expectedState = taskStates.instances.Unstarted;
  t.equal(states0.get(msg2).ones.processOne.state, expectedState, `states0.get(msg2).ones.processOne.state must be ${expectedState}`);
  t.equal(states0.get(msg3).ones.processOne.state, expectedState, `states0.get(msg3).ones.processOne.state must be ${expectedState}`);

  t.equal(states0.get(batch0).alls.processAll.state, expectedState, `states0.get(batch0).alls.processAll.state must be ${expectedState}`);
  t.equal(states0.get(msg2).alls.processAll.state, expectedState, `states0.get(msg2).alls.processAll.state must be ${expectedState}`);
  t.equal(states0.get(msg3).alls.processAll.state, expectedState, `states0.get(msg3).alls.processAll.state must be ${expectedState}`);

  // const taskFactory = context.taskFactory;

  const msg2ProcessOneTask = states0.get(msg2).ones.processOne;
  const msg3ProcessOneTask = states0.get(msg3).ones.processOne;
  // const msg2ProcessAllTask = states0.get(msg2).alls.processAll;
  // const msg3ProcessAllTask = states0.get(msg3).alls.processAll;
  const batchProcessAllTask = states0.get(batch0).alls.processAll;

  // Simulate execution of tasks
  msg2ProcessOneTask.start();
  msg3ProcessOneTask.start();
  batchProcessAllTask.start();

  msg2ProcessOneTask.succeed();
  msg3ProcessOneTask.fail(new Error('Planned msg3 error'));
  batchProcessAllTask.fail(new Error('Planned batch process all error'));

  // console.log(`**** PRE-PRE-BEFORE Batch 0 = ${stringify(batch0)}`);

  // Simulate persistence & restore of tasks (tasks become task-likes)
  states0.set(msg2, JSON.parse(JSON.stringify(states0.get(msg2))));
  states0.set(msg3, JSON.parse(JSON.stringify(states0.get(msg3))));
  states0.set(batch0, JSON.parse(JSON.stringify(states0.get(batch0))));

  t.notOk(states0.get(msg2).ones.processOne instanceof Task, `states0.get(msg2).ones.processOne must NOT be an instanceof Task`);
  t.notOk(states0.get(msg3).ones.processOne instanceof Task, `states0.get(msg3).ones.processOne must NOT be an instanceof Task`);

  t.notOk(states0.get(batch0).alls.processAll instanceof Task, `states0.get(batch0).alls.processAll must NOT be an instanceof Task`);
  t.notOk(states0.get(msg2).alls.processAll instanceof Task, `states0.get(msg2).alls.processAll must NOT be an instanceof Task`);
  t.notOk(states0.get(msg3).alls.processAll instanceof Task, `states0.get(msg3).alls.processAll must NOT be an instanceof Task`);

  t.notOk(states0.get(msg2).ones.processOne.execute, `states0.get(msg2).ones.processOne.execute must be undefined`);
  t.notOk(states0.get(msg3).ones.processOne.execute, `states0.get(msg3).ones.processOne.execute must be undefined`);

  t.notOk(states0.get(batch0).alls.processAll.execute, `states0.get(batch0).alls.processAll.execute must be undefined`);
  t.notOk(states0.get(msg2).alls.processAll.execute, `states0.get(msg2).alls.processAll.execute must be undefined`);
  t.notOk(states0.get(msg3).alls.processAll.execute, `states0.get(msg3).alls.processAll.execute must be undefined`);

  t.equal(states0.get(msg2).ones.processOne.attempts, 1, `states0.get(msg2).ones.processOne.attempts must be 1`);
  t.equal(states0.get(msg3).ones.processOne.attempts, 1, `states0.get(msg3).ones.processOne.attempts must be 1`);

  t.equal(states0.get(batch0).alls.processAll.attempts, 1, `states0.get(batch0).alls.processAll.attempts must be 1`);
  t.equal(states0.get(msg2).alls.processAll.attempts, 1, `states0.get(msg2).alls.processAll.attempts must be 1`);
  t.equal(states0.get(msg3).alls.processAll.attempts, 1, `states0.get(msg3).alls.processAll.attempts must be 1`);

  const batch = new Batch([rec2, rec3], [processOneTaskDef], [processAllTaskDef], context);
  const states = batch.states;
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  // Simulate load & restore of batch state too
  states.set(msg2, states0.get(msg2));
  states.set(msg3, states0.get(msg3));
  // states.set(batch, states0.get(batch0)); // Do NOT restore batch's state

  // console.log(`**** SEQ BEFORE Batch = ${stringify(batch)}`);

  batch.reviveTasks(context);

  // console.log(`**** SEQ AFTER Batch = ${stringify(batch)}`);

  t.ok(states.get(msg2).ones.processOne instanceof Task, `states.get(msg2).ones.processOne must be an instanceof Task`);
  t.ok(states.get(msg3).ones.processOne instanceof Task, `states.get(msg3).ones.processOne must be an instanceof Task`);

  t.ok(states.get(batch).alls.processAll instanceof Task, `states.get(batch).alls.processAll must be an instanceof Task`);
  t.ok(states.get(msg2).alls.processAll instanceof Task, `states.get(msg2).alls.processAll must be an instanceof Task`);
  t.ok(states.get(msg3).alls.processAll instanceof Task, `states.get(msg3).alls.processAll must be an instanceof Task`);

  t.ok(typeof states.get(msg2).ones.processOne.execute === 'function', `states.get(msg2).ones.processOne.execute must be a function`);
  t.ok(typeof states.get(msg3).ones.processOne.execute === 'function', `states.get(msg3).ones.processOne.execute must be a function`);

  t.ok(typeof states.get(batch).alls.processAll.execute === 'function', `states.get(batch).alls.processAll.execute must be a function`);
  t.ok(typeof states.get(msg2).alls.processAll.execute === 'function', `states.get(msg2).alls.processAll.execute must be a function`);
  t.ok(typeof states.get(msg3).alls.processAll.execute === 'function', `states.get(msg3).alls.processAll.execute must be a function`);

  t.equal(states.get(msg2).ones.processOne.attempts, 1, `states.get(msg2).ones.processOne.attempts must be 1`);
  t.equal(states.get(msg3).ones.processOne.attempts, 1, `states.get(msg3).ones.processOne.attempts must be 1`);

  t.equal(states.get(batch).alls.processAll.attempts, 1, `states.get(batch).alls.processAll.attempts must be 1`);
  t.equal(states.get(msg2).alls.processAll.attempts, 1, `states.get(msg2).alls.processAll.attempts must be 1`);
  t.equal(states.get(msg3).alls.processAll.attempts, 1, `states.get(msg3).alls.processAll.attempts must be 1`);

  // Check states
  expectedState = taskStates.instances.Unstarted;
  t.deepEqual(states.get(msg3).ones.processOne.state, expectedState, `states.get(msg3).ones.processOne.state must be ${expectedState}`);

  t.deepEqual(states.get(batch).alls.processAll.state, expectedState, `states.get(batch).alls.processAll.state must be ${expectedState}`);
  t.deepEqual(states.get(msg2).alls.processAll.state, expectedState, `states.get(msg2).alls.processAll.state must be ${expectedState}`);
  t.deepEqual(states.get(msg3).alls.processAll.state, expectedState, `states.get(msg3).alls.processAll.state must be ${expectedState}`);

  expectedState = taskStates.instances.Succeeded;
  t.deepEqual(states.get(msg2).ones.processOne.state, expectedState, `states.get(msg2).ones.processOne.state must be ${expectedState}`);

  t.end();
});

// =====================================================================================================================
// reviveTasks with 2 ok messages and 1 rejected message
// =====================================================================================================================

test(`reviveTasks with with 2 ok messages and 1 rejected message AND sequencingRequired ${sequencingRequired}`, t => {
  const context = createContext('kinesis', sequencingRequired, sequencingPerKey, ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  // First record will be rejected
  const shardId1 = 'shardId-000000000002';
  const eventSeqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId1, eventSeqNo1, eventSourceARN, '123', '455', 'ABC', 10, 1, 2, 3);

  const shardId2 = 'shardId-000000000002';
  const eventSeqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, eventSeqNo2, eventSourceARN, '123', '456', 'ABC', 10, 4, 5, 6);

  const shardId3 = 'shardId-000000000003';
  const eventSeqNo3 = '49545115243490985018280067714973144582180062593244200963';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, eventSeqNo3, eventSourceARN, '456', '789', 'DEF', 11, 7, 8, 9);

  const processOneTaskDef = TaskDef.defineTask('processOne', processOne);
  const processAllTaskDef = TaskDef.defineTask('processAll', processAll);

  const batch0 = new Batch([rec1, rec2, rec3], [processOneTaskDef], [processAllTaskDef], context);
  const states0 = batch0.states;
  batch0.addMessage(msg1, rec1, undefined, context);
  batch0.addMessage(msg2, rec2, undefined, context);
  batch0.addMessage(msg3, rec3, undefined, context);

  batch0.reviveTasks(context);

  // console.log(`**** UNSEQ INIT Batch = ${stringify(batch0)}`);

  t.ok(states0.get(msg1).ones.processOne instanceof Task, `states0.get(msg1).ones.processOne must be an instanceof Task`);
  t.ok(states0.get(msg2).ones.processOne instanceof Task, `states0.get(msg2).ones.processOne must be an instanceof Task`);
  t.ok(states0.get(msg3).ones.processOne instanceof Task, `states0.get(msg3).ones.processOne must be an instanceof Task`);

  t.ok(states0.get(batch0).alls.processAll instanceof Task, `states0.get(batch0).alls.processAll must be an instanceof Task`);
  t.ok(states0.get(msg1).alls.processAll instanceof Task, `states0.get(msg1).alls.processAll must be an instanceof Task`);
  t.ok(states0.get(msg2).alls.processAll instanceof Task, `states0.get(msg2).alls.processAll must be an instanceof Task`);
  t.ok(states0.get(msg3).alls.processAll instanceof Task, `states0.get(msg3).alls.processAll must be an instanceof Task`);
  // No automatic creation of discard tasks
  t.notOk(states0.get(msg1).discards, `states0.get(msg1).discards must be undefined`);
  t.notOk(states0.get(msg2).discards, `states0.get(msg2).discards must be undefined`);
  t.notOk(states0.get(msg3).discards, `states0.get(msg3).discards must be undefined`);

  t.ok(typeof states0.get(msg1).ones.processOne.execute === 'function', `states0.get(msg1).ones.processOne.execute must be a function`);
  t.ok(typeof states0.get(msg2).ones.processOne.execute === 'function', `states0.get(msg2).ones.processOne.execute must be a function`);
  t.ok(typeof states0.get(msg3).ones.processOne.execute === 'function', `states0.get(msg3).ones.processOne.execute must be a function`);

  t.ok(typeof states0.get(batch0).alls.processAll.execute === 'function', `states0.get(batch0).alls.processAll.execute must be a function`);
  t.ok(typeof states0.get(msg1).alls.processAll.execute === 'function', `states0.get(msg1).alls.processAll.execute must be a function`);
  t.ok(typeof states0.get(msg2).alls.processAll.execute === 'function', `states0.get(msg2).alls.processAll.execute must be a function`);
  t.ok(typeof states0.get(msg3).alls.processAll.execute === 'function', `states0.get(msg3).alls.processAll.execute must be a function`);

  t.equal(states0.get(msg1).ones.processOne.attempts, 0, `states0.get(msg1).ones.processOne.attempts must be 0`);
  t.equal(states0.get(msg2).ones.processOne.attempts, 0, `states0.get(msg2).ones.processOne.attempts must be 0`);
  t.equal(states0.get(msg3).ones.processOne.attempts, 0, `states0.get(msg3).ones.processOne.attempts must be 0`);

  t.equal(states0.get(batch0).alls.processAll.attempts, 0, `states0.get(batch0).alls.processAll.attempts must be 0`);
  t.equal(states0.get(msg1).alls.processAll.attempts, 0, `states0.get(msg1).alls.processAll.attempts must be 0`);
  t.equal(states0.get(msg2).alls.processAll.attempts, 0, `states0.get(msg2).alls.processAll.attempts must be 0`);
  t.equal(states0.get(msg3).alls.processAll.attempts, 0, `states0.get(msg3).alls.processAll.attempts must be 0`);

  let expectedState = taskStates.instances.Unstarted;
  t.equal(states0.get(msg1).ones.processOne.state, expectedState, `states0.get(msg1).ones.processOne.state must be ${expectedState}`);
  t.equal(states0.get(msg2).ones.processOne.state, expectedState, `states0.get(msg2).ones.processOne.state must be ${expectedState}`);
  t.equal(states0.get(msg3).ones.processOne.state, expectedState, `states0.get(msg3).ones.processOne.state must be ${expectedState}`);

  t.equal(states0.get(batch0).alls.processAll.state, expectedState, `states0.get(batch0).alls.processAll.state must be ${expectedState}`);
  t.equal(states0.get(msg1).alls.processAll.state, expectedState, `states0.get(msg1).alls.processAll.state must be ${expectedState}`);
  t.equal(states0.get(msg2).alls.processAll.state, expectedState, `states0.get(msg2).alls.processAll.state must be ${expectedState}`);
  t.equal(states0.get(msg3).alls.processAll.state, expectedState, `states0.get(msg3).alls.processAll.state must be ${expectedState}`);

  const msg1ProcessOneTask = states0.get(msg1).ones.processOne;
  const msg2ProcessOneTask = states0.get(msg2).ones.processOne;
  const msg3ProcessOneTask = states0.get(msg3).ones.processOne;
  const batchProcessAllTask = states0.get(batch0).alls.processAll;

  // Simulate LATER creation of discard rejected message(s) tasks from task definitions
  const discardRejectedMessageTaskDef = batch0.taskDefs.discardRejectedMessageTaskDef;

  const taskFactory = context.taskFactory;

  const msg1DiscardOneTask = taskFactory.createTask(discardRejectedMessageTaskDef, processOpts);
  const msg1DiscardOneTasks = batch0.getOrSetDiscardOneTasks(msg1);
  msg1DiscardOneTasks.discardRejectedMessage = msg1DiscardOneTask;

  // Simulate execution of tasks
  msg1ProcessOneTask.start();
  msg1DiscardOneTask.start();
  msg2ProcessOneTask.start();
  msg3ProcessOneTask.start();
  batchProcessAllTask.start();

  msg1ProcessOneTask.reject('Smells', new Error('Planned rejection'));
  msg1DiscardOneTask.fail(new Error('Planned msg1 discard one error'));
  msg2ProcessOneTask.succeed();
  msg3ProcessOneTask.fail(new Error('Planned msg3 error'));
  batchProcessAllTask.succeed(); //fail(new Error('Planned batch process all error'));

  // console.log(`**** PRE-PRE-BEFORE Batch 0 = ${stringify(batch0)}`);

  // Simulate persistence & restore of tasks (tasks become task-likes)
  states0.set(msg1, JSON.parse(JSON.stringify(states0.get(msg1))));
  states0.set(msg2, JSON.parse(JSON.stringify(states0.get(msg2))));
  states0.set(msg3, JSON.parse(JSON.stringify(states0.get(msg3))));
  states0.set(batch0, JSON.parse(JSON.stringify(states0.get(batch0))));

  t.notOk(states0.get(msg1).ones.processOne instanceof Task, `states0.get(msg1).ones.processOne must NOT be an instanceof Task`);
  t.notOk(states0.get(msg2).ones.processOne instanceof Task, `states0.get(msg2).ones.processOne must NOT be an instanceof Task`);
  t.notOk(states0.get(msg3).ones.processOne instanceof Task, `states0.get(msg3).ones.processOne must NOT be an instanceof Task`);

  t.notOk(states0.get(msg1).discards.discardRejectedMessage instanceof Task, `states0.get(msg1).discards.discardRejectedMessage must NOT be an instanceof Task`);
  t.notOk(states0.get(msg1).discards.discardUnusableRecord, `states0.get(msg1).discards.discardUnusableRecord must be undefined`);

  t.notOk(states0.get(batch0).alls.processAll instanceof Task, `states0.get(batch0).alls.processAll must NOT be an instanceof Task`);
  t.notOk(states0.get(msg1).alls.processAll instanceof Task, `states0.get(msg1).alls.processAll must NOT be an instanceof Task`);
  t.notOk(states0.get(msg2).alls.processAll instanceof Task, `states0.get(msg2).alls.processAll must NOT be an instanceof Task`);
  t.notOk(states0.get(msg3).alls.processAll instanceof Task, `states0.get(msg3).alls.processAll must NOT be an instanceof Task`);

  t.notOk(states0.get(msg1).ones.processOne.execute, `states0.get(msg1).ones.processOne.execute must be undefined`);
  t.notOk(states0.get(msg2).ones.processOne.execute, `states0.get(msg2).ones.processOne.execute must be undefined`);
  t.notOk(states0.get(msg3).ones.processOne.execute, `states0.get(msg3).ones.processOne.execute must be undefined`);

  t.notOk(states0.get(msg1).discards.discardRejectedMessage.execute, `states0.get(msg1).discards.discardRejectedMessage.execute must be undefined`);
  // t.notOk(states0.get(msg1).discards.discardUnusableRecord.execute, `states0.get(msg1).discards.discardUnusableRecord.execute must be undefined`);

  t.notOk(states0.get(batch0).alls.processAll.execute, `states0.get(batch0).alls.processAll.execute must be undefined`);
  t.notOk(states0.get(msg1).alls.processAll.execute, `states0.get(msg1).alls.processAll.execute must be undefined`);
  t.notOk(states0.get(msg2).alls.processAll.execute, `states0.get(msg2).alls.processAll.execute must be undefined`);
  t.notOk(states0.get(msg3).alls.processAll.execute, `states0.get(msg3).alls.processAll.execute must be undefined`);

  t.equal(states0.get(msg1).ones.processOne.attempts, 1, `states0.get(msg1).ones.processOne.attempts must be 1`);
  t.equal(states0.get(msg2).ones.processOne.attempts, 1, `states0.get(msg2).ones.processOne.attempts must be 1`);
  t.equal(states0.get(msg3).ones.processOne.attempts, 1, `states0.get(msg3).ones.processOne.attempts must be 1`);

  t.equal(states0.get(msg1).discards.discardRejectedMessage.attempts, 1, `states0.get(msg1).discards.discardRejectedMessage.attempts must be 1`);
  // t.equal(states0.get(msg1).discards.discardUnusableRecord.attempts, 1, `states0.get(msg1).discards.discardUnusableRecord.attempts must be 1`);

  t.equal(states0.get(batch0).alls.processAll.attempts, 1, `states0.get(batch0).alls.processAll.attempts must be 1`);
  t.equal(states0.get(msg1).alls.processAll.attempts, 1, `states0.get(msg1).alls.processAll.attempts must be 1`);
  t.equal(states0.get(msg2).alls.processAll.attempts, 1, `states0.get(msg2).alls.processAll.attempts must be 1`);
  t.equal(states0.get(msg3).alls.processAll.attempts, 1, `states0.get(msg3).alls.processAll.attempts must be 1`);

  const batch = new Batch([rec1, rec2, rec3], [processOneTaskDef], [processAllTaskDef], context);
  const states = batch.states;
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  // Simulate load & restore of batch state too
  states.set(msg1, states0.get(msg1));
  states.set(msg2, states0.get(msg2));
  states.set(msg3, states0.get(msg3));
  // states.set(batch, states0.get(batch0)); // Do NOT restore batch's state

  // console.log(`**** SEQ BEFORE Batch = ${stringify(batch)}`);

  batch.reviveTasks(context);

  // console.log(`**** SEQ AFTER Batch = ${stringify(batch)}`);

  t.ok(states.get(msg1).ones.processOne instanceof Task, `states.get(msg1).ones.processOne must be an instanceof Task`);
  t.ok(states.get(msg2).ones.processOne instanceof Task, `states.get(msg2).ones.processOne must be an instanceof Task`);
  t.ok(states.get(msg3).ones.processOne instanceof Task, `states.get(msg3).ones.processOne must be an instanceof Task`);

  t.ok(states.get(msg1).discards.discardRejectedMessage instanceof Task, `states.get(msg1).discards.discardRejectedMessage must be an instanceof Task`);
  t.notOk(states.get(msg1).discards.discardUnusableRecord, `states.get(msg1).discards.discardUnusableRecord must be undefined`);

  t.ok(states.get(batch).alls.processAll instanceof Task, `states.get(batch).alls.processAll must be an instanceof Task`);
  t.ok(states.get(msg1).alls.processAll instanceof Task, `states.get(msg1).alls.processAll must be an instanceof Task`);
  t.ok(states.get(msg2).alls.processAll instanceof Task, `states.get(msg2).alls.processAll must be an instanceof Task`);
  t.ok(states.get(msg3).alls.processAll instanceof Task, `states.get(msg3).alls.processAll must be an instanceof Task`);

  t.ok(typeof states.get(msg1).ones.processOne.execute === 'function', `states.get(msg1).ones.processOne.execute must be a function`);
  t.ok(typeof states.get(msg2).ones.processOne.execute === 'function', `states.get(msg2).ones.processOne.execute must be a function`);
  t.ok(typeof states.get(msg3).ones.processOne.execute === 'function', `states.get(msg3).ones.processOne.execute must be a function`);

  t.ok(typeof states.get(msg1).discards.discardRejectedMessage.execute === 'function', `states.get(msg1).discards.discardRejectedMessage.execute must be a function`);
  // t.ok(typeof states.get(msg1).discards.discardUnusableRecord.execute === 'function', `states.get(msg1).discards.discardUnusableRecord.execute must be a function`);

  t.ok(typeof states.get(batch).alls.processAll.execute === 'function', `states.get(batch).alls.processAll.execute must be a function`);
  t.ok(typeof states.get(msg1).alls.processAll.execute === 'function', `states.get(msg1).alls.processAll.execute must be a function`);
  t.ok(typeof states.get(msg2).alls.processAll.execute === 'function', `states.get(msg2).alls.processAll.execute must be a function`);
  t.ok(typeof states.get(msg3).alls.processAll.execute === 'function', `states.get(msg3).alls.processAll.execute must be a function`);

  t.equal(states.get(msg1).ones.processOne.attempts, 1, `states.get(msg1).ones.processOne.attempts must be 1`);
  t.equal(states.get(msg2).ones.processOne.attempts, 1, `states.get(msg2).ones.processOne.attempts must be 1`);
  t.equal(states.get(msg3).ones.processOne.attempts, 1, `states.get(msg3).ones.processOne.attempts must be 1`);

  t.equal(states.get(msg1).discards.discardRejectedMessage.attempts, 1, `states.get(msg1).discards.discardRejectedMessage.attempts must be 1`);
  // t.equal(states.get(msg1).discards.discardUnusableRecord.attempts, 1, `states.get(msg1).discards.discardUnusableRecord.attempts must be 1`);

  t.equal(states.get(batch).alls.processAll.attempts, 1, `states.get(batch).alls.processAll.attempts must be 1`);
  t.equal(states.get(msg1).alls.processAll.attempts, 1, `states.get(msg1).alls.processAll.attempts must be 1`);
  t.equal(states.get(msg2).alls.processAll.attempts, 1, `states.get(msg2).alls.processAll.attempts must be 1`);
  t.equal(states.get(msg3).alls.processAll.attempts, 1, `states.get(msg3).alls.processAll.attempts must be 1`);

  // Check states
  expectedState = new taskStates.Rejected('Smells', new Error('Planned rejection'));
  t.deepEqual(states.get(msg1).ones.processOne.state, expectedState, `states.get(msg1).ones.processOne.state must be ${expectedState}`);

  expectedState = taskStates.instances.Unstarted;
  t.deepEqual(states.get(msg1).discards.discardRejectedMessage.state, expectedState, `states.get(msg1).discards.discardRejectedMessage.state must be ${expectedState}`);
  t.deepEqual(states.get(msg3).ones.processOne.state, expectedState, `states.get(msg3).ones.processOne.state must be ${expectedState}`);

  expectedState = taskStates.instances.Succeeded;
  t.deepEqual(states.get(msg2).ones.processOne.state, expectedState, `states.get(msg2).ones.processOne.state must be ${expectedState}`);

  t.deepEqual(states.get(batch).alls.processAll.state, expectedState, `states.get(batch).alls.processAll.state must be ${expectedState}`);
  t.deepEqual(states.get(msg1).alls.processAll.state, expectedState, `states.get(msg1).alls.processAll.state must be ${expectedState}`);
  t.deepEqual(states.get(msg2).alls.processAll.state, expectedState, `states.get(msg2).alls.processAll.state must be ${expectedState}`);
  t.deepEqual(states.get(msg3).alls.processAll.state, expectedState, `states.get(msg3).alls.processAll.state must be ${expectedState}`);

  t.end();
});

// =====================================================================================================================
// discardUnusableRecords
// =====================================================================================================================

test(`discardUnusableRecords with 2 unusable records AND sequencingRequired ${!sequencingRequired}`, t => {
  const context = createContext('kinesis', !sequencingRequired, sequencingPerKey, ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  // First record is unusable
  const rec1 = {bad: 'apple'};

  // Second record is unusable
  const shardId2 = 'shardId-000000000001';
  const eventSeqNo2 = '49545115243490985018280067714973144582180062593244200961';
  const [, rec2] = sampleKinesisMessageAndRecord(shardId2, eventSeqNo2, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  const processOneTaskDef = TaskDef.defineTask('processOne', processOne);
  const processAllTaskDef = TaskDef.defineTask('processAll', processAll);

  const batch = new Batch([rec1, rec2], [processOneTaskDef], [processAllTaskDef], context);
  const states = batch.states;
  const uRec1 = batch.addUnusableRecord(rec1, undefined, 'Useless', context);
  const uRec2 = batch.addUnusableRecord(rec2, undefined, 'Terminated with prejudice', context);

  batch.reviveTasks(context);

  // console.log(`**** UNSEQ BEFORE Batch = ${stringify(batch)}`);

  const cancellable = {};
  const promise = batch.discardUnusableRecords(cancellable, context);

  // console.log(`**** UNSEQ AFTER 1 Batch = ${stringify(batch)}`);

  t.ok(states.get(uRec1).discards.discardUnusableRecord instanceof Task, `states.get(uRec1).discards.discardUnusableRecord must be an instanceof Task`);
  t.ok(states.get(uRec2).discards.discardUnusableRecord instanceof Task, `states.get(uRec2).discards.discardUnusableRecord must be an instanceof Task`);
  t.notOk(states.get(batch).alls, `states.get(batch).alls must be undefined`);

  t.ok(typeof states.get(uRec1).discards.discardUnusableRecord.execute === 'function', `states.get(uRec1).discards.discardUnusableRecord.execute must be a function`);
  t.ok(typeof states.get(uRec2).discards.discardUnusableRecord.execute === 'function', `states.get(uRec2).discards.discardUnusableRecord.execute must be a function`);

  t.equal(states.get(uRec1).discards.discardUnusableRecord.attempts, 1, `states.get(uRec1).discards.discardUnusableRecord.attempts must be 1`);
  t.equal(states.get(uRec2).discards.discardUnusableRecord.attempts, 1, `states.get(uRec2).discards.discardUnusableRecord.attempts must be 1`);

  // Check states
  let expectedState = taskStates.instances.Started;
  t.deepEqual(states.get(uRec1).discards.discardUnusableRecord.state, expectedState, `states.get(uRec1).discards.discardUnusableRecord.state must be ${expectedState}`);
  t.deepEqual(states.get(uRec2).discards.discardUnusableRecord.state, expectedState, `states.get(uRec2).discards.discardUnusableRecord.state must be ${expectedState}`);

  promise.then(
    outcomes => {
      //console.log(`**** outcomes = ${stringify(outcomes)}`);

      t.equal(outcomes.length, 2, `outcomes.length must be 2`);
      t.ok(outcomes[0].isSuccess(), `outcomes[0] must be Success`);
      t.ok(outcomes[1].isSuccess(), `outcomes[1] must be Success`);
      t.deepEqual(outcomes[0], new Success({index: 0}), `outcomes[0] must be Success({index: 0})`);
      t.deepEqual(outcomes[1], new Success({index: 1}), `outcomes[1] must be Success({index: 1})`);

      // console.log(`**** SEQ AFTER 2 Batch = ${stringify(batch)}`);

      // Check states
      expectedState = taskStates.instances.Completed;
      t.deepEqual(states.get(uRec1).discards.discardUnusableRecord.state, expectedState, `states.get(uRec1).discards.discardUnusableRecord.state must be ${expectedState}`);
      t.deepEqual(states.get(uRec2).discards.discardUnusableRecord.state, expectedState, `states.get(uRec2).discards.discardUnusableRecord.state must be ${expectedState}`);

      t.end();
    },
    err => {
      t.end(err);
    }
  );
});

// =====================================================================================================================
// abandonDeadProcessingTasks
// =====================================================================================================================

test(`abandonDeadProcessingTasks`, t => {
  const context = createContext('kinesis', sequencingRequired, sequencingPerKey, ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId, eventSeqNo1, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  const rec2 = {bad: 'apple'};

  const eventSeqNo3 = '49545115243490985018280067714973144582180062593244200962';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId, eventSeqNo3, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 4);

  const processOneTaskDef0 = TaskDef.defineTask('processOne0', processOne);
  const processAllTaskDef0 = TaskDef.defineTask('processAll0', processAll);

  const batch0 = new Batch([rec1, rec2, rec3], [processOneTaskDef0], [processAllTaskDef0], context);
  const states0 = batch0.states;
  batch0.addMessage(msg1, rec1, undefined, context);
  let uRec2 = batch0.addUnusableRecord(rec2, undefined, 'Useless', context);
  batch0.addMessage(msg3, rec3, undefined, context);

  const discardUnusableRecordTaskDef = batch0.taskDefs.discardUnusableRecordTaskDef;

  batch0.reviveTasks(context);
  let msg1State = states0.get(msg1);
  let msg3State = states0.get(msg3);

  const m1T1 = msg1State.ones.processOne0;
  m1T1.start();
  m1T1.fail(new Error('Msg1 Bang 1'));

  const m1T2 = msg1State.alls.processAll0;
  m1T2.start();
  m1T2.fail(new Error('Msg1 Bang 2'));

  const m3T1 = msg3State.ones.processOne0;
  m3T1.start();
  m3T1.reject('Begone foul message', undefined, true);

  const m3T2 = msg3State.alls.processAll0;
  m3T2.start();
  m3T2.reject('Begone foul message', undefined, true);

  const u2D1 = context.taskFactory.createTask(discardUnusableRecordTaskDef);
  u2D1.start();
  let uRec2State = states0.get(uRec2);
  if (!uRec2State.discards) uRec2State.discards = {};
  uRec2State.discards[u2D1.name] = u2D1;
  u2D1.fail(new Error('URec2 Bang 3'));

  // "Store" prior msg state
  const msgStateStored = JSON.stringify(msg1State); // serialize

  // "Store" prior uRec2 state
  const uRec2StateStored = JSON.stringify(uRec2State); // serialize

  // Completely change active task definitions
  const processOneTaskDef = TaskDef.defineTask('processOne', processOne);
  const processAllTaskDef = TaskDef.defineTask('processAll', processAll);
  // const uRec1 = batch0.addUnusableRecord(rec1, undefined, 'Useless', context);

  const batch = new Batch([rec1, rec2], [processOneTaskDef], [processAllTaskDef], context);
  const states = batch.states;
  batch.addMessage(msg1, rec1, undefined, context);
  uRec2 = batch.addUnusableRecord(rec2, undefined, 'Useless', context);

  // "Load" prior "stored" msg state
  states.set(msg1, JSON.parse(msgStateStored)); // de-serialize
  msg1State = states.get(msg1);

  // "Load" prior "stored" uRec2 state
  states.set(uRec2, JSON.parse(uRec2StateStored)); // de-serialize
  uRec2State = states.get(uRec2);
  // states.get(batch) = JSON.parse(JSON.stringify(states.get(batch0))); // serialize & de-serialize

  t.notOk(msg1State.ones.processOne instanceof Task, `msg1State.ones.processOne must NOT be an instanceof Task`);
  t.notOk(msg1State.alls.processAll instanceof Task, `msg1State.alls.processAll must NOT be an instanceof Task`);
  t.notOk(uRec2State.discards.discardUnusableRecord instanceof Task, `uRec2State.discards.discardUnusableRecord must NOT be an instanceof Task`);

  // Revive tasks from prior state & new active task definitions
  batch.reviveTasks(context);

  // New active tasks MUST be present
  t.ok(msg1State.ones.processOne instanceof Task, `msg1State.ones.processOne must be an instanceof Task`);
  t.equal(msg1State.ones.processOne.unusable, false, `msg1State.ones.processOne.unusable must be false`);
  t.equal(msg1State.ones.processOne.definition.unusable, false, `msg1State.ones.processOne.definition.unusable must be false`);

  t.ok(msg1State.alls.processAll instanceof Task, `msg1State.alls.processAll must be an instanceof Task`);
  t.notOk(msg1State.alls.processAll.isMasterTask(), `msg1State.alls.processAll.isMasterTask() must be false`);
  t.equal(msg1State.alls.processAll.unusable, false, `msg1State.alls.processAll.unusable must be false`);
  t.equal(msg1State.alls.processAll.definition.unusable, false, `msg1State.alls.processAll.definition.unusable must be false`);

  t.ok(states.get(batch).alls.processAll instanceof Task, `states.get(batch).alls.processAll must be an instanceof Task`);
  t.ok(states.get(batch).alls.processAll.isMasterTask(), `states.get(batch).alls.processAll.isMasterTask() must be true`);
  t.equal(states.get(batch).alls.processAll.unusable, false, `states.get(batch).alls.processAll.unusable must be false`);

  // Prior no longer active tasks must also be restored, but MUST be marked as unusable
  t.ok(msg1State.ones.processOne0 instanceof Task, `msg1State.ones.processOne0 must be an instanceof Task`);
  t.equal(msg1State.ones.processOne0.unusable, true, `msg1State.ones.processOne0.unusable must be true`);
  t.equal(msg1State.ones.processOne0.definition.unusable, true, `msg1State.ones.processOne0.definition.unusable must be true`);
  t.equal(msg1State.ones.processOne0.rejected, false, `msg1State.ones.processOne0.rejected must be false`);
  t.equal(msg1State.ones.processOne0.isAbandoned(), false, `msg1State.ones.processOne0.isAbandoned() must be false`);

  t.ok(msg1State.alls.processAll0 instanceof Task, `msg1State.alls.processAll0 must be an instanceof Task`);
  t.equal(msg1State.alls.processAll0.unusable, true, `msg1State.alls.processAll0.unusable must be true`);
  t.equal(msg1State.alls.processAll0.definition.unusable, true, `msg1State.alls.processAll0.definition.unusable must be true`);
  t.equal(msg1State.alls.processAll0.rejected, false, `msg1State.alls.processAll0.rejected must be false`);
  t.equal(msg1State.alls.processAll0.isAbandoned(), false, `msg1State.alls.processAll0.isAbandoned() must be false`);

  // t.ok(states.get(batch).alls.processAll0 instanceof Task, `states.get(batch).alls.processAll0 must be an instanceof Task`);

  t.ok(uRec2State.discards.discardUnusableRecord instanceof Task, `uRec2State.discards.discardUnusableRecord must be an instanceof Task`);
  t.equal(uRec2State.discards.discardUnusableRecord.unusable, false, `uRec2State.discards.discardUnusableRecord.unusable must be false`);
  t.equal(uRec2State.discards.discardUnusableRecord.definition.unusable, false, `uRec2State.discards.discardUnusableRecord.definition.unusable must be false`);
  t.equal(uRec2State.discards.discardUnusableRecord.rejected, false, `uRec2State.discards.discardUnusableRecord.rejected must be false`);
  t.equal(uRec2State.discards.discardUnusableRecord.isAbandoned(), false, `uRec2State.discards.discardUnusableRecord.isAbandoned() must be false`);

  // Complete the 2 new active tasks successfully
  msg1State.ones.processOne.complete();
  states.get(batch).alls.processAll.succeed();

  t.equal(batch.isFullyFinalised(), false, `batch.isFullyFinalised() must be false`);

  t.equal(msg1State.ones.processOne.isFullyFinalisedOrUnusable(), true, `msg1State.ones.processOne.isFullyFinalisedOrUnusable must be true`);
  t.equal(msg1State.alls.processAll.isFullyFinalisedOrUnusable(), true, `msg1State.alls.processAll.isFullyFinalisedOrUnusable() must be true`);
  t.equal(states.get(batch).alls.processAll.isFullyFinalisedOrUnusable(), true, `states.get(batch).alls.processAll.isFullyFinalisedOrUnusable() must be true`);

  t.equal(msg1State.ones.processOne0.isFullyFinalisedOrUnusable(), true, `msg1State.ones.processOne0.isFullyFinalisedOrUnusable must be true`);
  t.equal(msg1State.alls.processAll0.isFullyFinalisedOrUnusable(), true, `msg1State.alls.processAll0.isFullyFinalisedOrUnusable() must be true`);
  // t.equal(states.get(batch).alls.processAll0.isFullyFinalisedOrUnusable(), true, `states.get(batch).alls.processAll0.isFullyFinalisedOrUnusable() must be true`);

  const abandonedTasks = batch.abandonDeadProcessingTasks();

  t.equals(abandonedTasks.length, 2, `abandonedTasks length must be 2`);
  t.ok(abandonedTasks.includes(msg1State.ones.processOne0), `abandonedTasks includes msg1State.ones.processOne0`);
  t.ok(abandonedTasks.includes(msg1State.alls.processAll0), `abandonedTasks includes msg1State.alls.processAll0`);

  t.ok(msg1State.ones.processOne0 instanceof Task, `msg1State.ones.processOne0 must be an instanceof Task`);
  t.equal(msg1State.ones.processOne0.unusable, true, `msg1State.ones.processOne0.unusable must be true`);
  t.equal(msg1State.ones.processOne0.definition.unusable, true, `msg1State.ones.processOne0.definition.unusable must be true`);
  t.equal(msg1State.ones.processOne0.rejected, true, `msg1State.ones.processOne0.rejected must be true`);
  t.equal(msg1State.ones.processOne0.isAbandoned(), true, `msg1State.ones.processOne0.isAbandoned() must be true`);

  t.ok(msg1State.alls.processAll0 instanceof Task, `msg1State.alls.processAll0 must be an instanceof Task`);
  t.equal(msg1State.alls.processAll0.unusable, true, `msg1State.alls.processAll0.unusable must be true`);
  t.equal(msg1State.alls.processAll0.definition.unusable, true, `msg1State.alls.processAll0.definition.unusable must be true`);
  t.equal(msg1State.alls.processAll0.rejected, true, `msg1State.alls.processAll0.rejected must be true`);
  t.equal(msg1State.alls.processAll0.isAbandoned(), true, `msg1State.alls.processAll0.isAbandoned() must be true`);

  t.notOk(states.get(batch).alls.processAll0, `states.get(batch).alls.processAll0 must not exist`);

  t.end();
});

test('getState & getOrSetState', t => {
  const context = createContext('kinesis', sequencingRequired, sequencingPerKey, ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  // First record is unusable
  const rec1 = {bad: 'apple'};

  const shardId2 = 'shardId-000000000002';
  const eventSeqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, eventSeqNo2, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  const processOneTaskDef = TaskDef.defineTask('processOne', processOne);
  const processAllTaskDef = TaskDef.defineTask('processAll', processAll);

  const batch = new Batch([rec1, rec2], [processOneTaskDef], [processAllTaskDef], context);

  t.equal(batch.getState(rec1), undefined, `batch.getState(rec1) must be undefined`);
  t.equal(batch.getState(msg2), undefined, `batch.getState(msg2) must be undefined`);

  const msg2State = batch.getOrSetState(msg2);
  t.ok(msg2State, `batch.getOrSetState(msg2) must be defined`);
  t.notOk(msg2State.eventID, `msg2State.eventID must NOT be defined yet`);
  t.equal(batch.getState(msg2), msg2State, `batch.getState(msg2) must be msg2State`);

  batch.addMessage(msg2, rec2, undefined, context);

  t.equal(batch.getState(msg2), msg2State, `batch.getState(msg2) must still be msg2State`);
  t.ok(msg2State.eventID, `msg2State.eventID must be defined`);

  const uRec1 = batch.addUnusableRecord(rec1, undefined, 'Dud', context);

  const uRec1State = batch.getState(uRec1);
  t.ok(uRec1State, `batch.getState(uRec1) must be defined`);

  const uRec1State2 = batch.getOrSetState(uRec1);
  t.ok(uRec1State2, `batch.getOrSetState(uRec1) must be defined`);
  t.equal(uRec1State2, uRec1State, `batch.getOrSetState(uRec1) must be uRec1State`);

  t.end();
});