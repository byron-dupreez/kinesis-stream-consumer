'use strict';

/**
 * Unit tests for aws-stream-consumer-core/persisting.js to test loading & saving of tracked state for a batch sourced from a
 * Kinesis stream.
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const persisting = require('aws-stream-consumer-core/persisting');
const toBatchStateItem = persisting.toBatchStateItem;
const saveBatchStateToDynamoDB = persisting.saveBatchStateToDynamoDB;
const loadBatchStateFromDynamoDB = persisting.loadBatchStateFromDynamoDB;

// const hasMessageIdentifier = persisting.hasMessageIdentifier;
// const toMessageBFK = persisting.toMessageBFK;

const hasUnusableRecordIdentifier = persisting.hasUnusableRecordIdentifier;
const toUnusableRecordBFK = persisting.toUnusableRecordBFK;

const kinesisProcessing = require('../kinesis-processing');

const Batch = require('aws-stream-consumer-core/batch');

const Settings = require('aws-stream-consumer-core/settings');
const StreamType = Settings.StreamType;

const dynamoDBMocking = require('aws-core-test-utils/dynamodb-mocking');
const mockDynamoDBDocClient = dynamoDBMocking.mockDynamoDBDocClient;

const taskUtils = require('task-utils');
const TaskDef = require('task-utils/task-defs');
// const Task = require('task-utils/tasks');
const taskStates = require('task-utils/task-states');
const TaskState = taskStates.TaskState;
const core = require('task-utils/core');
const StateType = core.StateType;
const cleanCompletedState = JSON.parse(JSON.stringify(taskStates.instances.Completed));

const logging = require('logging-utils');
const LogLevel = logging.LogLevel;

const Promises = require('core-functions/promises');

const strings = require('core-functions/strings');
const stringify = strings.stringify;

const tries = require('core-functions/tries');
//const Try = tries.Try;
const Success = tries.Success;
// const Failure = tries.Failure;

// const noRecordsMsgRegex = /No Batch .* state to save, since 0 records/;
// const noMsgsOrUnusableRecordsMsgRegex = /LOGIC FLAWED - No Batch .* state to save, since 0 messages and 0 unusable records/;

const awsRegion = "us-west-2";
const accountNumber = "XXXXXXXXXXXX";

const samples = require('./samples');

const eventSourceARN = `arn:aws:kinesis:${awsRegion}:${accountNumber}:stream/MyStream_DEV`;

function createContext(sequencingRequired, idPropertyNames, keyPropertyNames, seqNoPropertyNames) {
  idPropertyNames = idPropertyNames ? idPropertyNames : ['id1', 'id2'];
  keyPropertyNames = sequencingRequired ? keyPropertyNames ? keyPropertyNames : ['k1', 'k2'] : [];
  seqNoPropertyNames = seqNoPropertyNames ? seqNoPropertyNames : ['n1', 'n2', 'n3'];
  process.env.AWS_REGION = awsRegion;
  const context = {
    region: awsRegion,
    stage: 'dev'
  };
  const streamProcessingOptions = {
    streamType: StreamType.kinesis,
    sequencingRequired: sequencingRequired,
    sequencingPerKey: sequencingRequired && (keyPropertyNames && keyPropertyNames.length > 0),
    consumerIdSuffix: undefined,
    maxNumberOfAttempts: 2,
    idPropertyNames: idPropertyNames,
    keyPropertyNames: keyPropertyNames,
    seqNoPropertyNames: seqNoPropertyNames,
    batchStateTableName: 'TEST_StreamConsumerBatchState'
  };
  logging.configureLogging(context, {logLevel: LogLevel.TRACE});

  kinesisProcessing.configureDefaultKinesisStreamProcessing(context, streamProcessingOptions, undefined,
    require('../default-kinesis-options.json'), undefined, undefined, true);

  context.streamProcessing.consumerId = `my-function:${context.stage}`;

  // Set the task factory to use on the context (after configuring logging, so that the context can be used as a logger too)
  taskUtils.configureTaskFactory(context, {logger: context}); //, {returnMode: taskUtils.ReturnMode.NORMAL});
  return context;
}

function processOne(message, batch, context) {
  context.trace(`Executing processOne on message (${batch.states.get(message).msgDesc})`);
  return Promise.resolve(batch.states.get(message).eventID);
}

function processAll(batch, context) {
  const messages = batch.messages;
  context.trace(`Executing processAll on batch (${batch.shardOrEventID})`);
  return Promise.resolve(messages.map(m => batch.states.get(m).eventID));
}

function discardUnusableRecord(unusableRecord, batch, context) {
  context.trace(`Simulating execution of discardUnusableRecord on record (${batch.states.get(unusableRecord).recDesc})`);
  return Promise.resolve(unusableRecord);
}

function discardRejectedMessage(rejectedMessage, batch, context) {
  context.trace(`Simulating execution of discardRejectedMessage on message (${batch.states.get(rejectedMessage).msgDesc})`);
  return Promise.resolve(rejectedMessage);
}

function kinesisFixture(sequencingRequired) {
  const context = createContext(sequencingRequired, ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4']);

  const processOneTaskDef = TaskDef.defineTask(processOne.name, processOne);
  const processAllTaskDef = TaskDef.defineTask(processAll.name, processAll);

  context.streamProcessing.discardUnusableRecord = discardUnusableRecord;
  context.streamProcessing.discardRejectedMessage = discardRejectedMessage;

  // Message 1 - usable
  const shardId1 = sequencingRequired ? 'shardId-111111111111' : 'shardId-400000000000';
  const seqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, record1] = samples.sampleKinesisMessageAndRecord(shardId1, seqNo1, eventSourceARN, 'ID1', '1001', 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');
  record1.kinesis.partitionKey = 'abfac769-e349-4d16-90d0-b857a578971f';

  // Message 2 - "usable", but with rejected task
  const shardId2 = sequencingRequired ? 'shardId-222222222222' : 'shardId-500000000000';
  const seqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, record2] = samples.sampleKinesisMessageAndRecord(shardId2, seqNo2, eventSourceARN, 'ID1', '1002', 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');
  record2.kinesis.partitionKey = 'b660092a-27d4-46b6-aa83-cade97ed6099';

  // Message 3 - "unusable"
  const shardId3 = sequencingRequired ? 'shardId-333333333333' : 'shardId-600000000000';
  const seqNo3 = '49545115243490985018280067714973144582180062593244200963';
  // noinspection JSUnusedLocalSymbols
  const [msg3, record3] = samples.sampleKinesisMessageAndRecord(shardId3, seqNo3, eventSourceARN, 'ID1', '1003', 'ABC', 10, 1, 100, '10000000000000000000003', '2017-01-17T23:59:59.003Z');
  record3.kinesis.partitionKey = '8e2893e9-0134-45d1-8f82-1c1f8c732cf3';

  // Message 4 - also usable
  const shardId4 = sequencingRequired ? 'shardId-444444444444' : 'shardId-700000000000';
  const seqNo4 = '49545115243490985018280067714973144582180062593244200964';
  const [msg4, record4] = samples.sampleKinesisMessageAndRecord(shardId4, seqNo4, eventSourceARN, 'ID1', '1004', 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.004Z');
  record4.kinesis.partitionKey = 'c291e2aa-87b0-48bf-85ff-500bb1f6041e';

  // Message 5 - also "usable", but with rejected task
  const shardId5 = sequencingRequired ? 'shardId-555555555555' : 'shardId-800000000000';
  const seqNo5 = '49545115243490985018280067714973144582180062593244200965';
  const [msg5, record5] = samples.sampleKinesisMessageAndRecord(shardId5, seqNo5, eventSourceARN, 'ID1', '1005', 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.005Z');
  record5.kinesis.partitionKey = 'da12eda8-ed73-4dbf-b54b-73dc1bd16a0a';

  // Unusable record 1
  const badRec1 = {badApple: true};

  // Unusable record 2
  // noinspection UnnecessaryLocalVariableJS
  const badRec2 = record3;

  return {
    context: context,
    cancellable: {},
    processOneTaskDef: processOneTaskDef,
    processAllTaskDef: processAllTaskDef,

    shardId1: shardId1,
    eventID1: record1.eventID,
    seqNo1: seqNo1,
    msg1: msg1,
    record1: record1,

    shardId2: shardId2,
    eventID2: record2.eventID,
    seqNo2: seqNo2,
    msg2: msg2,
    record2: record2,

    badRec1: badRec1,
    badRec2: badRec2,

    shardId3: shardId3,
    eventID3: record3.eventID,
    seqNo3: seqNo3,
    msg3: msg3,
    record3: record3,

    shardId4: shardId4,
    eventID4: record4.eventID,
    seqNo4: seqNo4,
    msg4: msg4,
    record4: record4,

    shardId5: shardId5,
    eventID5: record5.eventID,
    seqNo5: seqNo5,
    msg5: msg5,
    record5: record5
  };
}

function executeTasks(f, batch, t) {
  // Message 1 task 1 (process one task)
  const msg1Task1 = batch.getProcessOneTask(f.msg1, processOne.name);
  const m1p1 = msg1Task1 ? msg1Task1.execute(f.msg1, batch, f.context) : Promise.resolve(undefined);

  if (batch.messages.indexOf(f.msg1) !== -1 && batch.taskDefs.processOneTaskDefs.length > 0) {
    t.ok(msg1Task1, `msg1Task1 must exist`);
  }

  // Message 2 task 1 (process one task) - execute & then mark as rejected
  const msg2Task1 = batch.getProcessOneTask(f.msg2, processOne.name);
  const m2p1 = msg2Task1 ? msg2Task1.execute(f.msg2, batch, f.context) : Promise.resolve(undefined);

  if (batch.messages.indexOf(f.msg2) !== -1 && batch.taskDefs.processOneTaskDefs.length > 0) {
    t.ok(msg2Task1, `msg2Task1 must exist`);
  }

  const m2p1b = m2p1.then(res => {
    if (msg2Task1) {
      msg2Task1.reject('We\'re NOT gonna take it!', new Error('Twisted'));
    }
    return res;
  });

  // Message 4 task 1 (process one task)
  const msg4Task1 = batch.getProcessOneTask(f.msg4, processOne.name);
  const m4p1 = msg4Task1 ? msg4Task1.execute(f.msg4, batch, f.context) : Promise.resolve(undefined);

  if (batch.messages.indexOf(f.msg4) !== -1 && batch.taskDefs.processOneTaskDefs.length > 0) {
    t.ok(msg4Task1, `msg4Task1 must exist`);
  }

  // Message 5 task 1 (process one task) - execute & then mark as rejected
  const msg5Task1 = batch.getProcessOneTask(f.msg5, processOne.name);
  const m5p1 = msg5Task1 ? msg5Task1.execute(f.msg5, batch, f.context) : Promise.resolve(undefined);

  if (batch.messages.indexOf(f.msg5) !== -1 && batch.taskDefs.processOneTaskDefs.length > 0) {
    t.ok(msg5Task1, `msg5Task1 must exist`);
  }

  const m5p1b = m5p1.then(res => {
    if (msg5Task1) {
      msg5Task1.reject('No, we ain\'t gonna take it!', new Error('Twisted too'));
    }
    return res;
  });

  // Batch task 1 (process all task)
  const batchTask1 = batch.getProcessAllTask(batch, processAll.name);
  const b1p1 = batchTask1 ? batchTask1.execute(batch, f.context) : Promise.resolve(undefined);

  if (batch.messages.length > 0 && batch.taskDefs.processAllTaskDefs.length > 0) {
    t.ok(batchTask1, `batchTask1 must exist`);

    t.ok(batchTask1.isMasterTask(), `batchTask1 must be a master task`);

    // Message 1 task 2 (process all task)
    if (batch.messages.indexOf(f.msg1) !== -1) {
      const msg1Task2 = batch.getProcessAllTask(f.msg1, processAll.name);
      t.ok(msg1Task2, `msg1Task2 must exist`);
    }

    // Message 2 task 2 (process all task)
    if (batch.messages.indexOf(f.msg2) !== -1) {
      const msg2Task2 = batch.getProcessAllTask(f.msg2, processAll.name);
      t.ok(msg2Task2, `msg2Task2 must exist`);
    }

    // Message 4 task 2 (process all task)
    if (batch.messages.indexOf(f.msg4) !== -1) {
      const msg4Task2 = batch.getProcessAllTask(f.msg4, processAll.name);
      t.ok(msg4Task2, `msg4Task2 must exist`);
    }

    // Message 5 task 2 (process all task)
    if (batch.messages.indexOf(f.msg5) !== -1) {
      const msg5Task2 = batch.getProcessAllTask(f.msg5, processAll.name);
      t.ok(msg5Task2, `msg5Task2 must exist`);
    }
  }

  const d1 = batch.discardUnusableRecords(f.cancellable, f.context);

  const d2 = Promise.all([m2p1b, m5p1b, b1p1]).then(() => batch.discardRejectedMessages(f.cancellable, f.context));

  return Promises.every([m1p1, m2p1, m4p1, m5p1, m2p1b, m5p1b, b1p1, d1, d2], f.cancellable, f.context);
}

// =====================================================================================================================
// toBatchStateItem for kinesis
// =====================================================================================================================

test('toBatchStateItem for kinesis - 1 message', t => {
  const f = kinesisFixture(true);

  const batch = new Batch([f.record1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const item = toBatchStateItem(batch, f.context);
      // console.log(`item = ${JSON.stringify(item)}`);

      t.ok(item, `item must be defined`);
      const consumerId = f.context.streamProcessing.consumerId;
      t.equal(item.streamConsumerId, `K|MyStream_DEV|${consumerId}`, `item streamConsumerId must be 'K|MyStream_DEV|${consumerId}'`);
      t.equal(item.shardOrEventID, `S|${f.shardId1}`, `item shardOrEventID must be 'S|${f.shardId1}'`);

      t.equal(item.messageStates.length, 1, `item messageStates.length must be 1`);
      t.deepEqual(item.unusableRecordStates, [], `item unusableRecordStates must be []`);
      t.ok(item.batchState, `item batchState must be defined`);
      t.ok(item.batchState.alls.processAll, `item batchState.alls.processAll must be defined`);
      t.deepEqual(item.batchState.alls.processAll.state, cleanCompletedState, `item batchState.alls.processAll.state must be Completed`);

      const msgState1 = item.messageStates[0];
      t.equal(msgState1.eventID, f.eventID1, `msgState1 eventID must be ${f.eventID1}`);
      t.ok(msgState1.ones.processOne, `msgState1 ones.processOne must be defined`);
      t.deepEqual(msgState1.ones.processOne.state, cleanCompletedState, `msgState1 ones.processOne.state must be Completed`);
      t.deepEqual(msgState1.alls.processAll.state, cleanCompletedState, `msgState1.alls.processAll.state must be Completed`);

      t.end();
    },
    err => {
      t.end(err);
    }
  );
});

test('toBatchStateItem for kinesis - 2 messages - 1 rejected', t => {
  const f = kinesisFixture(true);

  // Wait for the messages processOne & processAll tasks to finish executing
  // 2 messages
  const batch = new Batch([f.record2, f.record1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  batch.addMessage(f.msg2, f.record2, undefined, f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);

  console.log(`batch = ${JSON.stringify(batch)}`);

  batch.reviveTasks(f.context);

  console.log(`batch POST reviveTasks = ${JSON.stringify(batch)}`);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      console.log(`batch POST executeTasks = ${JSON.stringify(batch)}`);

      const item = toBatchStateItem(batch, f.context);
      console.log(`item = ${JSON.stringify(item)}`);

      t.ok(item, `item must be defined`);
      const consumerId = f.context.streamProcessing.consumerId;
      t.equal(item.streamConsumerId, `K|MyStream_DEV|${consumerId}`, `item streamConsumerId must be 'K|MyStream_DEV|${consumerId}'`);
      t.equal(item.shardOrEventID, 'S|shardId-222222222222', `item shardOrEventID must be 'S|shardId-222222222222'`);

      t.equal(item.messageStates.length, 1, `item messageStates.length must be 1`);
      t.equal(item.rejectedMessageStates.length, 1, `item rejectedMessageStates.length must be 1`);
      t.deepEqual(item.unusableRecordStates, [], `item unusableRecordStates must be []`);
      t.ok(item.batchState, `item batchState must be defined`);
      t.ok(item.batchState.alls.processAll, `item batchState.alls.processAll must be defined`);
      t.deepEqual(item.batchState.alls.processAll.state, cleanCompletedState, `item batchState.alls.processAll.state must be Completed`);

      const msgState1 = item.messageStates[0];
      t.equal(msgState1.eventID, f.eventID1, `msgState1 eventID must be ${f.eventID1}`);
      t.ok(msgState1.ones.processOne, `msgState1 ones.processOne must be defined`);
      t.deepEqual(msgState1.ones.processOne.state, cleanCompletedState, `msgState1 ones.processOne.state must be Completed`);
      // t.notOk(msgState1.alls, `msgState1 ones must NOT be defined`);
      t.equal(msgState1.alls.processAll.state.type, StateType.Completed, `msgState1 alls.processAll.state.type must be ${StateType.Completed}`);

      const msgState2 = item.rejectedMessageStates[0];
      t.equal(msgState2.eventID, f.eventID2, `msgState2 eventID must be ${f.eventID2}`);
      t.ok(msgState2.ones.processOne, `msgState2 ones.processOne must be defined`);
      t.equal(msgState2.ones.processOne.state.type, StateType.Rejected, `msgState2 ones.processOne.state.type must be rejected`);
      // t.notOk(msgState2.alls, `msgState2 ones must NOT be defined`);
      t.equal(msgState2.alls.processAll.state.type, StateType.Completed, `msgState2 alls.processAll.state.type must be ${StateType.Completed}`);
      t.ok(msgState2.discards.discardRejectedMessage, `msgState2 discards.discardRejectedMessage must be defined`);
      t.deepEqual(msgState2.discards.discardRejectedMessage.state, cleanCompletedState, `msgState2 discards.discardRejectedMessage.state must be Completed`);

      t.end();
    },
    err => {
      t.end(err);
    }
  );
});

test('toBatchStateItem for kinesis - 1 completely unusable record', t => {
  const f = kinesisFixture(true);

  // 1 unusable record
  const batch = new Batch([f.badRec1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, 'Bad apple', f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const item = toBatchStateItem(batch, f.context);
      // console.log(`item = ${JSON.stringify(item)}`);

      t.ok(item, `item must be undefined`);
      t.equal(item.streamConsumerId, undefined, `item.streamConsumerId must be undefined`);
      t.equal(item.shardOrEventID, undefined, `item.shardOrEventID must be undefined`);
      t.equal(item.unusableRecordStates.length, 1, `item.unusableRecordStates.length must be 1`);

      const unusableRecordState1 = item.unusableRecordStates[0];
      t.ok(hasUnusableRecordIdentifier(unusableRecordState1), `hasUnusableRecordIdentifier(unusableRecordState1) must be true`);
      const bfk = toUnusableRecordBFK(unusableRecordState1);
      t.ok(bfk, `toUnusableRecordBFK -> (${bfk}) must be defined`);
      // t.deepEqual(unusableRecordState1.unusableRecord, f.uRec1, `unusableRecordState1.unusableRecord must be uRec1`);

      t.end();
    },
    err => {
      t.end(err);
    }
  );
});

test('toBatchStateItem for kinesis - 2 unusable records', t => {
  const f = kinesisFixture(true);

  // 2 unusable records
  const batch = new Batch([f.badRec1, f.badRec2], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, 'Bad apple', f.context);
  f.uRec2 = batch.addUnusableRecord(f.badRec2, undefined, 'Unparseable', f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const item = toBatchStateItem(batch, f.context);
      // console.log(`item = ${JSON.stringify(item)}`);

      t.ok(item, `item must be defined`);
      const consumerId = f.context.streamProcessing.consumerId;
      t.equal(item.streamConsumerId, `K|MyStream_DEV|${consumerId}`, `item streamConsumerId must be 'K|MyStream_DEV|${consumerId}'`);
      t.equal(item.shardOrEventID, `S|${f.shardId3}`, `item shardOrEventID must be 'S|${f.shardId3}'`);

      t.deepEqual(item.messageStates, [], `item messageStates must be []`);
      t.equal(item.unusableRecordStates.length, 2, `item unusableRecordStates.length must be 2`);
      t.ok(item.batchState, `item batchState must be defined`);
      t.deepEqual(item.batchState, {}, `item batchState must be {}`);
      t.notOk(item.batchState.alls, `item batchState.alls must be undefined`);
      t.notOk(item.batchState.ones, `item batchState.ones must be undefined`);
      t.notOk(item.batchState.discards, `item batchState.discards must be undefined`);

      const uRecState1 = item.unusableRecordStates[0];
      t.equal(uRecState1.eventID, undefined, `uRecState1 eventID must be undefined`);
      t.ok(hasUnusableRecordIdentifier(uRecState1), `hasUnusableRecordIdentifier(uRecState1) must be true`);
      t.notOk(uRecState1.unusableRecord, `uRecState1 unusableRecord must be undefined`);
      t.notOk(uRecState1.userRecord, `uRecState1 userRecord must be undefined`);
      t.notOk(uRecState1.record, `uRecState1 record must be undefined`);
      t.ok(uRecState1.discards.discardUnusableRecord, `uRecState1 discards.discardUnusableRecord must be defined`);
      t.deepEqual(uRecState1.discards.discardUnusableRecord.state, cleanCompletedState, `uRecState1 discards.discardUnusableRecord.state must be Completed`);

      const uRecState2 = item.unusableRecordStates[1];
      t.equal(uRecState2.eventID, f.eventID3, `uRecState2 eventID must be ${f.eventID3}`);
      t.notOk(uRecState2.record, `uRecState2 record must be undefined`);

      t.end();
    },
    err => {
      t.end(err);
    }
  );
});

test('toBatchStateItem for kinesis - 2 unusable records & 1 ok message', t => {
  const f = kinesisFixture(true);

  // 2 unusable records + 1 ok record
  const batch = new Batch([f.badRec1, f.record1, f.badRec2], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, 'Bad apple', f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);
  f.uRec2 = batch.addUnusableRecord(f.badRec2, undefined, 'Unparseable', f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const item = toBatchStateItem(batch, f.context);
      // console.log(`item = ${JSON.stringify(item)}`);

      t.ok(item, `item must be defined`);
      const consumerId = f.context.streamProcessing.consumerId;
      t.equal(item.streamConsumerId, `K|MyStream_DEV|${consumerId}`, `item streamConsumerId must be 'K|MyStream_DEV|${consumerId}'`);
      t.equal(item.shardOrEventID, 'S|shardId-111111111111', `item shardOrEventID must be 'S|shardId-111111111111'`);

      t.equal(item.messageStates.length, 1, `item messageStates.length must be 1`);
      t.equal(item.unusableRecordStates.length, 2, `item unusableRecordStates.length must be 2`);
      t.ok(item.batchState, `item batchState must be defined`);
      t.ok(item.batchState.alls, `item batchState.alls must be defined`);
      t.notOk(item.batchState.ones, `item batchState.ones must be undefined`);
      t.notOk(item.batchState.discards, `item batchState.discards must be undefined`);

      const msgState1 = item.messageStates[0];
      t.equal(msgState1.eventID, f.eventID1, `msgState1 eventID must be ${f.eventID1}`);

      const uRecState1 = item.unusableRecordStates[0];
      t.equal(uRecState1.eventID, undefined, `uRecState1 eventID must be undefined`);
      t.ok(hasUnusableRecordIdentifier(uRecState1), `hasUnusableRecordIdentifier(uRecState1) must be true`);
      t.notOk(uRecState1.unusableRecord, `uRecState1 unusableRecord must NOT be undefined`);
      t.notOk(uRecState1.userRecord, `uRecState1 userRecord must NOT be undefined`);
      t.notOk(uRecState1.record, `uRecState1 record must NOT be undefined`);

      const uRecState2 = item.unusableRecordStates[1];
      t.equal(uRecState2.eventID, f.eventID3, `uRecState2 eventID must be ${f.eventID3}`);
      t.notOk(uRecState2.record, `uRecState2 record must be undefined`);

      t.end();
    },
    err => {
      t.end(err);
    }
  );
});

// =====================================================================================================================
// saveBatchStateToDynamoDB - failure cases
// =====================================================================================================================

test('saveBatchStateToDynamoDB - 0 msgs, 0 rejected msgs, 0 unusable recs & 0 recs', t => {
  const f = kinesisFixture(true);
  const taskFactory = f.context.taskFactory;

  f.context.dynamoDBDocClient = mockDynamoDBDocClient(t, 'persisting.test.js', 5, {});

  const task = taskFactory.createTask(TaskDef.defineTask(saveBatchStateToDynamoDB.name, saveBatchStateToDynamoDB));

  const batch = new Batch([], [f.processOneTaskDef], [f.processAllTaskDef], f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  return promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      return executeResult.then(
        result => {
          t.pass(`saveBatchStateToDynamoDB must succeed - result ${stringify(result)}`);
          t.equal(result, undefined, `result must be undefined`);

          t.ok(task.started, `Task (${task.name}) must be started`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.equal(task.state.name, TaskState.names.Started, `Task (${task.name}) state.name must be ${TaskState.names.Started}`);
          // t.ok(noRecordsMsgRegex.test(task.state.reason), `Task (${task.name}) state.reason must match '${noRecordsMsgRegex}'`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must not be undefined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          return task.donePromise.then(
            doneResult => {
              t.equal(doneResult, undefined, `Task (${task.name}) donePromise result must be undefined`);
              t.equal(doneResult, result, `Task (${task.name}) donePromise result must be result`);

              t.ok(task.completed, `Task (${task.name}) must be completed`);
              t.equal(task.result, result, `Task (${task.name}) result must still be result`);
              t.end();
            },
            err => {
              t.fail(`Task (${task.name}) donePromise must NOT reject`, err);
              t.end();
            }
          );
        },
        err => {
          t.fail(`saveBatchStateToDynamoDB must NOT fail`, err);
          t.end();
        }
      );
    },
    err => {
      t.end(err);
    }
  );
});

test('saveBatchStateToDynamoDB - 0 msgs, 0 rejected msgs, 0 unusable recs, but 1 record', t => {
  const f = kinesisFixture(true);
  const taskFactory = f.context.taskFactory;

  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|MyStream_DEV|my-function:dev';
    const expectedShardOrEventId = 'S|shardId-111111111111';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 0, `item.messageStates.length must be 0`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  const putResult = {};
  f.context.dynamoDBDocClient = mockDynamoDBDocClient(t, 'persisting.test.js', 5,
    {put: {result: putResult, validateArgs: validatePutParams}});

  const task = taskFactory.createTask(TaskDef.defineTask(saveBatchStateToDynamoDB.name, saveBatchStateToDynamoDB));
  // const initialAttempts = task.attempts;

  const batch = new Batch([f.record1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  // batch.addMessage(f.msg1, f.record1, undefined, f.context); // explicitly NOT added

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  return promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      return executeResult.then(
        result => {
          t.pass(`saveBatchStateToDynamoDB must succeed - result ${JSON.stringify(result)}`);
          t.equal(result, putResult, `result must be putResult`);

          t.ok(task.started, `Task (${task.name}) must be started`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.equal(task.state.name, TaskState.names.Started, `Task (${task.name}) state.name must be ${TaskState.names.Started}`);
          // t.ok(noRecordsMsgRegex.test(task.state.reason), `Task (${task.name}) state.reason must match '${noRecordsMsgRegex}'`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must not be undefined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          return task.donePromise.then(
            doneResult => {
              t.equal(doneResult, putResult, `Task (${task.name}) donePromise result must be putResult`);
              t.equal(doneResult, result, `Task (${task.name}) donePromise result must be result`);

              t.ok(task.completed, `Task (${task.name}) must be completed`);
              t.equal(task.result, result, `Task (${task.name}) result must still be result`);
              t.end();
            },
            err => {
              t.fail(`Task (${task.name}) donePromise must NOT reject`, err);
              t.end();
            }
          );
        },
        err => {
          t.fail(`saveBatchStateToDynamoDB must NOT fail`, err);
          t.end();
        }
      );
    },
    err => {
      t.end(err);
    }
  );
});

test('saveBatchStateToDynamoDB for kinesis - 0 msgs, 0 rejected msgs, 1 totally unusable rec, 1 rec (with max attempts 2)', t => {
  const f = kinesisFixture(true);
  const taskFactory = f.context.taskFactory;

  // noinspection JSUnusedLocalSymbols
  function validatePutParams(t, params) {
    t.fail(`put should NOT be called`);
  }

  const putError = new Error('ConditionalCheckFailed');
  putError.code = 'ConditionalCheckFailedException';
  const updateResult = {};
  f.context.dynamoDBDocClient = mockDynamoDBDocClient(t, 'persisting.test.js', 5, {
    put: {error: putError},
    update: {result: updateResult, validateArgs: validatePutParams}
  });

  f.context.streamProcessing.maxNumberOfAttempts = 2;

  const task = taskFactory.createTask(TaskDef.defineTask(saveBatchStateToDynamoDB.name, saveBatchStateToDynamoDB));

  const batch = new Batch([f.badRec1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, 'Bad apple', f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  return promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      return executeResult.then(
        result => {
          t.fail(`saveBatchStateToDynamoDB must NOT succeed - result ${stringify(result)}`);
          t.end();
        },
        err => {
          t.pass(`saveBatchStateToDynamoDB must fail with an error`);
          t.ok(err.message.startsWith('Cannot save state of batch'), `err.message must start with 'Cannot save state of batch'`);

          t.ok(task.started, `Task (${task.name}) must be started`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.equal(task.error, undefined, `Task (${task.name}) error must be undefined`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must be defined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          return task.donePromise.then(
            doneResult => {
              t.fail(`Task (${task.name}) donePromise must NOT resolve with:`, doneResult);
              t.end();
            },
            doneErr => {
              t.deepEqual(doneErr, err, `Task (${task.name}) donePromise error must be ${stringify(err)}`);
              t.equal(doneErr, err, `Task (${task.name}) donePromise error must be rejected err`);

              t.ok(task.failed, `Task (${task.name}) must be failed`);
              t.ok(task.error, `Task (${task.name}) error must be defined`);
              t.equal(task.error, err, `Task (${task.name}).error must be err`);

              t.end();
            }
          );
        }
      );
    },
    err => {
      t.fail(`promise must NOT fail`, err);
      t.end(err);
    }
  );
});

test('saveBatchStateToDynamoDB for kinesis - 0 msgs, 0 rejected msgs, 1 totally unusable rec, 1 rec (with max attempts 1)', t => {
  const f = kinesisFixture(true);
  const taskFactory = f.context.taskFactory;

  f.context.dynamoDBDocClient = mockDynamoDBDocClient(t, 'persisting.test.js', 5, {});

  f.context.streamProcessing.maxNumberOfAttempts = 1;

  const task = taskFactory.createTask(TaskDef.defineTask(saveBatchStateToDynamoDB.name, saveBatchStateToDynamoDB));

  const batch = new Batch([f.badRec1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, 'Bad apple', f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  return promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      return executeResult.then(
        result => {
          t.fail(`saveBatchStateToDynamoDB must NOT succeed - result ${stringify(result)}`);
          t.end();
        },
        err => {
          t.pass(`saveBatchStateToDynamoDB must fail with an error`);
          t.ok(err.message.startsWith('Cannot save state of batch'), `err.message must start with 'Cannot save state of batch'`);

          t.ok(task.started, `Task (${task.name}) must be started`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.equal(task.error, undefined, `Task (${task.name}) error must be undefined`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must be defined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          return task.donePromise.then(
            doneResult => {
              t.fail(`Task (${task.name}) donePromise must NOT resolve with:`, doneResult);
              t.end();
            },
            doneErr => {
              t.deepEqual(doneErr, err, `Task (${task.name}) donePromise error must be ${stringify(err)}`);
              t.equal(doneErr, err, `Task (${task.name}) donePromise error must be rejected err`);

              t.ok(task.failed, `Task (${task.name}) must be failed`);
              t.ok(task.error, `Task (${task.name}) error must be defined`);
              t.equal(task.error, err, `Task (${task.name}).error must be err`);

              t.end();
            }
          );
        }
      );
    },
    err => {
      t.fail(`promise must NOT fail`, err);
      t.end(err);
    }
  );
});

// =====================================================================================================================
// saveBatchStateToDynamoDB - kinesis
// =====================================================================================================================

test('saveBatchStateToDynamoDB for kinesis - 1 message & 1 totally unusable record', t => {
  const f = kinesisFixture(true);
  const taskFactory = f.context.taskFactory;

  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|MyStream_DEV|my-function:dev';
    const expectedShardOrEventId = 'S|shardId-111111111111';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 1, `item.messageStates.length must be 1`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 1, `item.unusableRecordStates.length must be 1`);
  }

  const putResult = {};
  f.context.dynamoDBDocClient = mockDynamoDBDocClient(t, 'persisting.test.js', 5, {
    put: {result: putResult, validateArgs: validatePutParams}
  });

  const task = taskFactory.createTask(TaskDef.defineTask(saveBatchStateToDynamoDB.name, saveBatchStateToDynamoDB));

  const batch = new Batch([f.badRec1, f.record1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, 'Bad apple', f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      return executeResult.then(
        result => {
          t.pass(`saveBatchStateToDynamoDB must succeed - result ${stringify(result)}`);
          t.ok(task.started, `Task (${task.name}) must be started`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.equal(task.error, undefined, `Task (${task.name}) error must be undefined`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must not be undefined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          return task.donePromise.then(
            doneResult => {
              t.equal(doneResult, result, `Task (${task.name}) donePromise result must be result`);

              t.ok(task.completed, `Task (${task.name}) must be completed`);
              t.equal(task.result, result, `Task (${task.name}) result must be result`);
              t.end();
            },
            err => {
              t.fail(`Task (${task.name}) donePromise must NOT reject`, err);
              t.end();
            }
          );
        },
        err => {
          t.fail(`saveBatchStateToDynamoDB must NOT fail`, err);
          t.end();
        }
      );
    },
    err => {
      t.fail(`promise must NOT fail`, err);
      t.end(err);
    }
  );
});

test('saveBatchStateToDynamoDB for kinesis - 1 message with DynamoDB error', t => {
  const f = kinesisFixture(true);
  const taskFactory = f.context.taskFactory;


  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|MyStream_DEV|my-function:dev';
    const expectedShardOrEventId = 'S|shardId-111111111111';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 1, `item.messageStates.length must be 1`);
    t.equal(item.rejectedMessageStates.length, 0, `item.rejectedMessageStates.length must be 0`);
    t.equal(item.unusableRecordStates.length, 0, `item.unusableRecordStates.length must be 0`);
  }

  const putError = new Error('Planned DynamoDB error');
  f.context.dynamoDBDocClient = mockDynamoDBDocClient(t, 'persisting.test.js', 5, {
    put: {error: putError, validateArgs: validatePutParams}
  });

  const task = taskFactory.createTask(TaskDef.defineTask(saveBatchStateToDynamoDB.name, saveBatchStateToDynamoDB));

  const batch = new Batch([f.record1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      return executeResult.then(
        result => {
          t.fail(`saveBatchStateToDynamoDB must NOT succeed - result ${stringify(result)}`);
          t.end();
        },
        err => {
          t.pass(`saveBatchStateToDynamoDB must fail`);
          t.equal(err, putError, `Error ${err} must be putError`);
          t.ok(task.started, `Task (${task.name}) must be started`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.equal(task.error, undefined, `Task (${task.name}) error must be undefined`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must not be undefined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          return task.donePromise.then(
            doneResult => {
              t.fail(`Task (${task.name}) donePromise must NOT resolve with:`, doneResult);
              t.end();
            },
            doneErr => {
              t.equal(doneErr, putError, `Task (${task.name}) donePromise error must be putError`);

              t.ok(task.failed, `Task (${task.name}) must be failed`);
              t.equal(task.result, undefined, `Task (${task.name}) result must still be undefined`);
              t.equal(task.error, putError, `Task (${task.name}) error must be putError`);
              t.end();
            }
          );
        }
      );
    },
    err => {
      t.end(err);
    }
  );
});

test('saveBatchStateToDynamoDB for kinesis - 1 msg, 1 rejected msg & 2 unusable recs - sequencing required', t => {
  const f = kinesisFixture(true);
  const taskFactory = f.context.taskFactory;

  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|MyStream_DEV|my-function:dev';
    const expectedShardOrEventId = 'S|shardId-333333333333';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 1, `item.messageStates.length must be 1`);
    t.equal(item.rejectedMessageStates.length, 1, `item.rejectedMessageStates.length must be 1`);
    t.equal(item.unusableRecordStates.length, 2, `item.unusableRecordStates.length must be 2`);
  }

  const putResult = {};
  f.context.dynamoDBDocClient = mockDynamoDBDocClient(t, 'persisting.test.js', 5, {
    put: {result: putResult, validateArgs: validatePutParams}
  });

  const task = taskFactory.createTask(TaskDef.defineTask(saveBatchStateToDynamoDB.name, saveBatchStateToDynamoDB));

  const batch = new Batch([f.badRec1, f.badRec2, f.record1, f.record2], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, 'Bad apple', f.context);
  f.uRec2 = batch.addUnusableRecord(f.badRec2, undefined, 'Unparseable', f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);
  batch.addMessage(f.msg2, f.record2, undefined, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      return executeResult.then(
        result => {
          t.pass(`saveBatchStateToDynamoDB must succeed - result ${stringify(result)}`);
          t.ok(task.started, `Task (${task.name}) must be started`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.equal(task.error, undefined, `Task (${task.name}) error must be undefined`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must not be undefined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          return task.donePromise.then(
            doneResult => {
              t.equal(doneResult, result, `Task (${task.name}) donePromise result must be result`);

              t.ok(task.completed, `Task (${task.name}) must be completed`);
              t.equal(task.result, result, `Task (${task.name}) result must be result`);
              t.end();
            },
            err => {
              t.fail(`Task (${task.name}) donePromise must NOT reject`, err);
              t.end();
            }
          );
        },
        err => {
          t.fail(`saveBatchStateToDynamoDB must NOT fail`, err);
          t.end();
        }
      );
    },
    err => {
      t.fail(`promise must NOT fail`, err);
      t.end(err);
    }
  );
});

test('saveBatchStateToDynamoDB for kinesis - 2 msgs, 2 rejected msgs & 2 unusable recs - sequencing NOT required', t => {
  const f = kinesisFixture(false);
  const taskFactory = f.context.taskFactory;

  function validatePutParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|MyStream_DEV|my-function:dev';
    const expectedShardOrEventId = 'S|shardId-600000000000';
    const item = params.Item;
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.equal(item.streamConsumerId, expectedStreamConsumerId, `item.streamConsumerId must be ${expectedStreamConsumerId}`);
    t.equal(item.shardOrEventID, expectedShardOrEventId, `item.shardOrEventID must be ${expectedShardOrEventId}`);
    t.equal(item.messageStates.length, 2, `item.messageStates.length must be 2`);
    t.equal(item.rejectedMessageStates.length, 2, `item.rejectedMessageStates.length must be 2`);
    t.equal(item.unusableRecordStates.length, 2, `item.unusableRecordStates.length must be 2`);
  }

  const putResult = {};
  f.context.dynamoDBDocClient = mockDynamoDBDocClient(t, 'persisting.test.js', 5, {
    put: {result: putResult, validateArgs: validatePutParams}
  });

  const task = taskFactory.createTask(TaskDef.defineTask(saveBatchStateToDynamoDB.name, saveBatchStateToDynamoDB));

  const batch = new Batch([f.badRec1, f.badRec2, f.record1, f.record2, f.record4, f.record5], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, 'Bad apple', f.context);
  f.uRec2 = batch.addUnusableRecord(f.badRec2, undefined, 'Unparseable', f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);
  batch.addMessage(f.msg2, f.record2, undefined, f.context);
  batch.addMessage(f.msg4, f.record4, undefined, f.context);
  batch.addMessage(f.msg5, f.record5, undefined, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      return executeResult.then(
        result => {
          t.pass(`saveBatchStateToDynamoDB must succeed - result ${stringify(result)}`);
          t.ok(task.started, `Task (${task.name}) must be started`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.equal(task.error, undefined, `Task (${task.name}) error must be undefined`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must not be undefined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          return task.donePromise.then(
            doneResult => {
              t.equal(doneResult, result, `Task (${task.name}) donePromise result must be result`);

              t.ok(task.completed, `Task (${task.name}) must be completed`);
              t.equal(task.result, result, `Task (${task.name}) result must be result`);
              t.end();
            },
            err => {
              t.fail(`Task (${task.name}) donePromise must NOT reject`, err);
              t.end();
            }
          );
        },
        err => {
          t.fail(`saveBatchStateToDynamoDB must NOT fail`, err);
          t.end();
        }
      );
    },
    err => {
      t.fail(`promise must NOT fail`, err);
      t.end(err);
    }
  );
});

// =====================================================================================================================
// loadBatchStateFromDynamoDB - kinesis - sequencingRequired true
// =====================================================================================================================

test('loadBatchStateFromDynamoDB for kinesis - 1 msg, 1 rejected msg & 2 unusable recs - sequencing required', t => {
  const f = kinesisFixture(true);
  const taskFactory = f.context.taskFactory;

  const data = require('./persisting.test.json');

  function validateGetParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|MyStream_DEV|my-function:dev';
    const expectedShardOrEventId = 'S|shardId-333333333333';
    const expectedKey = {streamConsumerId: expectedStreamConsumerId, shardOrEventID: expectedShardOrEventId};
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.deepEqual(params.Key, expectedKey, `params.Key must be ${JSON.stringify(expectedKey)}`);
  }

  f.context.dynamoDBDocClient = mockDynamoDBDocClient(t, 'persisting.test.js', 5, {
    get: {result: data, validateArgs: validateGetParams}
  });

  const task = taskFactory.createTask(TaskDef.defineTask(loadBatchStateFromDynamoDB.name, loadBatchStateFromDynamoDB));

  const batch = new Batch([f.badRec1, f.badRec2, f.record1, f.record2], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, 'Bad apple', f.context);
  f.uRec2 = batch.addUnusableRecord(f.badRec2, undefined, 'Unparseable', f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);
  batch.addMessage(f.msg2, f.record2, undefined, f.context);

  batch.reviveTasks(f.context);

  batch.records.map((rec, i) => console.log(`##### rec [${i}] ${JSON.stringify(rec)}`));

  const executeResult = task.execute(batch, f.context);
  return executeResult.then(
    result => {
      t.pass(`loadBatchStateFromDynamoDB must succeed - result ${stringify(result)}`);
      t.ok(task.started, `Task (${task.name}) must be started`);
      t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
      t.equal(task.error, undefined, `Task (${task.name}) error must be undefined`);

      t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
      t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

      t.ok(task.donePromise, `Task (${task.name}) donePromise must not be undefined`);
      t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
      return task.donePromise.then(
        doneResult => {
          t.equal(doneResult, data, `Task (${task.name}) donePromise result must be data`);
          t.equal(doneResult, result, `Task (${task.name}) donePromise result must be executeResult result`);

          t.ok(task.completed, `Task (${task.name}) must be completed`);
          t.equal(task.result, result, `Task (${task.name}) result must be result`);
          t.end();
        },
        err => {
          t.fail(`Task (${task.name}) donePromise must NOT reject`, err);
          t.end();
        }
      );
    },
    err => {
      t.fail(`loadBatchStateFromDynamoDB must NOT fail`, err);
      t.end();
    }
  );
});

// =====================================================================================================================
// loadBatchStateFromDynamoDB - kinesis - sequencingRequired false
// =====================================================================================================================

test('loadBatchStateFromDynamoDB for kinesis - 2 msgs, 2 rejected msgs & 2 unusable recs - sequencing NOT required', t => {
  const f = kinesisFixture(false);
  const taskFactory = f.context.taskFactory;

  const data = require('./persisting-unseq.test.json');

  function validateGetParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|MyStream_DEV|my-function:dev';
    const expectedShardOrEventId = 'S|shardId-600000000000';
    const expectedKey = {streamConsumerId: expectedStreamConsumerId, shardOrEventID: expectedShardOrEventId};
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.deepEqual(params.Key, expectedKey, `params.Key must be ${JSON.stringify(expectedKey)}`);
  }

  f.context.dynamoDBDocClient = mockDynamoDBDocClient(t, 'persisting.test.js', 5, {
    get: {result: data, validateArgs: validateGetParams}
  });

  const task = taskFactory.createTask(TaskDef.defineTask(loadBatchStateFromDynamoDB.name, loadBatchStateFromDynamoDB));

  const batch = new Batch([f.badRec1, f.badRec2, f.record1, f.record2, f.record4, f.record5], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, 'Bad apple', f.context);
  f.uRec2 = batch.addUnusableRecord(f.badRec2, undefined, 'Unparseable', f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);
  batch.addMessage(f.msg2, f.record2, undefined, f.context);
  batch.addMessage(f.msg4, f.record4, undefined, f.context);
  batch.addMessage(f.msg5, f.record5, undefined, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = Promise.resolve(undefined); // executeTasks(f, batch, t);

  promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      return executeResult.then(
        result => {
          t.pass(`loadBatchStateFromDynamoDB must succeed - result ${stringify(result)}`);
          t.ok(task.started, `Task (${task.name}) must be started`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.equal(task.error, undefined, `Task (${task.name}) error must be undefined`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must not be undefined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          return task.donePromise.then(
            doneResult => {
              t.equal(doneResult, data, `Task (${task.name}) donePromise result must be data`);
              t.equal(doneResult, result, `Task (${task.name}) donePromise result must be executeResult result`);

              t.ok(task.completed, `Task (${task.name}) must be completed`);
              t.equal(task.result, result, `Task (${task.name}) result must be result`);
              t.end();
            },
            err => {
              t.fail(`Task (${task.name}) donePromise must NOT reject`, err);
              t.end();
            }
          );
        },
        err => {
          t.fail(`loadBatchStateFromDynamoDB must NOT fail`, err);
          t.end();
        }
      );
    },
    err => {
      t.end(err);
    }
  );
});

// =====================================================================================================================
// loadBatchStateFromDynamoDB - kinesis - with simulated DynamoDB error
// =====================================================================================================================

test('loadBatchStateFromDynamoDB for kinesis - 1 msg, 1 rejected msg & 2 unusable recs with DynamoDB error', t => {
  const f = kinesisFixture(true);
  const taskFactory = f.context.taskFactory;

  function validateGetParams(t, params) {
    const expectedTableName = 'TEST_StreamConsumerBatchState_DEV';
    const expectedStreamConsumerId = 'K|MyStream_DEV|my-function:dev';
    const expectedShardOrEventId = 'S|shardId-333333333333';
    const expectedKey = {streamConsumerId: expectedStreamConsumerId, shardOrEventID: expectedShardOrEventId};
    t.equal(params.TableName, expectedTableName, `params.TableName must be ${expectedTableName}`);
    t.deepEqual(params.Key, expectedKey, `params.Key must be ${JSON.stringify(expectedKey)}`);
  }

  const dynamoDBError = new Error('Planned DynamoDB error');
  f.context.dynamoDBDocClient = mockDynamoDBDocClient(t, 'persisting.test.js', 5, {
    get: {error: dynamoDBError, validateArgs: validateGetParams}
  });

  const task = taskFactory.createTask(TaskDef.defineTask(loadBatchStateFromDynamoDB.name, loadBatchStateFromDynamoDB));

  const batch = new Batch([f.badRec1, f.badRec2, f.record1, f.record2], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, 'Bad apple', f.context);
  f.uRec2 = batch.addUnusableRecord(f.badRec2, undefined, 'Unparseable', f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);
  batch.addMessage(f.msg2, f.record2, undefined, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = Promise.resolve(undefined); // executeTasks(f, batch, t);

  promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      executeResult.then(
        result => {
          t.fail(`loadBatchStateFromDynamoDB must NOT succeed - result ${stringify(result)}`);
          t.end();
        },
        err => {
          t.pass(`loadBatchStateFromDynamoDB must fail`);
          t.equal(err, dynamoDBError, `Error ${err} must be dynamoDBError`);
          t.ok(task.started, `Task (${task.name}) must be started`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.equal(task.error, undefined, `Task (${task.name}) error must be undefined`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must not be undefined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          task.donePromise.then(
            doneResult => {
              t.fail(`Task (${task.name}) donePromise must NOT resolve with:`, doneResult);
              t.end();
            },
            doneErr => {
              t.equal(doneErr, dynamoDBError, `Task (${task.name}) donePromise error must be dynamoDBError`);

              t.ok(task.failed, `Task (${task.name}) must be failed`);
              t.equal(task.result, undefined, `Task (${task.name}) result must still be undefined`);
              t.equal(task.error, dynamoDBError, `Task (${task.name}) error must be dynamoDBError`);

              t.end();
            }
          );
        }
      );
    },
    err => {
      t.end(err);
    }
  );
});
