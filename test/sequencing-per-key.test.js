'use strict';

/**
 * Unit tests for aws-stream-consumer-core/sequencing.js
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const sequencing = require('aws-stream-consumer-core/sequencing');
const sorting = require('core-functions/sorting');
const SortType = sorting.SortType;

const samples = require('./samples');
const sampleKinesisMessageAndRecord = samples.sampleKinesisMessageAndRecord;

const Batch = require('aws-stream-consumer-core/batch');

// Setting-related utilities
const Settings = require('aws-stream-consumer-core/settings');
const StreamType = Settings.StreamType;

const strings = require('core-functions/strings');
const stringify = strings.stringify;

const identify = require('../kinesis-identify');

const logging = require('logging-utils');
const LogLevel = logging.LogLevel;

const eventSourceARN = samples.sampleKinesisEventSourceArn('us-west-2', 'TestStream_DEV');

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

function createContext(streamType, idPropertyNames, keyPropertyNames, seqNoPropertyNames) {
  const context = {
    streamProcessing: {
      streamType: streamType,
      sequencingRequired: true,
      sequencingPerKey: true,
      consumerIdSuffix: undefined,
      idPropertyNames: idPropertyNames,
      keyPropertyNames: keyPropertyNames,
      seqNoPropertyNames: seqNoPropertyNames,
      generateMD5s: identify.generateKinesisMD5s,
      resolveEventIdAndSeqNos: identify.resolveKinesisEventIdAndSeqNos,
      resolveMessageIdsAndSeqNos: identify.resolveKinesisMessageIdsAndSeqNos,
      discardUnusableRecord: discardUnusableRecord,
      discardRejectedMessage: discardRejectedMessage
    }
  };
  logging.configureLogging(context, {logLevel: LogLevel.TRACE});
  return context;
}

function checkMsgSeqNoPart(t, seqNos, m, p, expectedValue, expectedOldValue, expectedSortType) {
  const seqNoPart = seqNos[p];
  const key = seqNoPart[0];

  const actualValue = seqNoPart[1];
  const opts = {quoteStrings: true};
  if (actualValue instanceof Date) {
    t.equal(actualValue.toISOString(), expectedValue, `msg${m} part[${p}] key(${key}): new value Date(${stringify(actualValue, opts)}).toISOString() must be ${stringify(expectedValue, opts)}`);
  } else {
    t.equal(actualValue, expectedValue, `msg${m} part[${p}] key(${key}): new value (${stringify(actualValue, opts)}) must be ${stringify(expectedValue, opts)}`);
  }
  t.equal(seqNoPart.oldValue, expectedOldValue, `msg${m} part[${p}] key(${key}): old value (${stringify(seqNoPart.oldValue, opts)}) must be ${stringify(expectedOldValue, opts)}`);
  t.equal(seqNoPart.sortable.sortType, expectedSortType, `msg${m} part[${p}] key(${key}): sortable.sortType (${stringify(seqNoPart.sortable.sortType, opts)}) must be ${stringify(expectedSortType, opts)}`);
}

// =====================================================================================================================
// prepareMessagesForSequencing - kinesis
// =====================================================================================================================

test(`prepareMessagesForSequencing for kinesis`, t => {
  const context = createContext(StreamType.kinesis, [], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4']);

  const shardId1 = 'shardId-000000000000';
  const seqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1.1, 100, 3000, '2017-01-17T23:59:59.001Z');

  const shardId2 = 'shardId-000000000000';
  const seqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 2.5, 200, '20000000000000000000002', '2017-01-17T23:59:59.002Z');

  const shardId3 = 'shardId-000000000000';
  const seqNo3 = '49545115243490985018280067714973144582180062593244200901';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 3.3, 300, '30000000000000000000003', '2017-01-17T23:59:59.003Z');

  const batch = new Batch([rec1, rec2, rec3], [], [], context);
  const states = batch.states;
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  const msgs = batch.messages;
  // const msgs = [msg1, msg2, msg3];

  sequencing.prepareMessagesForSequencing(msgs, states, context);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 0, 1.1, 1.1, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 0, 2.5, 2.5, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 0, 3.3, 3.3, SortType.NUMBER);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 1, 100, 100, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 1, 200, 200, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 1, 300, 300, SortType.NUMBER);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 2, '3000', 3000, SortType.INTEGER_LIKE);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 2, '20000000000000000000002', '20000000000000000000002', SortType.INTEGER_LIKE);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 2, '30000000000000000000003', '30000000000000000000003', SortType.INTEGER_LIKE);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 3, '2017-01-17T23:59:59.001Z', '2017-01-17T23:59:59.001Z', SortType.DATE_TIME);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 3, '2017-01-17T23:59:59.002Z', '2017-01-17T23:59:59.002Z', SortType.DATE_TIME);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 3, '2017-01-17T23:59:59.003Z', '2017-01-17T23:59:59.003Z', SortType.DATE_TIME);

  t.end();
});

test(`prepareMessagesForSequencing for kinesis with sequence numbers with different keys & sequencing required`, t => {
  const context1 = createContext('kinesis', [], ['k2', 'k1'], ['n3', 'n2', 'n1']);
  const context3 = createContext('kinesis', [], ['k2', 'k1'], ['n1', 'n2', 'n3']);

  const shardId1 = 'shardId-000000000000';
  const seqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1.1, 100, 3000);

  const shardId2 = 'shardId-000000000000';
  const seqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, '2.5', '200', '20000000000000000000002');

  const shardId3 = 'shardId-000000000000';
  const seqNo3 = '49545115243490985018280067714973144582180062593244200901';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 3.3, 300, '30000000000000000000003');

  const batch = new Batch([rec1, rec2, rec3], [], [], context1);
  const states = batch.states;
  batch.addMessage(msg2, rec2, undefined, context1);
  batch.addMessage(msg1, rec1, undefined, context1);
  batch.addMessage(msg3, rec3, undefined, context3); // different context!

  const msgs = batch.messages;
  // const msgs = [msg2, msg1, msg3];

  t.throws(() => sequencing.prepareMessagesForSequencing(msgs, states, context3), /NOT all of the messages have the same key at sequence number part\[0\]/, `different seq number keys must throw an error`);

  t.end();
});

test(`prepareMessagesForSequencing for kinesis with sequence numbers with different keys & sequencing NOT required`, t => {
  const context1 = createContext('kinesis', [], ['k2', 'k1'], ['n3', 'n2', 'n1', 'n4', 'n5']);
  context1.streamProcessing.sequencingRequired = false;

  const context3 = createContext('kinesis', [], ['k2', 'k1'], ['n1', 'n2', 'n3', 'n4']); // msg3 has NO 'n5'
  context3.streamProcessing.sequencingRequired = false;

  const shardId1 = 'shardId-000000000000';
  const seqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1.1, 100, 3000, '2016-12-30', 'zZz');

  const shardId2 = 'shardId-000000000000';
  const seqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 2.5, 200, '20000000000000000000002', '2016-12-31', 'Abc');

  const shardId3 = 'shardId-000000000000';
  const seqNo3 = '49545115243490985018280067714973144582180062593244200901';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 3.3, 300, '30000000000000000000003', '2017-01-01');

  const batch = new Batch([rec1, rec2, rec3], [], [], context3);
  const states = batch.states;
  batch.addMessage(msg1, rec1, undefined, context1);
  batch.addMessage(msg2, rec2, undefined, context1);
  batch.addMessage(msg3, rec3, undefined, context3); // different context!

  const msgs = batch.messages; // [msg1, msg2, msg3];

  sequencing.prepareMessagesForSequencing(msgs, states, context3);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 0, '3000', 3000, SortType.INTEGER_LIKE);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 0, '20000000000000000000002', '20000000000000000000002', SortType.INTEGER_LIKE);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 0, 3.3, 3.3, SortType.NUMBER);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 1, 100, 100, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 1, 200, 200, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 1, 300, 300, SortType.NUMBER);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 2, 1.1, 1.1, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 2, 2.5, 2.5, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 2, '30000000000000000000003', '30000000000000000000003', SortType.INTEGER_LIKE);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 3, '2016-12-30T00:00:00.000Z', '2016-12-30', SortType.DATE);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 3, '2016-12-31T00:00:00.000Z', '2016-12-31', SortType.DATE);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 3, '2017-01-01T00:00:00.000Z', '2017-01-01', SortType.DATE);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 4, 'zZz', 'zZz', SortType.STRING);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 4, 'Abc', 'Abc', SortType.STRING);

  t.end();
});

// =====================================================================================================================
// compareSameKeyMessages - kinesis
// =====================================================================================================================

test(`compareSameKeyMessages for kinesis with NO sequence property names`, t => {
  const context = createContext('kinesis', [], ['k1', 'k2'], []);

  const shardId1 = 'shardId-000000000000';
  const seqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId2 = 'shardId-000000000000';
  const seqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');

  const shardId3 = 'shardId-000000000000';
  const seqNo3 = '49545115243490985018280067714973144582180062593244200901';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.003Z');

  const batch = new Batch([rec1, rec2, rec3], [], [], context);
  const states = batch.states;
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  const msgs = batch.messages;
  // const msgs = [msg1, msg2, msg3];

  // Normalize all of the messages' sequence numbers
  sequencing.prepareMessagesForSequencing(msgs, states, context);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg1, states, context) === 0, `msg 1 = msg 1`);
  t.ok(sequencing.compareSameKeyMessages(msg2, msg2, states, context) === 0, `msg 2 = msg 2`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg3, states, context) === 0, `msg 3 = msg 3`);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg2, states, context) < 0, `msg 1 < msg 2`);
  t.ok(sequencing.compareSameKeyMessages(msg2, msg1, states, context) > 0, `msg 2 > msg 1`);

  t.ok(sequencing.compareSameKeyMessages(msg2, msg3, states, context) > 0, `msg 2 > msg 3`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg2, states, context) < 0, `msg 3 < msg 2`);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg3, states, context) > 0, `msg 1 > msg 3`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg1, states, context) < 0, `msg 3 < msg 1`);

  t.end();
});

test(`compareSameKeyMessages for kinesis with key & sequence property names with difference in 4th seq # part`, t => {
  const context = createContext('kinesis', [], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4']);

  const shardId1 = 'shardId-000000000000';
  const seqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId2 = 'shardId-000000000000';
  const seqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');

  const shardId3 = 'shardId-000000000000';
  const seqNo3 = '49545115243490985018280067714973144582180062593244200901';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.003Z');

  const batch = new Batch([rec1, rec2, rec3], [], [], context);
  const states = batch.states;
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  const msgs = batch.messages;
  // const msgs = [msg1, msg2, msg3];

  // Normalize all of the messages' sequence numbers
  sequencing.prepareMessagesForSequencing(msgs, states, context);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg1, states, context) === 0, `msg 1 = msg 1`);
  t.ok(sequencing.compareSameKeyMessages(msg2, msg2, states, context) === 0, `msg 2 = msg 2`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg3, states, context) === 0, `msg 3 = msg 3`);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg2, states, context) < 0, `msg 1 < msg 2`);
  t.ok(sequencing.compareSameKeyMessages(msg2, msg1, states, context) > 0, `msg 2 > msg 1`);

  t.ok(sequencing.compareSameKeyMessages(msg2, msg3, states, context) < 0, `msg 2 < msg 3`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg2, states, context) > 0, `msg 3 > msg 2`);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg3, states, context) < 0, `msg 1 < msg 3`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg1, states, context) > 0, `msg 3 > msg 1`);

  t.end();
});

test(`compareSameKeyMessages for kinesis with key & sequence property names with difference in 3rd seq # part`, t => {
  const context = createContext('kinesis', [], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4']);

  const shardId1 = 'shardId-000000000000';
  const seqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId2 = 'shardId-000000000000';
  const seqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000002', '2017-01-17T23:59:59.001Z');

  const shardId3 = 'shardId-000000000000';
  const seqNo3 = '49545115243490985018280067714973144582180062593244200901';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000003', '2017-01-17T23:59:59.001Z');

  const batch = new Batch([rec1, rec2, rec3], [], [], context);
  const states = batch.states;
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  const msgs = batch.messages; // [msg1, msg2, msg3];

  // Normalize all of the messages' sequence numbers
  sequencing.prepareMessagesForSequencing(msgs, states, context);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg1, states, context) === 0, `msg 1 = msg 1`);
  t.ok(sequencing.compareSameKeyMessages(msg2, msg2, states, context) === 0, `msg 2 = msg 2`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg3, states, context) === 0, `msg 3 = msg 3`);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg2, states, context) < 0, `msg 1 < msg 2`);
  t.ok(sequencing.compareSameKeyMessages(msg2, msg1, states, context) > 0, `msg 2 > msg 1`);

  t.ok(sequencing.compareSameKeyMessages(msg2, msg3, states, context) < 0, `msg 2 < msg 3`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg2, states, context) > 0, `msg 3 > msg 2`);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg3, states, context) < 0, `msg 1 < msg 3`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg1, states, context) > 0, `msg 3 > msg 1`);

  t.end();
});

test(`compareSameKeyMessages for kinesis with key & sequence property names with difference in 2nd seq # part`, t => {
  const context = createContext('kinesis', [], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4']);
  const shardId1 = 'shardId-000000000000';
  const seqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 103, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId2 = 'shardId-000000000000';
  const seqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 102, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId3 = 'shardId-000000000000';
  const seqNo3 = '49545115243490985018280067714973144582180062593244200901';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 101, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const batch = new Batch([rec1, rec2, rec3], [], [], context);
  const states = batch.states;
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  const msgs = batch.messages; // [msg1, msg2, msg3];

  // Normalize all of the messages' sequence numbers
  sequencing.prepareMessagesForSequencing(msgs, states, context);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg1, states, context) === 0, `msg 1 = msg 1`);
  t.ok(sequencing.compareSameKeyMessages(msg2, msg2, states, context) === 0, `msg 2 = msg 2`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg3, states, context) === 0, `msg 3 = msg 3`);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg2, states, context) > 0, `msg 1 > msg 2`);
  t.ok(sequencing.compareSameKeyMessages(msg2, msg1, states, context) < 0, `msg 2 < msg 1`);

  t.ok(sequencing.compareSameKeyMessages(msg2, msg3, states, context) > 0, `msg 2 > msg 3`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg2, states, context) < 0, `msg 3 < msg 2`);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg3, states, context) > 0, `msg 1 > msg 3`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg1, states, context) < 0, `msg 3 < msg 1`);

  t.end();
});

test(`compareSameKeyMessages for kinesis with key & sequence property names with difference in 1st seq # part`, t => {
  const context = createContext('kinesis', [], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4']);
  const shardId1 = 'shardId-000000000000';
  const seqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 3, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId2 = 'shardId-000000000000';
  const seqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId3 = 'shardId-000000000000';
  const seqNo3 = '49545115243490985018280067714973144582180062593244200901';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 2, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const batch = new Batch([rec1, rec2, rec3], [], [], context);
  const states = batch.states;
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  const msgs = batch.messages; // [msg1, msg2, msg3];

  // Normalize all of the messages' sequence numbers
  sequencing.prepareMessagesForSequencing(msgs, states, context);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg1, states, context) === 0, `msg 1 = msg 1`);
  t.ok(sequencing.compareSameKeyMessages(msg2, msg2, states, context) === 0, `msg 2 = msg 2`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg3, states, context) === 0, `msg 3 = msg 3`);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg2, states, context) > 0, `msg 1 > msg 2`);
  t.ok(sequencing.compareSameKeyMessages(msg2, msg1, states, context) < 0, `msg 2 < msg 1`);

  t.ok(sequencing.compareSameKeyMessages(msg2, msg3, states, context) < 0, `msg 2 < msg 3`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg2, states, context) > 0, `msg 3 > msg 2`);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg3, states, context) > 0, `msg 1 > msg 3`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg1, states, context) < 0, `msg 3 < msg 1`);

  t.end();
});

test(`compareSameKeyMessages for kinesis with key & sequence property names with no differences in any seq # part`, t => {
  const context = createContext('kinesis', [], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4']);

  const shardId1 = 'shardId-000000000000';
  const seqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId2 = 'shardId-000000000000';
  const seqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId3 = 'shardId-000000000000';
  const seqNo3 = '49545115243490985018280067714973144582180062593244200901';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const batch = new Batch([rec1, rec2, rec3], [], [], context);
  const states = batch.states;
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  const msgs = batch.messages; // [msg1, msg2, msg3];

  // Normalize all of the messages' sequence numbers
  sequencing.prepareMessagesForSequencing(msgs, states, context);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg1, states, context) === 0, `msg 1 = msg 1`);
  t.ok(sequencing.compareSameKeyMessages(msg2, msg2, states, context) === 0, `msg 2 = msg 2`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg3, states, context) === 0, `msg 3 = msg 3`);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg2, states, context) === 0, `msg 1 = msg 2`);
  t.ok(sequencing.compareSameKeyMessages(msg2, msg1, states, context) === 0, `msg 2 = msg 1`);

  t.ok(sequencing.compareSameKeyMessages(msg2, msg3, states, context) === 0, `msg 2 = msg 3`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg2, states, context) === 0, `msg 3 = msg 2`);

  t.ok(sequencing.compareSameKeyMessages(msg1, msg3, states, context) === 0, `msg 1 = msg 3`);
  t.ok(sequencing.compareSameKeyMessages(msg3, msg1, states, context) === 0, `msg 3 = msg 1`);

  t.end();
});

// =====================================================================================================================
// sequenceMessages - kinesis
// =====================================================================================================================

test(`sequenceMessages for kinesis with NO sequence property names`, t => {
  const context = createContext('kinesis', [], ['k1', 'k2'], []);

  // 3 messages with the same key differing ONLY in sequence number)
  const shardId1 = 'shardId-000000000000';
  const seqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId2 = 'shardId-000000000000';
  const seqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');

  const shardId3 = 'shardId-000000000000';
  const seqNo3 = '49545115243490985018280067714973144582180062593244200901';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.003Z');

  // 2 messages with a different key (differing ONLY in sequence number from each other)
  const shardId4 = 'shardId-000000000000';
  const seqNo4 = '49545115243490985018280067714973144582180062593244200961';
  const [msg4, rec4] = sampleKinesisMessageAndRecord(shardId4, seqNo4, eventSourceARN, undefined, undefined, 'ABC', 11, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId5 = 'shardId-000000000000';
  const seqNo5 = '49545115243490985018280067714973144582180062593244200963';
  const [msg5, rec5] = sampleKinesisMessageAndRecord(shardId5, seqNo5, eventSourceARN, undefined, undefined, 'ABC', 11, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');

  // 1 message with a different key
  const shardId6 = 'shardId-000000000000';
  const seqNo6 = '49545115243490985018280067714973144582180062593244200901';
  const [msg6, rec6] = sampleKinesisMessageAndRecord(shardId6, seqNo6, eventSourceARN, undefined, undefined, 'XYZ', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.003Z');

  const batch = new Batch([rec1, rec2, rec3, rec4, rec5, rec6], [], [], context);
  const states = batch.states;
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg5, rec5, undefined, context);
  batch.addMessage(msg6, rec6, undefined, context);
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg4, rec4, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  // Sequence the messages
  const firstMessagesToProcess = sequencing.sequenceMessages(batch, context);

  const msgState1 = states.get(msg1);
  const msgState2 = states.get(msg2);
  const msgState3 = states.get(msg3);
  const msgState4 = states.get(msg4);
  const msgState5 = states.get(msg5);
  const msgState6 = states.get(msg6);

  // expected sequence: [msg3, msg1, msg2]
  t.equal(msgState3.prevMessage, undefined, `msg3.prevMessage must be undefined`);
  t.equal(msgState3.nextMessage, msg1, `msg3.nextMessage must be msg1`);
  t.equal(msgState1.prevMessage, msg3, `msg1.prevMessage must be msg3`);
  t.equal(msgState1.nextMessage, msg2, `msg1.nextMessage must be msg2`);
  t.equal(msgState2.prevMessage, msg1, `msg2.prevMessage must be msg1`);
  t.equal(msgState2.nextMessage, undefined, `msg2.nextMessage must be undefined`);

  // expected sequence: [msg4, msg5]
  t.equal(msgState4.prevMessage, undefined, `msg4.prevMessage must be undefined`);
  t.equal(msgState4.nextMessage, msg5, `msg4.nextMessage must be msg5`);
  t.equal(msgState5.prevMessage, msg4, `msg5.prevMessage must be msg4`);
  t.equal(msgState5.nextMessage, undefined, `msg5.nextMessage must be undefined`);

  // expected sequence: [msg6]
  t.equal(msgState6.prevMessage, undefined, `msg6.prevMessage must be undefined`);
  t.equal(msgState6.nextMessage, undefined, `msg6.nextMessage must be undefined`);

  t.equal(firstMessagesToProcess.length, 3, `firstMessagesToProcess must have 3 messages`);
  t.notEqual(firstMessagesToProcess.indexOf(msg3), -1, `firstMessagesToProcess must contain msg3`);
  t.notEqual(firstMessagesToProcess.indexOf(msg4), -1, `firstMessagesToProcess must contain msg4`);
  t.notEqual(firstMessagesToProcess.indexOf(msg6), -1, `firstMessagesToProcess must contain msg6`);

  t.end();
});

test(`sequenceMessages for kinesis with key and sequence property names`, t => {
  const context = createContext('kinesis', [], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4']);

  // 3 messages with the same key differing ONLY in sequence number)
  const shardId1 = 'shardId-000000000000';
  const seqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId2 = 'shardId-000000000000';
  const seqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');

  const shardId3 = 'shardId-000000000000';
  const seqNo3 = '49545115243490985018280067714973144582180062593244200901';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.003Z');

  // 2 messages with a different key (differing ONLY in sequence number from each other)
  const shardId4 = 'shardId-000000000000';
  const seqNo4 = '49545115243490985018280067714973144582180062593244200961';
  const [msg4, rec4] = sampleKinesisMessageAndRecord(shardId4, seqNo4, eventSourceARN, undefined, undefined, 'ABC', 11, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId5 = 'shardId-000000000000';
  const seqNo5 = '49545115243490985018280067714973144582180062593244200963';
  const [msg5, rec5] = sampleKinesisMessageAndRecord(shardId5, seqNo5, eventSourceARN, undefined, undefined, 'ABC', 11, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');

  // 1 message with a different key
  const shardId6 = 'shardId-000000000000';
  const seqNo6 = '49545115243490985018280067714973144582180062593244200901';
  const [msg6, rec6] = sampleKinesisMessageAndRecord(shardId6, seqNo6, eventSourceARN, undefined, undefined, 'XYZ', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.003Z');

  const batch = new Batch([rec1, rec2, rec3, rec4, rec5, rec6], [], [], context);
  const states = batch.states;
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg5, rec5, undefined, context);
  batch.addMessage(msg6, rec6, undefined, context);
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg4, rec4, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  // Sequence the messages
  const firstMessagesToProcess = sequencing.sequenceMessages(batch, context);

  const msgState1 = states.get(msg1);
  const msgState2 = states.get(msg2);
  const msgState3 = states.get(msg3);
  const msgState4 = states.get(msg4);
  const msgState5 = states.get(msg5);
  const msgState6 = states.get(msg6);

  // expected sequence: [msg1, msg2, msg3]
  t.equal(msgState1.prevMessage, undefined, `msg1.prevMessage must be undefined`);
  t.equal(msgState1.nextMessage, msg2, `msg1.nextMessage must be msg2`);
  t.equal(msgState2.prevMessage, msg1, `msg2.prevMessage must be msg1`);
  t.equal(msgState2.nextMessage, msg3, `msg2.nextMessage must be msg3`);
  t.equal(msgState3.prevMessage, msg2, `msg3.prevMessage must be msg2`);
  t.equal(msgState3.nextMessage, undefined, `msg3.nextMessage must be undefined`);

  // expected sequence: [msg4, msg5]
  t.equal(msgState4.prevMessage, undefined, `msg4.prevMessage must be undefined`);
  t.equal(msgState4.nextMessage, msg5, `msg4.nextMessage must be msg5`);
  t.equal(msgState5.prevMessage, msg4, `msg5.prevMessage must be msg4`);
  t.equal(msgState5.nextMessage, undefined, `msg5.nextMessage must be undefined`);

  // expected sequence: [msg6]
  t.equal(msgState6.prevMessage, undefined, `msg6.prevMessage must be undefined`);
  t.equal(msgState6.nextMessage, undefined, `msg6.nextMessage must be undefined`);

  t.equal(firstMessagesToProcess.length, 3, `firstMessagesToProcess must have 3 messages`);
  t.notEqual(firstMessagesToProcess.indexOf(msg1), -1, `firstMessagesToProcess must contain msg1`);
  t.notEqual(firstMessagesToProcess.indexOf(msg4), -1, `firstMessagesToProcess must contain msg4`);
  t.notEqual(firstMessagesToProcess.indexOf(msg6), -1, `firstMessagesToProcess must contain msg6`);

  t.end();
});