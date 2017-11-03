'use strict';

/**
 * Unit tests for aws-stream-consumer-core/sequencing.js
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const sequencing = require('aws-stream-consumer-core/sequencing');

const samples = require('./samples');
const sampleKinesisMessageAndRecord = samples.sampleKinesisMessageAndRecord;

const Batch = require('aws-stream-consumer-core/batch');

// const strings = require('core-functions/strings');

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
      sequencingRequired: false,
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

// =====================================================================================================================
// sequenceMessages - kinesis
// =====================================================================================================================

test(`sequenceMessages for kinesis with NO sequence property names`, t => {
  const context = createContext('kinesis', [], ['k1', 'k2'], []);

  // 3 messages with the same key differing ONLY in sequence number)
  const shardId1 = 'shardId-000000000000';
  const seqNo1 = '49545115243490985018280067714973144582180062593244200962';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId2 = 'shardId-000000000000';
  const seqNo2 = '49545115243490985018280067714973144582180062593244200963';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');

  const shardId3 = 'shardId-000000000000';
  const seqNo3 = '49545115243490985018280067714973144582180062593244200960';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.003Z');

  // 2 messages with a different key (differing ONLY in sequence number from each other)
  const shardId4 = 'shardId-000000000000';
  const seqNo4 = '49545115243490985018280067714973144582180062593244200961';
  const [msg4, rec4] = sampleKinesisMessageAndRecord(shardId4, seqNo4, eventSourceARN, undefined, undefined, 'ABC', 11, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const shardId5 = 'shardId-000000000000';
  const seqNo5 = '49545115243490985018280067714973144582180062593244200900';
  const [msg5, rec5] = sampleKinesisMessageAndRecord(shardId5, seqNo5, eventSourceARN, undefined, undefined, 'ABC', 11, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');

  // 1 message with a different key
  const shardId6 = 'shardId-000000000000';
  const seqNo6 = '49545115243490985018280067714973144582180062593244200950';
  const [msg6, rec6] = sampleKinesisMessageAndRecord(shardId6, seqNo6, eventSourceARN, undefined, undefined, 'XYZ', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.003Z');

  // expected sequence: [msg5, msg6, msg3, msg4, msg1, msg2]

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

  // expected sequence: NONE

  t.equal(msgState5.prevMessage, undefined, `msg5.prevMessage must be undefined`);

  t.equal(msgState5.nextMessage, undefined, `msg5.nextMessage must be undefined`);
  t.equal(msgState6.prevMessage, undefined, `msg6.prevMessage must be undefined`);

  t.equal(msgState6.nextMessage, undefined, `msg6.nextMessage must be undefined`);
  t.equal(msgState3.prevMessage, undefined, `msg3.prevMessage must be undefined`);

  t.equal(msgState3.nextMessage, undefined, `msg3.nextMessage must be undefined`);
  t.equal(msgState4.prevMessage, undefined, `msg4.prevMessage must be undefined`);

  t.equal(msgState4.nextMessage, undefined, `msg4.nextMessage must be undefined`);
  t.equal(msgState1.prevMessage, undefined, `msg1.prevMessage must be undefined`);

  t.equal(msgState1.nextMessage, undefined, `msg1.nextMessage must be undefined`);
  t.equal(msgState2.prevMessage, undefined, `msg2.prevMessage must be undefined`);

  t.equal(msgState2.nextMessage, undefined, `msg2.nextMessage must be undefined`);

  t.equal(firstMessagesToProcess.length, 6, `firstMessagesToProcess must have 6 messages`);
  t.notEqual(firstMessagesToProcess.indexOf(msg1), -1, `firstMessagesToProcess must contain msg1`);
  t.notEqual(firstMessagesToProcess.indexOf(msg2), -1, `firstMessagesToProcess must contain msg2`);
  t.notEqual(firstMessagesToProcess.indexOf(msg3), -1, `firstMessagesToProcess must contain msg3`);
  t.notEqual(firstMessagesToProcess.indexOf(msg4), -1, `firstMessagesToProcess must contain msg4`);
  t.notEqual(firstMessagesToProcess.indexOf(msg5), -1, `firstMessagesToProcess must contain msg5`);
  t.notEqual(firstMessagesToProcess.indexOf(msg6), -1, `firstMessagesToProcess must contain msg6`);

  t.end();
});

test(`sequenceMessages for kinesis with key and sequence property names`, t => {
  const context = createContext('kinesis', [], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4']);

  // 3 messages with the same key differing ONLY in sequence number)
  const shardId1 = 'shardId-000000000000';
  const seqNo1 = '49545115243490985018280067714973144582180062593244200961';
  const [msg1, rec1] = sampleKinesisMessageAndRecord(shardId1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.003Z');

  const shardId2 = 'shardId-000000000000';
  const seqNo2 = '49545115243490985018280067714973144582180062593244200962';
  const [msg2, rec2] = sampleKinesisMessageAndRecord(shardId2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.005Z');

  const shardId3 = 'shardId-000000000000';
  const seqNo3 = '49545115243490985018280067714973144582180062593244200901';
  const [msg3, rec3] = sampleKinesisMessageAndRecord(shardId3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.000Z');

  // 2 messages with a different key (differing ONLY in sequence number from each other)
  const shardId4 = 'shardId-000000000000';
  const seqNo4 = '49545115243490985018280067714973144582180062593244200961';
  const [msg4, rec4] = sampleKinesisMessageAndRecord(shardId4, seqNo4, eventSourceARN, undefined, undefined, 'ABC', 11, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.004Z');

  const shardId5 = 'shardId-000000000000';
  const seqNo5 = '49545115243490985018280067714973144582180062593244200963';
  const [msg5, rec5] = sampleKinesisMessageAndRecord(shardId5, seqNo5, eventSourceARN, undefined, undefined, 'ABC', 11, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');

  // 1 message with a different key
  const shardId6 = 'shardId-000000000000';
  const seqNo6 = '49545115243490985018280067714973144582180062593244200901';
  const [msg6, rec6] = sampleKinesisMessageAndRecord(shardId6, seqNo6, eventSourceARN, undefined, undefined, 'XYZ', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

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

  // expected sequence: NONE

  t.equal(msgState5.prevMessage, undefined, `msg5.prevMessage must be undefined`);

  t.equal(msgState5.nextMessage, undefined, `msg5.nextMessage must be undefined`);
  t.equal(msgState6.prevMessage, undefined, `msg6.prevMessage must be undefined`);

  t.equal(msgState6.nextMessage, undefined, `msg6.nextMessage must be undefined`);
  t.equal(msgState3.prevMessage, undefined, `msg3.prevMessage must be undefined`);

  t.equal(msgState3.nextMessage, undefined, `msg3.nextMessage must be undefined`);
  t.equal(msgState4.prevMessage, undefined, `msg4.prevMessage must be undefined`);

  t.equal(msgState4.nextMessage, undefined, `msg4.nextMessage must be undefined`);
  t.equal(msgState1.prevMessage, undefined, `msg1.prevMessage must be undefined`);

  t.equal(msgState1.nextMessage, undefined, `msg1.nextMessage must be undefined`);
  t.equal(msgState2.prevMessage, undefined, `msg2.prevMessage must be undefined`);

  t.equal(msgState2.nextMessage, undefined, `msg2.nextMessage must be undefined`);

  t.equal(firstMessagesToProcess.length, 6, `firstMessagesToProcess must have 6 messages`);
  t.notEqual(firstMessagesToProcess.indexOf(msg1), -1, `firstMessagesToProcess must contain msg1`);
  t.notEqual(firstMessagesToProcess.indexOf(msg2), -1, `firstMessagesToProcess must contain msg2`);
  t.notEqual(firstMessagesToProcess.indexOf(msg3), -1, `firstMessagesToProcess must contain msg3`);
  t.notEqual(firstMessagesToProcess.indexOf(msg4), -1, `firstMessagesToProcess must contain msg4`);
  t.notEqual(firstMessagesToProcess.indexOf(msg5), -1, `firstMessagesToProcess must contain msg5`);
  t.notEqual(firstMessagesToProcess.indexOf(msg6), -1, `firstMessagesToProcess must contain msg6`);

  t.end();
});