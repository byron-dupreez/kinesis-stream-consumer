'use strict';

/**
 * Unit tests for aws-stream-consumer-core/identify.js
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const kinesisIdentify = require('../kinesis-identify');

const samples = require('./samples');
const sampleKinesisMessageAndRecord = samples.sampleKinesisMessageAndRecord;

const strings = require('core-functions/strings');
const stringify = strings.stringify;

const logging = require('logging-utils');
const LogLevel = logging.LogLevel;

const eventSourceARN = samples.sampleKinesisEventSourceArn('us-west-2', 'TestStream_DEV');

const crypto = require('crypto');

function md5Sum(data) {
  return crypto.createHash('md5').update(data).digest("hex");
}

function createContext(streamType, idPropertyNames, keyPropertyNames, seqNoPropertyNames) {
  const context = {
    streamProcessing: {
      streamType: streamType,
      sequencingRequired: true,
      sequencingPerKey: true,
      consumerIdSuffix: undefined,
      // consumerId: 'consumer',
      idPropertyNames: idPropertyNames,
      keyPropertyNames: keyPropertyNames,
      seqNoPropertyNames: seqNoPropertyNames,
      generateMD5s: kinesisIdentify.generateKinesisMD5s,
      resolveEventIdAndSeqNos: kinesisIdentify.resolveKinesisEventIdAndSeqNos,
      resolveMessageIdsAndSeqNos: kinesisIdentify.resolveKinesisMessageIdsAndSeqNos
    }
  };
  logging.configureLogging(context, {logLevel: LogLevel.TRACE});
  return context;
}

function checkEventIdAndSeqNos(t, eventIdAndSeqNos, expectedEventID, expectedEventSeqNo, expectedEventSubSeqNo) {
  t.equal(eventIdAndSeqNos.eventID, expectedEventID, `eventID must be '${expectedEventID}'`);
  t.equal(eventIdAndSeqNos.eventSeqNo, expectedEventSeqNo, `eventSeqNo must be '${expectedEventSeqNo}'`);
  t.equal(eventIdAndSeqNos.eventSubSeqNo, expectedEventSubSeqNo, `eventSubSeqNo must be '${expectedEventSubSeqNo}'`);
}

function checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos) {
  t.deepEqual(messageIdsAndSeqNos.ids, expectedIds, `ids must be ${stringify(expectedIds)}`);
  t.deepEqual(messageIdsAndSeqNos.keys, expectedKeys, `keys must be ${stringify(expectedKeys)}`);
  t.deepEqual(messageIdsAndSeqNos.seqNos, expectedSeqNos, `seqNos must be ${stringify(expectedSeqNos)}`);
}

function resolveMessageIdsAndSeqNos(msg, rec, userRec, context) {
  const md5s = kinesisIdentify.generateKinesisMD5s(msg, rec, userRec);
  const eventIdAndSeqNos = kinesisIdentify.resolveKinesisEventIdAndSeqNos(rec, userRec);
  return kinesisIdentify.resolveKinesisMessageIdsAndSeqNos(msg, rec, userRec, eventIdAndSeqNos, md5s, context);
}

// =====================================================================================================================
// generateKinesisMD5s - kinesis
// =====================================================================================================================

test(`generateKinesisMD5s for kinesis`, t => {
  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200960';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  const userRec = {a: 1, b: 2, c: {d: 4}}; // Dummy user record

  const md5s = kinesisIdentify.generateKinesisMD5s(msg, rec, userRec);

  let md5 = md5Sum(JSON.stringify(msg));
  t.equal(md5s.msg, md5, `md5s.msg must be ${md5}`);

  md5 = md5Sum(JSON.stringify(rec));
  t.equal(md5s.rec, md5, `md5s.rec must be ${md5}`);

  md5 = md5Sum(rec.kinesis.data);
  t.equal(md5s.data, md5, `md5s.data must be ${md5}`);

  md5 = md5Sum(JSON.stringify(userRec));
  t.equal(md5s.userRec, md5, `md5s.userRec must be ${md5}`);

  t.end();
});

test(`generateKinesisMD5s for kinesis with no message`, t => {
  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200960';

  const [, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  const userRec = {a: 1, b: 2, c: {d: 4}}; // Dummy user record

  const md5s = kinesisIdentify.generateKinesisMD5s(undefined, rec, userRec);

  let md5 = undefined;
  t.equal(md5s.msg, md5, `md5s.msg must be ${md5}`);

  md5 = md5Sum(JSON.stringify(rec));
  t.equal(md5s.rec, md5, `md5s.rec must be ${md5}`);

  md5 = md5Sum(rec.kinesis.data);
  t.equal(md5s.data, md5, `md5s.data must be ${md5}`);

  md5 = md5Sum(JSON.stringify(userRec));
  t.equal(md5s.userRec, md5, `md5s.userRec must be ${md5}`);

  t.end();
});

test(`generateKinesisMD5s for kinesis with no message & no user record`, t => {
  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200960';

  const [, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  const md5s = kinesisIdentify.generateKinesisMD5s(undefined, rec, undefined);

  let md5 = undefined;
  t.equal(md5s.msg, md5, `md5s.msg must be ${md5}`);

  md5 = md5Sum(JSON.stringify(rec));
  t.equal(md5s.rec, md5, `md5s.rec must be ${md5}`);

  md5 = md5Sum(rec.kinesis.data);
  t.equal(md5s.data, md5, `md5s.data must be ${md5}`);

  md5 = undefined;
  t.equal(md5s.userRec, md5, `md5s.userRec must be ${md5}`);

  t.end();
});

test(`generateKinesisMD5s for kinesis with no message, no record & no user record`, t => {
  const md5s = kinesisIdentify.generateKinesisMD5s(undefined, undefined, undefined);

  let md5 = undefined;
  t.equal(md5s.msg, md5, `md5s.msg must be ${md5}`);

  md5 = undefined;
  t.equal(md5s.rec, md5, `md5s.rec must be ${md5}`);

  md5 = undefined;
  t.equal(md5s.data, md5, `md5s.data must be ${md5}`);

  md5 = undefined;
  t.equal(md5s.userRec, md5, `md5s.userRec must be ${md5}`);

  t.end();
});

// =====================================================================================================================
// resolveEventIdAndSeqNos - kinesis
// =====================================================================================================================

test(`resolveKinesisEventIdAndSeqNos for kinesis`, t => {
  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: '007'}; // Dummy user record

  const eventIdAndSeqNos = kinesisIdentify.resolveKinesisEventIdAndSeqNos(rec, userRec);

  checkEventIdAndSeqNos(t, eventIdAndSeqNos, rec.eventID, rec.kinesis.sequenceNumber, userRec.subSequenceNumber);

  t.end();
});

test(`resolveKinesisEventIdAndSeqNos for kinesis with no user record`, t => {
  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  const eventIdAndSeqNos = kinesisIdentify.resolveKinesisEventIdAndSeqNos(rec, undefined);

  checkEventIdAndSeqNos(t, eventIdAndSeqNos, rec.eventID, rec.kinesis.sequenceNumber, undefined);

  t.end();
});

test(`resolveKinesisEventIdAndSeqNos for kinesis with no record & no user record`, t => {
  const eventIdAndSeqNos = kinesisIdentify.resolveKinesisEventIdAndSeqNos(undefined, undefined);

  checkEventIdAndSeqNos(t, eventIdAndSeqNos, undefined, undefined, undefined);

  t.end();
});

// =====================================================================================================================
// resolveMessageIdsAndSeqNos - kinesis
// =====================================================================================================================


test(`resolveKinesisMessageIdsAndSeqNos for kinesis with all property names configured & all properties present`, t => {
  const context = createContext('kinesis', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: '007'}; // Dummy user record

  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, userRec, context);

  const expectedIds = [['id1', '123'], ['id2', '456']];
  const expectedKeys = [['k1', 'ABC'], ['k2', 10]];
  const expectedSeqNos = [['n1', 1], ['n2', 2], ['n3', 3]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for kinesis with idPropertyNames and seqNoPropertyNames NOT configured`, t => {
  const context = createContext('kinesis', [], ['k2'], []);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';
  const eventSubSeqNo = 789;

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: eventSubSeqNo}; // Dummy user record

  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, userRec, context);

  const expectedIds = []; //[['k2', 10], ['eventSeqNo', '49545115243490985018280067714973144582180062593244200961']];
  const expectedKeys = [['k2', 10]];
  const expectedSeqNos = [['eventSeqNo', eventSeqNo], ['eventSubSeqNo', eventSubSeqNo]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for kinesis with keyPropertyNames, seqNoPropertyNames & idPropertyNames NOT configured and message sequencing required, but sequencingPerKey true`, t => {
  const context = createContext('kinesis', [], [], []);
  context.streamProcessing.sequencingPerKey = true;

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '457', 'ABC', 10, 1, 2, 3);
  const userRec = {a: 1, b: 2, c: {d: 4}}; // Dummy user record

  // Not allowed to have NO keyPropertyNames if sequencingPerKey is true!
  t.throws(() => resolveMessageIdsAndSeqNos(msg, rec, userRec, context), /FATAL - sequencingPerKey is true, but keyPropertyNames is NOT configured/, `sequencingPerKey true and no keyPropertyNames must throw error`);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for kinesis with keyPropertyNames, seqNoPropertyNames & idPropertyNames NOT configured and message sequencing required, but sequencingPerKey false`, t => {
  const context = createContext('kinesis', [], [], []);
  context.streamProcessing.sequencingPerKey = false;

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '457', 'ABC', 10, 1, 2, 3);
  const userRec = {a: 1, b: 2, c: {d: 4}}; // Dummy user record

  // Allowed to have NO keyPropertyNames, but ONLY if sequencingPerKey is false, which will force every message to be sequenced together
  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, userRec, context);

  const expectedIds = [];
  const expectedKeys = [];
  const expectedSeqNos = [['eventSeqNo', eventSeqNo]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for kinesis with keyPropertyNames, seqNoPropertyNames & idPropertyNames all undefined and message sequencing required`, t => {
  const context = createContext('kinesis', undefined, undefined, undefined);
  context.streamProcessing.sequencingPerKey = false;

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';
  const eventSubSeqNo = 12;

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '457', 'ABC', 10, 1, 2, 3);
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: eventSubSeqNo}; // Dummy user record

  // Now allowed to have NO keyPropertyNames i.e. NO keys, which will force every message to be sequenced together
  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, userRec, context);

  const expectedIds = [];
  const expectedKeys = [];
  const expectedSeqNos = [['eventSeqNo', eventSeqNo], ['eventSubSeqNo', eventSubSeqNo]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for kinesis with keyPropertyNames, seqNoPropertyNames & idPropertyNames NOT configured and message sequencing NOT required`, t => {
  const context = createContext('kinesis', [], [], []);
  context.streamProcessing.sequencingRequired = false;
  context.streamProcessing.sequencingPerKey = true;

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '458', 'ABC', 10, 1, 2, 3);
  const userRec = {a: 1, b: 2, c: {d: 4}}; // Dummy user record

  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, userRec, context);

  const expectedIds = [];
  const expectedKeys = [];
  const expectedSeqNos = [['eventSeqNo', eventSeqNo]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos, rec.eventID, eventSeqNo);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for kinesis with a key property undefined and message sequencing required`, t => {
  const context = createContext('kinesis', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '456', '789', 'ABC', undefined, 1, 2, 3);
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: '007'}; // Dummy user record

  t.throws(() => resolveMessageIdsAndSeqNos(msg, rec, userRec, context), /Missing property \[k2] for keys for message/, `any undefined key must throw error`);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for kinesis with BOTH key properties undefined and message sequencing required`, t => {
  const context = createContext('kinesis', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '456', '789', undefined, undefined, 1, 2, 3);
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: '007'}; // Dummy user record

  t.throws(() => resolveMessageIdsAndSeqNos(msg, rec, userRec, context), /Missing properties \[k1, k2] for keys for message/, `any undefined key must throw error`);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for kinesis with a key property undefined and message sequencing NOT required`, t => {
  const context = createContext('kinesis', ['id2', 'id1'], ['k2', 'k1'], ['n3','n2','n1']);
  context.streamProcessing.sequencingRequired = false;

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '444', '999', 'XYZ', undefined, -1, -2, -3);
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: '007'}; // Dummy user record

  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, userRec, context);

  const expectedIds = [['id2', '999'], ['id1', '444']];
  const expectedKeys = [['k2', undefined], ['k1', 'XYZ']];
  const expectedSeqNos = [['n3', -3], ['n2', -2], ['n1', -1]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos, rec.eventID, eventSeqNo);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for kinesis with an id property undefined and message sequencing required`, t => {
  const context = createContext('kinesis', ['id1', 'id2'], ['k2', 'k1'], ['n3', 'n2', 'n1']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', undefined, 'ABC', 10, 1, 2, 3);
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: '007'}; // Dummy user record

  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, userRec, context);

  const expectedIds = [['id1', '123'], ['id2', undefined]];
  const expectedKeys = [['k2', 10], ['k1', 'ABC']];
  const expectedSeqNos = [['n3', 3], ['n2', 2], ['n1', 1]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos, rec.eventID, eventSeqNo);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for kinesis with a sequence number property undefined and message sequencing required`, t => {
  const context = createContext('kinesis', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '555', 'ABC', '10', '1', '2', undefined);
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: '007'}; // Dummy user record

  t.throws(() => resolveMessageIdsAndSeqNos(msg, rec, userRec, context), /Missing property \[n3] for seqNos for message/, `any undefined sequence number must throw error`);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for kinesis with no idPropertyNames defined`, t => {
  const context = createContext('kinesis', [], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '777', 'PQR', 11, 10, 20, 30);
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: '007'}; // Dummy user record

  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, userRec, context);

  // Expect ids to contain a concatenation of keys and sequence numbers when no idPropertyNames are defined
  const expectedIds = []; //[['k1', 'PQR'], ['k2', 11], ['n1', 10], ['n2', 20], ['n3', 30]];
  const expectedKeys = [['k1', 'PQR'], ['k2', 11]];
  const expectedSeqNos = [['n1', 10], ['n2', 20], ['n3', 30]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos, rec.eventID, eventSeqNo);

  t.end();
});