'use strict';

/**
 * Unit tests for kinesis-stream-consumer/kinesis-processing.js & aws-stream-consumer-core/settings.js
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const Settings = require('aws-stream-consumer-core/settings');
const StreamType = Settings.StreamType;
const names = Settings.names;
const getStreamProcessingSetting = Settings.getStreamProcessingSetting;
const getStreamProcessingFunction = Settings.getStreamProcessingFunction;

// Convenience accessors for specific stream processing settings
const getStreamType = Settings.getStreamType;
const isKinesisStreamType = Settings.isKinesisStreamType;
const isDynamoDBStreamType = Settings.isDynamoDBStreamType;
const isSequencingRequired = Settings.isSequencingRequired;
const isSequencingPerKey = Settings.isSequencingPerKey;
const isBatchKeyedOnEventID = Settings.isBatchKeyedOnEventID;
const getConsumerIdSuffix = Settings.getConsumerIdSuffix;
const getConsumerId = Settings.getConsumerId;
const getMaxNumberOfAttempts = Settings.getMaxNumberOfAttempts;

const getIdPropertyNames = Settings.getIdPropertyNames;
const getKeyPropertyNames = Settings.getKeyPropertyNames;
const getSeqNoPropertyNames = Settings.getSeqNoPropertyNames;

// Convenience accessors for specific stream processing functions
const getGenerateMD5sFunction = Settings.getGenerateMD5sFunction;
const getResolveEventIdAndSeqNosFunction = Settings.getResolveEventIdAndSeqNosFunction;
const getResolveMessageIdsAndSeqNosFunction = Settings.getResolveMessageIdsAndSeqNosFunction;
const getExtractMessagesFromRecordFunction = Settings.getExtractMessagesFromRecordFunction;
const getExtractMessageFromRecordFunction = Settings.getExtractMessageFromRecordFunction;
const getLoadBatchStateFunction = Settings.getLoadBatchStateFunction;
const getPreProcessBatchFunction = Settings.getPreProcessBatchFunction;

const getPreFinaliseBatchFunction = Settings.getPreFinaliseBatchFunction;
const getSaveBatchStateFunction = Settings.getSaveBatchStateFunction;
const getDiscardUnusableRecordFunction = Settings.getDiscardUnusableRecordFunction;
const getDiscardRejectedMessageFunction = Settings.getDiscardRejectedMessageFunction;
const getPostFinaliseBatchFunction = Settings.getPostFinaliseBatchFunction;

const kinesisIdentify = require('../kinesis-identify');

const kinesisProcessing = require('../kinesis-processing');
const persisting = require('aws-stream-consumer-core/persisting');

// Default generateMD5s function
const generateKinesisMD5s = kinesisIdentify.generateKinesisMD5s;

// Default resolveEventIdAndSeqNos function
const resolveKinesisEventIdAndSeqNos = kinesisIdentify.resolveKinesisEventIdAndSeqNos;

// Default resolveMessageIdsAndSeqNos function
const resolveKinesisMessageIdsAndSeqNos = kinesisIdentify.resolveKinesisMessageIdsAndSeqNos;

// Default extractMessagesFromRecord functions
const extractMessagesFromKinesisRecord = kinesisProcessing.extractMessagesFromKinesisRecord;
const extractMessagesFromKplEncodedRecord = kinesisProcessing.extractMessagesFromKplEncodedRecord;

// Default extractMessageFromRecord functions
const extractJsonMessageFromKplUserRecord = kinesisProcessing.extractJsonMessageFromKplUserRecord;
const extractJsonMessageFromKinesisRecord = kinesisProcessing.extractJsonMessageFromKinesisRecord;

// Default loadBatchState function
const loadBatchStateFromDynamoDB = persisting.loadBatchStateFromDynamoDB;

// Default saveBatchState function
const saveBatchStateToDynamoDB = persisting.saveBatchStateToDynamoDB;

// External dependencies
// const logging = require('logging-utils');
// const base64 = require('core-functions/base64');
const regions = require('aws-core-utils/regions');
// const stages = require('aws-core-utils/stages');
const kinesisCache = require('aws-core-utils/kinesis-cache');
const dynamoDBDocClientCache = require('aws-core-utils/dynamodb-doc-client-cache');

// const samples = require('./samples');
const strings = require('core-functions/strings');
const stringify = strings.stringify;

const errors = require('core-functions/errors');
const FatalError = errors.FatalError;

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
  const region = regions.getRegion();
  kinesisCache.deleteKinesis(region);
  dynamoDBDocClientCache.deleteDynamoDBDocClient(region);
}

function checkConfig(t, context, expected, sequencingRequired, sequencingPerKey, kplEncoded) {
  const extractMessagesFromRecord = kplEncoded ? extractMessagesFromKplEncodedRecord : extractMessagesFromKinesisRecord;
  const extractMessageFromRecord = kplEncoded ? extractJsonMessageFromKplUserRecord : extractJsonMessageFromKinesisRecord;

  // Generic accessors
  t.equal(getStreamProcessingSetting(context, names.streamType), expected.streamType, `streamType setting must be ${expected.streamType}`);
  t.equal(getStreamProcessingSetting(context, names.sequencingRequired), expected.sequencingRequired, `sequencingRequired setting must be ${expected.sequencingRequired}`);
  t.equal(getStreamProcessingSetting(context, names.sequencingPerKey), expected.sequencingPerKey, `sequencingPerKey setting must be ${expected.sequencingPerKey}`);
  t.equal(getStreamProcessingSetting(context, names.batchKeyedOnEventID), expected.batchKeyedOnEventID, `batchKeyedOnEventID setting must be ${expected.batchKeyedOnEventID}`);
  t.equal(getStreamProcessingSetting(context, names.consumerIdSuffix), expected.consumerIdSuffix, `consumerIdSuffix setting must be ${expected.consumerIdSuffix}`);
  t.equal(getStreamProcessingSetting(context, names.consumerId), expected.consumerId, `consumerId setting must be ${expected.consumerId}`);
  t.equal(getStreamProcessingSetting(context, names.timeoutAtPercentageOfRemainingTime), expected.timeoutAtPercentageOfRemainingTime, `timeoutAtPercentageOfRemainingTime setting must be ${expected.timeoutAtPercentageOfRemainingTime}`);
  t.equal(getStreamProcessingSetting(context, names.maxNumberOfAttempts), expected.maxNumberOfAttempts, `maxNumberOfAttempts setting must be ${expected.maxNumberOfAttempts}`);

  t.deepEqual(getStreamProcessingSetting(context, names.idPropertyNames), expected.idPropertyNames, `idPropertyNames setting must be ${stringify(expected.idPropertyNames)}`);
  t.deepEqual(getStreamProcessingSetting(context, names.keyPropertyNames), expected.keyPropertyNames, `keyPropertyNames setting must be ${stringify(expected.keyPropertyNames)}`);
  t.deepEqual(getStreamProcessingSetting(context, names.seqNoPropertyNames), expected.seqNoPropertyNames, `seqNoPropertyNames setting must be ${stringify(expected.seqNoPropertyNames)}`);

  // Generic function accessors
  t.equal(getStreamProcessingFunction(context, names.extractMessagesFromRecord), extractMessagesFromRecord, `extractMessagesFromRecord function must be ${stringify(extractMessagesFromRecord)}`);
  t.equal(getStreamProcessingFunction(context, names.extractMessageFromRecord), extractMessageFromRecord, `extractMessageFromRecord function must be ${stringify(extractMessageFromRecord)}`);
  t.equal(getStreamProcessingFunction(context, names.generateMD5s), generateKinesisMD5s, `generateMD5s function must be ${stringify(generateKinesisMD5s)}`);
  t.equal(getStreamProcessingFunction(context, names.resolveEventIdAndSeqNos), resolveKinesisEventIdAndSeqNos, `resolveEventIdAndSeqNos function must be ${stringify(resolveKinesisEventIdAndSeqNos)}`);
  t.equal(getStreamProcessingFunction(context, names.resolveMessageIdsAndSeqNos), resolveKinesisMessageIdsAndSeqNos, `resolveMessageIdsAndSeqNos function must be ${stringify(resolveKinesisMessageIdsAndSeqNos)}`);
  t.equal(getStreamProcessingFunction(context, names.loadBatchState), loadBatchStateFromDynamoDB, `loadBatchState function must be ${stringify(loadBatchStateFromDynamoDB)}`);
  t.equal(getStreamProcessingFunction(context, names.preProcessBatch), undefined, `preProcessBatch function must be ${stringify(undefined)}`);

  t.equal(getStreamProcessingFunction(context, names.preFinaliseBatch), undefined, `preFinaliseBatch function must be ${stringify(undefined)}`);
  t.equal(getStreamProcessingFunction(context, names.saveBatchState), saveBatchStateToDynamoDB, `saveBatchState function must be ${stringify(saveBatchStateToDynamoDB)}`);
  t.equal(getStreamProcessingFunction(context, names.discardUnusableRecord), kinesisProcessing.discardUnusableRecordToDRQ, `discardUnusableRecord function must be ${stringify(kinesisProcessing.discardUnusableRecordToDRQ)}`);
  t.equal(getStreamProcessingFunction(context, names.discardRejectedMessage), kinesisProcessing.discardRejectedMessageToDMQ, `discardRejectedMessage function must be ${stringify(kinesisProcessing.discardRejectedMessageToDMQ)}`);
  t.equal(getStreamProcessingFunction(context, names.postFinaliseBatch), undefined, `postFinaliseBatch function must be ${stringify(undefined)}`);

  t.equal(getStreamProcessingSetting(context, names.batchStateTableName), expected.batchStateTableName, `batchStateTableName setting must be ${expected.batchStateTableName}`);
  t.equal(getStreamProcessingSetting(context, names.deadRecordQueueName), expected.deadRecordQueueName, `deadRecordQueueName setting must be ${expected.deadRecordQueueName}`);
  t.equal(getStreamProcessingSetting(context, names.deadMessageQueueName), expected.deadMessageQueueName, `deadMessageQueueName setting must be ${expected.deadMessageQueueName}`);

  // Specific setting accessors
  t.equal(getStreamType(context), StreamType.kinesis, `streamType setting must be ${StreamType.kinesis}`);
  t.equal(getStreamType(context), expected.streamType, `streamType setting must be ${expected.streamType}`);
  t.ok(isKinesisStreamType(context), `isKinesisStreamType must be ${true}`);
  t.notOk(isDynamoDBStreamType(context), `isDynamoDBStreamType must be ${false}`);

  t.equal(isSequencingRequired(context), sequencingRequired, `sequencingRequired setting must be ${sequencingRequired}`);
  t.equal(isSequencingPerKey(context), sequencingPerKey, `sequencingPerKey setting must be ${sequencingPerKey}`);

  t.equal(isSequencingRequired(context), expected.sequencingRequired, `sequencingRequired setting must be ${expected.sequencingRequired}`);
  t.equal(isSequencingPerKey(context), expected.sequencingPerKey, `sequencingPerKey setting must be ${expected.sequencingPerKey}`);
  t.equal(isBatchKeyedOnEventID(context), expected.batchKeyedOnEventID, `batchKeyedOnEventID setting must be ${expected.batchKeyedOnEventID}`);
  t.equal(getConsumerIdSuffix(context), expected.consumerIdSuffix, `consumerIdSuffix setting must be ${expected.consumerIdSuffix}`);
  t.equal(getConsumerId(context), expected.consumerId, `consumerId setting must be ${expected.consumerId}`);
  //t.equal(getTimeoutAtPercentageOfRemainingTime(context), defaultSettings.timeoutAtPercentageOfRemainingTime, `timeoutAtPercentageOfRemainingTime setting must be ${defaultSettings.timeoutAtPercentageOfRemainingTime}`);
  t.equal(getMaxNumberOfAttempts(context), expected.maxNumberOfAttempts, `maxNumberOfAttempts setting must be ${expected.maxNumberOfAttempts}`);

  t.deepEqual(getIdPropertyNames(context), expected.idPropertyNames, `idPropertyNames setting must be ${stringify(expected.idPropertyNames)}`);
  t.deepEqual(getKeyPropertyNames(context), expected.keyPropertyNames, `keyPropertyNames setting must be ${stringify(expected.keyPropertyNames)}`);
  t.deepEqual(getSeqNoPropertyNames(context), expected.seqNoPropertyNames, `seqNoPropertyNames setting must be ${stringify(expected.seqNoPropertyNames)}`);

  // Specific function accessors
  t.equal(getExtractMessagesFromRecordFunction(context), extractMessagesFromRecord, `extractMessagesFromRecord function must be ${stringify(extractMessagesFromRecord)}`);
  t.equal(getExtractMessageFromRecordFunction(context), extractMessageFromRecord, `extractMessageFromRecord function must be ${stringify(extractMessageFromRecord)}`);
  t.equal(getGenerateMD5sFunction(context), generateKinesisMD5s, `generateMD5s function must be ${stringify(generateKinesisMD5s)}`);
  t.equal(getResolveEventIdAndSeqNosFunction(context), resolveKinesisEventIdAndSeqNos, `resolveEventIdAndSeqNos function must be ${stringify(resolveKinesisEventIdAndSeqNos)}`);
  t.equal(getResolveMessageIdsAndSeqNosFunction(context), resolveKinesisMessageIdsAndSeqNos, `resolveMessageIdsAndSeqNos function must be ${stringify(resolveKinesisMessageIdsAndSeqNos)}`);
  t.equal(getLoadBatchStateFunction(context), loadBatchStateFromDynamoDB, `loadBatchState function must be ${stringify(loadBatchStateFromDynamoDB)}`);
  t.equal(getPreProcessBatchFunction(context), undefined, `preProcessBatch function must be ${stringify(undefined)}`);

  t.equal(getPreFinaliseBatchFunction(context), undefined, `preFinaliseBatch function must be ${stringify(undefined)}`);
  t.equal(getSaveBatchStateFunction(context), saveBatchStateToDynamoDB, `saveBatchState function must be ${stringify(saveBatchStateToDynamoDB)}`);
  t.equal(getDiscardUnusableRecordFunction(context), kinesisProcessing.discardUnusableRecordToDRQ, `discardUnusableRecord function must be ${stringify(kinesisProcessing.discardUnusableRecordToDRQ)}`);
  t.equal(getDiscardRejectedMessageFunction(context), kinesisProcessing.discardRejectedMessageToDMQ, `discardRejectedMessage function must be ${stringify(kinesisProcessing.discardRejectedMessageToDMQ)}`);
  t.equal(getPostFinaliseBatchFunction(context), undefined, `postFinaliseBatch function must be ${stringify(undefined)}`);
}

// =====================================================================================================================
// Accessors of settings and functions on default Kinesis configuration
// =====================================================================================================================

test('Accessors of settings and functions on default Kinesis configuration', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    // Configure default stream processing settings
    const context = kinesisProcessing.configureDefaultKinesisStreamProcessing({});

    const options = kinesisProcessing.loadKinesisDefaultOptions();
    const expected = kinesisProcessing.getDefaultKinesisStreamProcessingSettings(options.streamProcessingOptions);

    checkConfig(t, context, expected, true, false, false);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('Accessors of settings and functions on default Kinesis configuration with minimalistic sequencing per key configuration', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    // Configure stream processing settings
    // Using minimalist options for sequenced per key
    const settings = {sequencingPerKey: true, keyPropertyNames: ['k']};
    const context = kinesisProcessing.configureDefaultKinesisStreamProcessing({}, settings);

    const options = kinesisProcessing.loadKinesisDefaultOptions();
    options.streamProcessingOptions.sequencingPerKey = true;
    const expected = kinesisProcessing.getDefaultKinesisStreamProcessingSettings(options.streamProcessingOptions);
    expected.keyPropertyNames = ['k'];

    checkConfig(t, context, expected, true, true, false);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('Accessors of settings and functions on default Kinesis configuration with conflicting sequencing per key configuration - case 1', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    // Configure stream processing settings
    const settings = {sequencingRequired: false, sequencingPerKey: true};
    t.throws(() => kinesisProcessing.configureDefaultKinesisStreamProcessing({}, settings), FatalError, 'conflict case 1 must throw FatalError');

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('Accessors of settings and functions on default Kinesis configuration with conflicting sequencing per key configuration - case 2', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    // Configure stream processing settings
    const settings = {sequencingPerKey: false, keyPropertyNames: ['k']};
    t.throws(() => kinesisProcessing.configureDefaultKinesisStreamProcessing({}, settings), FatalError, 'conflict case 2 must throw FatalError');

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('Accessors of settings and functions on default Kinesis configuration with conflicting sequencing per key configuration - case 3', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    // Configure stream processing settings
    const settings = {sequencingPerKey: true, keyPropertyNames: []};
    t.throws(() => kinesisProcessing.configureDefaultKinesisStreamProcessing({}, settings), FatalError, 'conflict case 3 must throw FatalError');

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('Accessors of settings and functions on default Kinesis configuration with conflicting sequencing per key configuration - case 4', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    // Configure stream processing settings
    // Using minimalist options for sequenced per key
    const settings = {sequencingRequired: false, sequencingPerKey: false, keyPropertyNames: ['k']};
    t.throws(() => kinesisProcessing.configureDefaultKinesisStreamProcessing({}, settings), FatalError, 'conflict case 4 must throw FatalError');

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// Accessors of settings and functions on UNSEQUENCED Kinesis configuration
// =====================================================================================================================

test('Accessors of settings and functions on UNSEQUENCED Kinesis configuration', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    // Configure UNSEQUENCED stream processing settings
    // Using minimalist options for unsequenced
    const settings = {sequencingRequired: false};
    const context = kinesisProcessing.configureDefaultKinesisStreamProcessing({}, settings);

    const options = kinesisProcessing.loadKinesisDefaultOptions();
    options.streamProcessingOptions.sequencingRequired = false;
    const expected = kinesisProcessing.getDefaultKinesisStreamProcessingSettings(options.streamProcessingOptions);

    checkConfig(t, context, expected, false, false, false);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});