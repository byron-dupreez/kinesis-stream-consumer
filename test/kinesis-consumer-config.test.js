'use strict';

/**
 * Unit tests for testing the configuration aspects of kinesis-stream-consumer/kinesis-consumer.js
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const kinesisConsumer = require('../kinesis-consumer');
const kinesisProcessing = require('../kinesis-processing');

const logging = require('logging-utils');

const persisting = require('aws-stream-consumer-core/persisting');

const Settings = require('aws-stream-consumer-core/settings');
const StreamType = Settings.StreamType;

// External dependencies
const regions = require('aws-core-utils/regions');
const stages = require('aws-core-utils/stages');
const kinesisCache = require('aws-core-utils/kinesis-cache');
const dynamoDBDocClientCache = require('aws-core-utils/dynamodb-doc-client-cache');

// const Strings = require('core-functions/strings');
// const stringify = Strings.stringify;

// Testing dependencies
const samples = require('./samples');

function sampleAwsEvent(streamName, partitionKey, data, omitEventSourceARN) {
  const region = process.env.AWS_REGION;
  const eventSourceArn = omitEventSourceARN ? undefined : samples.sampleKinesisEventSourceArn(region, streamName);
  return samples.sampleKinesisEventWithSampleRecord(undefined, undefined, partitionKey, data, eventSourceArn, region);
}

function sampleAwsContext(functionVersion, functionAlias) {
  const region = process.env.AWS_REGION;
  const functionName = 'sampleFunctionName';
  const invokedFunctionArn = samples.sampleInvokedFunctionArn(region, functionName, functionAlias);
  return samples.sampleAwsContext(functionName, functionVersion, invokedFunctionArn);
}

function checkDependencies(t, context, stdSettings, stdOptions, event, awsContext, expectedStage) {
  t.ok(logging.isLoggingConfigured(context), `logging must be configured`);
  t.ok(stages.isStageHandlingConfigured(context), `stage handling must be configured`);
  t.ok(context.stageHandling && typeof context.stageHandling === 'object', 'context.stageHandling must be configured');
  t.ok(context.custom && typeof context.custom === 'object', `context.custom must be configured`);
  t.ok(kinesisProcessing.isStreamProcessingConfigured(context), 'stream processing must be configured');
  t.ok(context.streamProcessing && typeof context.streamProcessing === 'object', 'context.streamProcessing must be configured');

  const kinesisOptions = stdSettings && stdSettings.kinesisOptions ? stdSettings.kinesisOptions :
    stdOptions && stdOptions.kinesisOptions ? stdOptions.kinesisOptions : undefined;

  const dynamoDBDocClientOptions = stdSettings && stdSettings.dynamoDBDocClientOptions ? stdSettings.dynamoDBDocClientOptions :
    stdOptions && stdOptions.dynamoDBDocClientOptions ? stdOptions.dynamoDBDocClientOptions : undefined;

  // Check Kinesis instance is also configured
  const region = regions.getRegion();
  if (kinesisOptions) {
    t.ok(context.kinesis && typeof context.kinesis === 'object', 'context.kinesis must be configured');
    t.equal(context.kinesis.config.region, region, `context.kinesis.config.region (${context.kinesis.config.region}) must be ${region}`);
    t.equal(context.kinesis.config.maxRetries, kinesisOptions.maxRetries, `context.kinesis.config.maxRetries (${context.kinesis.config.maxRetries}) must be ${kinesisOptions.maxRetries}`);
  } else {
    t.notOk(context.kinesis, 'context.kinesis must not be configured');
  }

  // Check DynamoDB DocumentClient instance is also configured
  if (dynamoDBDocClientOptions) {
    // Check DynamoDB.DocumentClient is also configured
    t.ok(context.dynamoDBDocClient && typeof context.dynamoDBDocClient === 'object', 'context.dynamoDBDocClient must be configured');
    t.equal(context.dynamoDBDocClient.service.config.region, region, `context.dynamoDBDocClient.service.config.region (${context.dynamoDBDocClient.service.config.region}) must be ${region}`);
    t.equal(context.dynamoDBDocClient.service.config.maxRetries, dynamoDBDocClientOptions.maxRetries,
      `context.dynamoDBDocClient.service.config.maxRetries (${context.dynamoDBDocClient.service.config.maxRetries}) must be ${dynamoDBDocClientOptions.maxRetries}`);
  } else {
    t.notOk(context.dynamoDBDocClient, 'context.dynamoDBDocClient must not be configured');
  }

  if (event && awsContext) {
    t.equal(context.region, region, `context.region must be ${region}`);
    t.equal(context.stage, expectedStage, `context.stage must be ${expectedStage}`);
    t.equal(context.awsContext, awsContext, 'context.awsContext must be given AWS context');
  }
}

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

// =====================================================================================================================
// kinesisConsumer.isStreamConsumerConfigured with "default" Kinesis unsequenced options
// =====================================================================================================================

test('kinesisConsumer.isStreamConsumerConfigured with "default" Kinesis unsequenced options', t => {
  try {
    // Simulate a region in AWS_REGION for testing
    setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};
    const options = kinesisProcessing.loadKinesisDefaultOptions();
    options.streamProcessingOptions.sequencingRequired = false;

    // Must not be configured yet
    t.notOk(kinesisConsumer.isStreamConsumerConfigured(context), `stream consumer must not be configured`);

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    // Now configure the stream consumer runtime settings
    kinesisConsumer.configureStreamConsumer(context, undefined, options, event, awsContext);

    // Must be configured now
    t.ok(kinesisConsumer.isStreamConsumerConfigured(context), `stream consumer must be configured`);
    checkDependencies(t, context, undefined, options, event, awsContext, 'dev1');

    t.equal(context.streamProcessing.streamType, StreamType.kinesis, `streamType must be kinesis`);
    t.equal(context.streamProcessing.sequencingRequired, false, `sequencingRequired must be false`);
    t.equal(context.streamProcessing.batchStateTableName, Settings.defaults.batchStateTableName, `batchStateTableName must be '${Settings.defaults.batchStateTableName}'`);
    t.ok(context.streamProcessing.saveBatchState, `saveBatchState must be defined`);
    t.ok(context.streamProcessing.loadBatchState, `loadBatchState must be defined`);

    t.ok(context.kinesis, `kinesis must be defined`);
    t.ok(context.dynamoDBDocClient, `dynamoDBDocClient must be defined`);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// kinesisConsumer.isStreamConsumerConfigured with default Kinesis sequenced per shard options
// =====================================================================================================================

test('kinesisConsumer.isStreamConsumerConfigured with default Kinesis sequenced per shard options', t => {
  try {
    // Simulate a region in AWS_REGION for testing
    setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};
    const options = kinesisProcessing.loadKinesisDefaultOptions();
    // options.streamProcessingOptions.sequencingRequired = true;

    // keyPropertyNames are mandatory for sequenced consumption
    // options.streamProcessingOptions.keyPropertyNames = ['k1'];

    // Must not be configured yet
    t.notOk(kinesisConsumer.isStreamConsumerConfigured(context), `stream consumer must not be configured`);

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleAwsEvent(streamName, 'partitionKey', {k1: 1, n1: 2}, false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    // Now configure the stream consumer runtime settings
    kinesisConsumer.configureStreamConsumer(context, undefined, options, event, awsContext);

    // Must be configured now
    t.ok(kinesisConsumer.isStreamConsumerConfigured(context), `stream consumer must be configured`);
    checkDependencies(t, context, undefined, options, event, awsContext, 'dev1');

    t.equal(context.streamProcessing.streamType, StreamType.kinesis, `streamType must be kinesis`);
    t.equal(context.streamProcessing.sequencingRequired, true, `sequencingRequired must be true`);
    t.equal(context.streamProcessing.batchStateTableName, Settings.defaults.batchStateTableName, `batchStateTableName must be '${Settings.defaults.batchStateTableName}'`);
    t.equal(context.streamProcessing.saveBatchState, persisting.saveBatchStateToDynamoDB, `saveBatchState must be saveBatchStateToDynamoDB`);
    t.equal(context.streamProcessing.loadBatchState, persisting.loadBatchStateFromDynamoDB, `loadBatchState must be loadBatchStateFromDynamoDB`);

    t.ok(context.kinesis, `kinesis must be defined`);
    t.ok(context.dynamoDBDocClient, `dynamoDBDocClient must be defined`);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// kinesisConsumer.isStreamConsumerConfigured with "default" Kinesis sequenced per key options
// =====================================================================================================================

test('kinesisConsumer.isStreamConsumerConfigured with "default" Kinesis sequenced per key options', t => {
  try {
    // Simulate a region in AWS_REGION for testing
    setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};
    const options = kinesisProcessing.loadKinesisDefaultOptions();
    // options.streamProcessingOptions.sequencingRequired = true;
    options.streamProcessingOptions.sequencingPerKey = true;

    // keyPropertyNames are mandatory for sequenced consumption
    options.streamProcessingOptions.keyPropertyNames = ['k1'];

    // Must not be configured yet
    t.notOk(kinesisConsumer.isStreamConsumerConfigured(context), `stream consumer must not be configured`);

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleAwsEvent(streamName, 'partitionKey', {k1: 1, n1: 2}, false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    // Now configure the stream consumer runtime settings
    kinesisConsumer.configureStreamConsumer(context, undefined, options, event, awsContext);

    // Must be configured now
    t.ok(kinesisConsumer.isStreamConsumerConfigured(context), `stream consumer must be configured`);
    checkDependencies(t, context, undefined, options, event, awsContext, 'dev1');

    t.equal(context.streamProcessing.streamType, StreamType.kinesis, `streamType must be kinesis`);
    t.equal(context.streamProcessing.sequencingRequired, true, `sequencingRequired must be true`);
    t.equal(context.streamProcessing.batchStateTableName, Settings.defaults.batchStateTableName, `batchStateTableName must be '${Settings.defaults.batchStateTableName}'`);
    t.equal(context.streamProcessing.saveBatchState, persisting.saveBatchStateToDynamoDB, `saveBatchState must be saveBatchStateToDynamoDB`);
    t.equal(context.streamProcessing.loadBatchState, persisting.loadBatchStateFromDynamoDB, `loadBatchState must be loadBatchStateFromDynamoDB`);

    t.ok(context.kinesis, `kinesis must be defined`);
    t.ok(context.dynamoDBDocClient, `dynamoDBDocClient must be defined`);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// kinesisConsumer.configureStreamConsumer with default Kinesis unsequenced options
// =====================================================================================================================

test('kinesisConsumer.configureStreamConsumer with default Kinesis unsequenced options must fail if missing stage', t => {
  const context = {};
  const options = kinesisProcessing.loadKinesisDefaultOptions();
  options.streamProcessingOptions.sequencingRequired = false;

  // Generate a sample AWS event
  const streamName = 'TestStream';
  const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', '1.0.1');

  try {
    kinesisConsumer.configureStreamConsumer(context, undefined, options, event, awsContext);
    t.fail(`kinesisConsumer.configureStreamConsumer should NOT have passed`);

  } catch (err) {
    t.pass(`kinesisConsumer.configureStreamConsumer must fail (${err})`);
    const errMsgMatch = 'Failed to resolve stage';
    t.ok(err.message.indexOf(errMsgMatch) !== -1, `kinesisConsumer.configureStreamConsumer error should contain (${errMsgMatch})`);
  }

  t.end();
});

test('kinesisConsumer.configureStreamConsumer with default Kinesis sequenced options must fail if missing stage', t => {
  try {
    // Simulate a region in AWS_REGION for testing
    setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};
    const options = kinesisProcessing.loadKinesisDefaultOptions();
    // options.streamProcessingOptions.sequencingRequired = true;

    // Generate a sample AWS event
    const streamName = 'TestStream';
    const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', '1.0.1');

    try {
      kinesisConsumer.configureStreamConsumer(context, undefined, options, event, awsContext);
      t.fail(`kinesisConsumer.configureStreamConsumer should NOT have passed`);

    } catch (err) {
      t.pass(`kinesisConsumer.configureStreamConsumer must fail (${err})`);
      const errMsgMatch = 'Failed to resolve stage';
      t.ok(err.message.indexOf(errMsgMatch) !== -1, `kinesisConsumer.configureStreamConsumer error should contain (${errMsgMatch})`);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('kinesisConsumer.configureStreamConsumer with default Kinesis unsequenced options & ideal conditions must pass', t => {
  try {
    // Simulate a region in AWS_REGION for testing
    setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};
    const options = kinesisProcessing.loadKinesisDefaultOptions();
    options.streamProcessingOptions.sequencingRequired = false;

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    try {
      // Simulate perfect conditions - everything meant to be configured beforehand has been configured as well
      kinesisConsumer.configureStreamConsumer(context, undefined, options, event, awsContext);
      t.pass(`kinesisConsumer.configureStreamConsumer should have passed`);

      t.ok(kinesisProcessing.isStreamProcessingConfigured(context), 'stream processing must be configured');
      checkDependencies(t, context, undefined, options, event, awsContext, 'dev1');

    } catch (err) {
      t.fail(`kinesisConsumer.configureStreamConsumer should NOT have failed (${err})`);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('kinesisConsumer.configureStreamConsumer with default Kinesis sequenced per shard options WITH keyPropertyNames must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing
    setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};
    const options = kinesisProcessing.loadKinesisDefaultOptions();
    // options.streamProcessingOptions.sequencingRequired = true;

    // keyPropertyNames must NOT be specified for sequenced per shard consumption
    options.streamProcessingOptions.keyPropertyNames = ['k1'];

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleAwsEvent(streamName, 'partitionKey', {k1: 1, n1: 2}, false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    try {
      // Simulate near perfect conditions - everything meant to be configured beforehand has been configured as well
      kinesisConsumer.configureStreamConsumer(context, undefined, options, event, awsContext);

      t.fail(`kinesisConsumer.configureStreamConsumer should NOT have passed`);

    } catch (err) {
      t.pass(`kinesisConsumer.configureStreamConsumer should have failed (${err})`);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('kinesisConsumer.configureStreamConsumer with "default" Kinesis sequenced per key options WITHOUT keyPropertyNames must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing
    setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};
    const options = kinesisProcessing.loadKinesisDefaultOptions();
    // options.streamProcessingOptions.sequencingRequired = true;
    options.streamProcessingOptions.sequencingPerKey = true;

    // keyPropertyNames are mandatory for sequenced per key consumption
    // options.streamProcessingOptions.keyPropertyNames = ['k1'];

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleAwsEvent(streamName, 'partitionKey', {k1: 1, n1: 2}, false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    try {
      // Simulate near perfect conditions - everything meant to be configured beforehand has been configured as well
      kinesisConsumer.configureStreamConsumer(context, undefined, options, event, awsContext);

      t.fail(`kinesisConsumer.configureStreamConsumer should NOT have passed`);

    } catch (err) {
      t.pass(`kinesisConsumer.configureStreamConsumer should have failed (${err})`);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('kinesisConsumer.configureStreamConsumer with default Kinesis sequenced options & ideal conditions must pass', t => {
  try {
    // Simulate a region in AWS_REGION for testing
    setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};
    const options = kinesisProcessing.loadKinesisDefaultOptions();
    // options.streamProcessingOptions.sequencingRequired = true;

    // keyPropertyNames must NOT be specified for sequenced per shard consumption
    // options.streamProcessingOptions.keyPropertyNames = ['k1'];

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleAwsEvent(streamName, 'partitionKey', {k1: 1, n1: 2}, false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    try {
      // Simulate perfect conditions - everything meant to be configured beforehand has been configured as well
      kinesisConsumer.configureStreamConsumer(context, undefined, options, event, awsContext);

      t.pass(`kinesisConsumer.configureStreamConsumer should have passed`);

      t.ok(kinesisProcessing.isStreamProcessingConfigured(context), 'Kinesis stream processing must be configured');
      t.ok(kinesisConsumer.isStreamConsumerConfigured(context), 'Kinesis stream consumer must be configured');
      checkDependencies(t, context, undefined, options, event, awsContext, 'dev1');

    } catch (err) {
      t.fail(`kinesisConsumer.configureStreamConsumer should NOT have failed (${err})`);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});
