'use strict';

const streamEvents = require('aws-core-utils/stream-events');
const MAX_PARTITION_KEY_SIZE = streamEvents.MAX_PARTITION_KEY_SIZE;

const Booleans = require('core-functions/booleans');
const isTrueOrFalse = Booleans.isTrueOrFalse;

const tries = require('core-functions/tries');
const Try = tries.Try;

const copying = require('core-functions/copying');
const copy = copying.copy;
const merging = require('core-functions/merging');
const merge = merging.merge;
const deep = {deep: true};

const errors = require('core-functions/errors');
const FatalError = errors.FatalError;

const Strings = require('core-functions/strings');
const isNotBlank = Strings.isNotBlank;
const stringify = Strings.stringify;

// Setting-related utilities
const Settings = require('aws-stream-consumer-core/settings');
const defaults = Settings.defaults;
const StreamType = Settings.StreamType;

const streamProcessing = require('aws-stream-consumer-core/stream-processing');

// Utilities for loading and saving tracked state of the current batch being processed from and to an external store
const persisting = require('aws-stream-consumer-core/persisting');

const kinesisIdentify = require('./kinesis-identify');

// Kinesis-specific
// let agg = undefined;
let deaggregateAsync = undefined;

const LAST_RESORT_KEY = 'LAST_RESORT_KEY';

/**
 * Utilities for configuring stream processing, which configures and determines the processing behaviour of a stream
 * consumer.
 * @module aws-stream-consumer-core/stream-processing-config
 * @author Byron du Preez
 */
exports._ = '_'; //IDE workaround

exports.LAST_RESORT_KEY = LAST_RESORT_KEY;

// Stream processing configuration - configures and determines the processing behaviour of a stream consumer
exports.isStreamProcessingConfigured = isStreamProcessingConfigured;

exports.configureStreamProcessing = configureStreamProcessing;
exports.configureStreamProcessingWithSettings = configureStreamProcessingWithSettings;

exports.validateKinesisStreamProcessingConfiguration = validateKinesisStreamProcessingConfiguration;

exports.configureDefaultKinesisStreamProcessing = configureDefaultKinesisStreamProcessing;
exports.getDefaultKinesisStreamProcessingSettings = getDefaultKinesisStreamProcessingSettings;
exports.getDefaultKinesisStreamProcessingOptions = getDefaultKinesisStreamProcessingOptions;

// Local json file defaults for configuration purposes
exports.loadKinesisDefaultOptions = loadKinesisDefaultOptions;
// Static defaults for configuration purposes
exports.getKinesisStaticDefaults = getKinesisStaticDefaults;

/**
 * Default implementations of the stream processing functions, which are NOT meant to be used directly and are ONLY
 * exposed to facilitate re-using some of these functions if needed in a customised stream processing configuration.
 */
// Default Kinesis stream processing functions
// ===========================================

// Default extractMessagesFromRecord function for extracting from normal (i.e. non-KPL encoded) Kinesis stream event records
exports.extractMessagesFromKinesisRecord = extractMessagesFromKinesisRecord;

// Default extractMessageFromRecord function from normal (i.e. non-KPL encoded) Kinesis stream event records
exports.extractJsonMessageFromKinesisRecord = extractJsonMessageFromKinesisRecord;

// Default extractMessagesFromRecord function for extracting from KPL encoded records
exports.extractMessagesFromKplEncodedRecord = extractMessagesFromKplEncodedRecord;

// Default extractMessageFromRecord function for extracting from KPL user records
exports.extractJsonMessageFromKplUserRecord = extractJsonMessageFromKplUserRecord;

// Default generateMD5s function (re-exported for convenience)
exports.generateKinesisMD5s = kinesisIdentify.generateKinesisMD5s;

// Default resolveEventIdAndSeqNos function (re-exported for convenience)
exports.resolveKinesisEventIdAndSeqNos = kinesisIdentify.resolveKinesisEventIdAndSeqNos;

// Default resolveMessageIdsAndSeqNos function (re-exported for convenience)
exports.resolveKinesisMessageIdsAndSeqNos = kinesisIdentify.resolveKinesisMessageIdsAndSeqNos;

// Default common Kinesis and DynamoDB stream processing functions
// ===============================================================

// Default loadBatchState function (re-exported for convenience)
exports.loadBatchStateFromDynamoDB = persisting.loadBatchStateFromDynamoDB;

// Default saveBatchState function (re-exported for convenience)
exports.saveBatchStateToDynamoDB = persisting.saveBatchStateToDynamoDB;

// Default discardUnusableRecord function
exports.discardUnusableRecordToDRQ = discardUnusableRecordToDRQ;

// Default discardRejectedMessage function
exports.discardRejectedMessageToDMQ = discardRejectedMessageToDMQ;

// Other default stream processing functions
exports.useStreamEventRecordAsMessage = streamProcessing.useStreamEventRecordAsMessage;


// =====================================================================================================================
// Stream processing configuration - configures and determines the processing behaviour of a stream consumer
// =====================================================================================================================

/**
 * Returns true if stream processing is already configured on the given context; false otherwise.
 * @param {Object|StreamProcessing} context - the context to check
 * @returns {boolean} true if configured; false otherwise
 */
function isStreamProcessingConfigured(context) {
  return streamProcessing.isStreamProcessingConfigured(context);
  // return context && typeof context === 'object' && context.streamProcessing && typeof context.streamProcessing === 'object';
}

/**
 * Configures the given context as a standard context with the given standard settings and standard options and with
 * EITHER the given stream processing settings (if any) OR the default stream processing settings partially overridden
 * by the given stream processing options (if any), but only if stream processing is not already configured on the given
 * context OR if forceConfiguration is true.
 *
 * Note that if either the given event or AWS context are undefined, then everything other than the event, AWS context &
 * stage will be configured. This missing configuration can be configured at a later point in your code by invoking
 * {@linkcode module:aws-core-utils/contexts#configureEventAwsContextAndStage}. This separation of configuration is
 * primarily useful for unit testing.
 *
 * @param {Object|StreamProcessing|StandardContext} context - the context to configure
 * @param {StreamProcessingSettings|undefined} [settings] - optional stream processing settings to use to configure stream processing
 * @param {StreamProcessingOptions|undefined} [options] - optional stream processing options to use to override default options
 * @param {StandardSettings|undefined} [standardSettings] - optional standard settings to use to configure dependencies
 * @param {StandardOptions|undefined} [standardOptions] - optional other options to use to configure dependencies
 * @param {AWSEvent|undefined} [event] - the AWS event, which was passed to your lambda
 * @param {AWSContext|undefined} [awsContext] - the AWS context, which was passed to your lambda
 * @param {boolean|undefined} [forceConfiguration] - whether or not to force configuration of the given settings, which
 * will override any previously configured stream processing settings on the given context
 * @returns {StreamProcessing} the given context configured with stream processing settings, stage handling settings and
 * logging functionality
 */
function configureStreamProcessing(context, settings, options, standardSettings, standardOptions, event, awsContext, forceConfiguration) {
  const settingsAvailable = settings && typeof settings === 'object';
  const optionsAvailable = options && typeof options === 'object';

  // Check if stream processing was already configured
  const streamProcessingWasConfigured = streamProcessing.isStreamProcessingConfigured(context);

  // Determine the stream processing settings to be used
  const defaultSettings = getDefaultKinesisStreamProcessingSettings(options);

  const streamProcessingSettings = settingsAvailable ? merge(defaultSettings, settings) : defaultSettings;

  // Configure stream processing with the given or derived stream processing settings
  configureStreamProcessingWithSettings(context, streamProcessingSettings, standardSettings, standardOptions,
    event, awsContext, forceConfiguration);

  // Log a warning if no settings and no options were provided and the default settings were applied
  if (!settingsAvailable && !optionsAvailable && (forceConfiguration || !streamProcessingWasConfigured)) {
    context.warn(`Stream processing was configured without settings or options - used default stream processing configuration (${stringify(streamProcessingSettings)})`);
  }
  return context;
}

/**
 * Configures the given context with the given stream processing settings, but only if stream processing is not already
 * configured on the given context OR if forceConfiguration is true, and with the given standard settings and options.
 *
 * Note that if either the given event or AWS context are undefined, then everything other than the event, AWS context &
 * stage will be configured. This missing configuration can be configured at a later point in your code by invoking
 * {@linkcode module:aws-core-utils/contexts#configureEventAwsContextAndStage}. This separation of configuration is
 * primarily useful for unit testing.
 *
 * @param {Object|StreamProcessing|StandardContext} context - the context onto which to configure the given stream processing settings and standard settings
 * @param {StreamProcessingSettings} settings - the stream processing settings to use
 * @param {StandardSettings|undefined} [standardSettings] - optional standard settings to use to configure dependencies
 * @param {StandardOptions|undefined} [standardOptions] - optional standard options to use to configure dependencies
 * @param {AWSEvent|undefined} [event] - the AWS event, which was passed to your lambda
 * @param {AWSContext|undefined} [awsContext] - the AWS context, which was passed to your lambda
 * @param {boolean|undefined} [forceConfiguration] - whether or not to force configuration of the given settings and
 * options, which will override any previously configured stream processing and stage handling settings on the given context
 * @return {StreamProcessing} the context object configured with stream processing (either existing or new) and standard settings
 */
function configureStreamProcessingWithSettings(context, settings, standardSettings, standardOptions, event, awsContext, forceConfiguration) {
  return streamProcessing.configureStreamProcessingWithSettings(context, settings, standardSettings, standardOptions,
    event, awsContext, forceConfiguration, validateKinesisStreamProcessingConfiguration);
}

// noinspection JSUnusedLocalSymbols
/**
 * Resolves whether sequencing is required or not from the given stream processing settings and/or options (if any).
 * @param {StreamProcessingSettings|undefined} [settings] - optional stream processing settings to use
 * @param {StreamProcessingOptions|undefined} [options] - optional stream processing options to use
 * @returns {boolean} whether sequencing is required or not (defaults to true if not specified)
 */
function resolveSequencingRequired(settings, options) {
  return settings && typeof settings === 'object' && isTrueOrFalse(settings.sequencingRequired) ?
    !!settings.sequencingRequired : options && typeof options === 'object' && isTrueOrFalse(options.sequencingRequired) ?
      !!options.sequencingRequired : true; // default to true if not explicitly true or false
}

/**
 * Configures the given context as a standard context with the given standard settings and standard options and ALSO
 * with the default Kinesis stream processing settings partially overridden by the given stream processing options (if
 * any), but ONLY if stream processing is not already configured on the given context OR if forceConfiguration is true.
 *
 * Default Kinesis stream processing assumes the following:
 * - The stream event is a Kinesis stream event, which contains one or more records in its `Records` property.
 * - Each of these event records will be converted into one or more messages using the configured `extractMessagesFromRecord`
 *   function.
 * - The default Kinesis `extractMessagesFromRecord` function assumes that:
 *   - Each Kinesis stream event record contains one or more user records, which will be extracted from the record using
 *     the `aws-kinesis-agg` module.
 *   - See {@link extractMessagesFromKinesisRecord} for the default Kinesis {@link ExtractMessagesFromRecord} implementation.
 *   - Note: This default behaviour can be changed by configuring a different `extractMessagesFromRecord` function.
 *   - Each of these user records will then be converted into a corresponding message using the configured
 *     `extractMessageFromRecord` function.
 *   - The default Kinesis `extractMessageFromRecord` function assumes that:
 *     - Each message is a JSON object parsed from the base 64 data within its corresponding user record's data property
 *       (or its event record's kinesis.data property, if no user record is provided).
 *     - See {@link extractJsonMessageFromKplUserRecord} for the default Kinesis {@link ExtractMessageFromKinesisUserRecord}
 *       implementation.
 *     - Note: This behaviour can be changed by configuring a different `extractMessageFromRecord` function.
 *
 * Note that if either the given event or AWS context are undefined, then everything other than the event, AWS context &
 * stage will be configured. This missing configuration can be configured at a later point in your code by invoking
 * {@linkcode module:aws-core-utils/contexts#configureEventAwsContextAndStage}. This separation of configuration is
 * primarily useful for unit testing.
 *
 * @param {Object|StreamProcessing|StandardContext} context - the context onto which to configure the default stream processing settings
 * @param {StreamProcessingOptions|undefined} [options] - optional stream processing options to use
 * @param {StandardSettings|undefined} [standardSettings] - optional standard settings to use to configure dependencies
 * @param {StandardOptions|undefined} [standardOptions] - optional standard options to use to configure dependencies
 * @param {AWSEvent|undefined} [event] - the AWS event, which was passed to your lambda
 * @param {AWSContext|undefined} [awsContext] - the AWS context, which was passed to your lambda
 * @param {boolean|undefined} [forceConfiguration] - whether or not to force configuration of the given settings, which
 * will override any previously configured stream processing and stage handling settings on the given context
 * @return {StreamProcessing} the context object configured with Kinesis stream processing settings (either existing or defaults)
 */
function configureDefaultKinesisStreamProcessing(context, options, standardSettings, standardOptions, event, awsContext, forceConfiguration) {
  // Get the default Kinesis stream processing settings from the local options file
  const settings = getDefaultKinesisStreamProcessingSettings(options);

  // Configure the context with the default stream processing settings defined above
  return configureStreamProcessingWithSettings(context, settings, standardSettings, standardOptions,
    event, awsContext, forceConfiguration);
}

/**
 * Returns the appropriate default Kinesis stream processing settings partially overridden by the given stream
 * processing options (if any) based on whether sequencing is required or not, which is resolved as follows:
 * 1. If the given sequencingRequired is explicitly true or false, then it will be used;
 * 2. If options.sequencingRequired is explicitly true or false, then it will be used;
 * 3. Otherwise it defaults to true
 *
 * This function is used internally by {@linkcode configureDefaultKinesisStreamProcessing}, but could also be used in
 * custom configurations to get the default settings as a base to be overridden with your custom settings before calling
 * {@linkcode configureStreamProcessing}.
 *
 * @param {StreamProcessingOptions|undefined} [options] - optional stream processing options to use to override the default options
 * @returns {StreamProcessingSettings} a stream processing settings object (including both property and function settings)
 */
function getDefaultKinesisStreamProcessingSettings(options) {
  const settings = options && typeof options === 'object' ? copy(options, deep) : {};

  // Load defaults from local default-kinesis-options.json file
  const defaultOptions = getDefaultKinesisStreamProcessingOptions();
  merge(defaultOptions, settings);

  const kplEncoded = settings.kplEncoded;

  const defaultSettings = {
    // Configurable processing functions
    extractMessagesFromRecord: kplEncoded ?
      extractMessagesFromKplEncodedRecord : extractMessagesFromKinesisRecord,
    extractMessageFromRecord: kplEncoded ?
      extractJsonMessageFromKplUserRecord : extractJsonMessageFromKinesisRecord,
    generateMD5s: kinesisIdentify.generateKinesisMD5s,
    resolveEventIdAndSeqNos: kinesisIdentify.resolveKinesisEventIdAndSeqNos,
    resolveMessageIdsAndSeqNos: kinesisIdentify.resolveKinesisMessageIdsAndSeqNos,
    loadBatchState: persisting.loadBatchStateFromDynamoDB,
    preProcessBatch: undefined,

    preFinaliseBatch: undefined,
    saveBatchState: persisting.saveBatchStateToDynamoDB,
    discardUnusableRecord: discardUnusableRecordToDRQ,
    discardRejectedMessage: discardRejectedMessageToDMQ,
    postFinaliseBatch: undefined
  };
  return merge(defaultSettings, settings);
}

// =====================================================================================================================
// Default options for configuration purposes
// =====================================================================================================================

/**
 * Loads a copy of the default Kinesis stream processing options from the local default-kinesis-options.json file and
 * fills in any missing options with the static default options.
 * @returns {StreamProcessingOptions} the default stream processing options
 */
function getDefaultKinesisStreamProcessingOptions() {
  // Load the local default sequenced options if sequencing is required; otherwise load the default unsequenced options
  const stdOptions = loadKinesisDefaultOptions();

  const defaultOptions = stdOptions && stdOptions.streamProcessingOptions && typeof stdOptions.streamProcessingOptions === 'object' ?
    stdOptions.streamProcessingOptions : {};

  const staticDefaults = getKinesisStaticDefaults();
  return merge(staticDefaults, defaultOptions);
}

/**
 * Loads a copy of the default Kinesis stream processing options from the local default-kinesis-options.json file.
 * @returns {StreamProcessingOptions} the default stream processing options
 */
function loadKinesisDefaultOptions() {
  return copy(require('./default-kinesis-options.json'), deep);
}

/**
 * Gets the last-resort, static defaults for Kinesis stream processing options.
 * @returns {StreamProcessingOptions} last-resort, static defaults for Kinesis stream processing options
 */
function getKinesisStaticDefaults() {
  return {
    // Generic settings
    streamType: StreamType.kinesis,
    sequencingRequired: true,
    sequencingPerKey: false,
    batchKeyedOnEventID: false,
    kplEncoded: false,
    consumerIdSuffix: undefined,
    timeoutAtPercentageOfRemainingTime: defaults.timeoutAtPercentageOfRemainingTime,
    maxNumberOfAttempts: defaults.maxNumberOfAttempts,
    avoidEsmCache: false,

    idPropertyNames: undefined, // Optional, but RECOMMENDED - otherwise complicates duplicate elimination & batch state loading/saving if not specified
    keyPropertyNames: undefined, // OPTIONAL, if unsequenced or sequencing per shard; but MANDATORY, if sequencing per key per shard
    seqNoPropertyNames: undefined, // Optional, since will derive seqNos from kinesis.sequenceNumber if not specified

    batchStateTableName: defaults.batchStateTableName,
    // Specialised settings needed by default implementations - e.g. DRQ and DMQ stream names
    deadRecordQueueName: defaults.deadRecordQueueName,
    deadMessageQueueName: defaults.deadMessageQueueName
  };
}

function validateKinesisStreamProcessingConfiguration(context) {
  // Run the common validations
  streamProcessing.validateStreamProcessingConfiguration(context);

  const sequencingRequired = context.streamProcessing.sequencingRequired;
  const sequencingPerKey = context.streamProcessing.sequencingPerKey;

  const error = context.error || console.error;
  // const warn = context.warn || console.warn;

  const kplEncoded = context.streamProcessing.kplEncoded;
  const extractMessagesFromRecord = context.streamProcessing.extractMessagesFromRecord;
  const extractMessageFromRecord = context.streamProcessing.extractMessageFromRecord;

  if ((extractMessagesFromRecord === extractMessagesFromKinesisRecord ||
      extractMessagesFromRecord === extractMessagesFromKplEncodedRecord) && typeof extractMessageFromRecord !== 'function') {
    const errMsg = `FATAL - Mis-configured with ${extractMessagesFromRecord.name}, but WITHOUT an extractMessageFromRecord function - cannot extract any message from any Kinesis record or user record! Fix by configuring one and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    error(errMsg);
    throw new FatalError(errMsg);
  }

  if (kplEncoded) {
    if (extractMessagesFromRecord === extractMessagesFromKinesisRecord) {
      const errMsg = `FATAL - Mis-configured as KPL encoded, but WITH incompatible, non-KPL encoded ${extractMessagesFromKinesisRecord.name} function! Fix and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
      error(errMsg);
      throw new FatalError(errMsg);
    }
    if (extractMessageFromRecord === extractJsonMessageFromKinesisRecord) {
      const errMsg = `FATAL - Mis-configured as KPL encoded, but WITH incompatible, non-KPL encoded ${extractJsonMessageFromKinesisRecord.name} function! Fix and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
      error(errMsg);
      throw new FatalError(errMsg);
    }
  } else { // non-KPL encoded
    if (extractMessagesFromRecord === extractMessagesFromKplEncodedRecord) {
      const errMsg = `FATAL - Mis-configured as non-KPL encoded, but WITH incompatible, KPL encoded ${extractMessagesFromKplEncodedRecord.name} function! Fix and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
      error(errMsg);
      throw new FatalError(errMsg);
    }
    if (extractMessageFromRecord === extractJsonMessageFromKplUserRecord) {
      const errMsg = `FATAL - Mis-configured as non-KPL encoded, but WITH incompatible, KPL encoded ${extractJsonMessageFromKplUserRecord.name} function! Fix and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
      error(errMsg);
      throw new FatalError(errMsg);
    }
  }

  // Validate message sequencing configuration
  if (!sequencingRequired) { // Sequencing is NOT required, but ...
    // warn(`Configured with sequencingRequired (${sequencingRequired}) - messages will be processed out of sequence!`);
    if (sequencingPerKey) {
      // warn(`Configured with sequencingRequired (${sequencingRequired}), but sequencingPerKey (${sequencingPerKey}) - messages will be processed out of sequence & sequencingPerKey will be ignored`);
      const errMsg = `FATAL - Mis-configured with sequencingRequired (${sequencingRequired}), but sequencingPerKey (${sequencingPerKey}). Fix conflict ASAP by setting sequencingRequired to true (for sequencing per key per shard) or sequencingPerKey to false (for unsequenced)!`;
      error(errMsg);
      throw new FatalError(errMsg);
    }
  }
  // else { // Sequencing is required, but ...
  //   if (!sequencingPerKey) {
  //     warn(`Configured with sequencingRequired (${sequencingRequired}), but sequencingPerKey (${sequencingPerKey}) - messages will be processed one after another in a SINGLE sequence per shard (instead of in a sequence per key per shard)`);
  //   }
  // }

  // Only check id/key/seqNo property names if configured with the default resolveKinesisMessageIdsAndSeqNos
  if (context.streamProcessing.resolveMessageIdsAndSeqNos === kinesisIdentify.resolveKinesisMessageIdsAndSeqNos) {
    // const idPropertyNames = context.streamProcessing.idPropertyNames; // Settings.getIdPropertyNames(context);

    const keyPropertyNames = context.streamProcessing.keyPropertyNames; // Settings.getKeyPropertyNames(context);
    const hasKeyPropertyNames = keyPropertyNames && keyPropertyNames.length > 0;

    // const seqNoPropertyNames = context.streamProcessing.seqNoPropertyNames; // Settings.getSeqNoPropertyNames(context);
    // const hasSeqNoPropertyNames = seqNoPropertyNames && seqNoPropertyNames.length > 0;

    // if (!idPropertyNames || idPropertyNames.length <= 0) {
    //   warn(`Configured WITHOUT idPropertyNames (${JSON.stringify(idPropertyNames)}), which complicates duplicate elimination & loading/saving of batch state`);
    // }

    if (hasKeyPropertyNames) { // Key property names configured, but ...
      if (!sequencingRequired || !sequencingPerKey) {
        // const impact = sequencingRequired ?
        //   'messages will be processed one after another in a SINGLE sequence per shard (instead of in a sequence per key per shard)' :
        //   'messages will be processed out of sequence';
        // warn(`Configured with keyPropertyNames (${JSON.stringify(keyPropertyNames)}), but with sequencingRequired (${sequencingRequired}) & sequencingPerKey (${sequencingPerKey}) - keyPropertyNames will be ignored - ${impact}. Fix by either removing keyPropertyNames or by setting both sequencingRequired & sequencingPerKey to true`);
        const errMsg = `FATAL - Mis-configured with keyPropertyNames (${JSON.stringify(keyPropertyNames)}), but with sequencingRequired (${sequencingRequired}) & sequencingPerKey (${sequencingPerKey}). Fix conflict ASAP by setting both sequencingRequired & sequencingPerKey to true (for sequencing per key per shard) OR by removing keyPropertyNames (for ${sequencingRequired ? 'sequencing per shard' : 'unsequenced'})!`;
        error(errMsg);
        throw new FatalError(errMsg);
      }
      // if (!hasSeqNoPropertyNames) {
      //   warn(`Configured with keyPropertyNames (${JSON.stringify(keyPropertyNames)}), but WITHOUT seqNoPropertyNames (${JSON.stringify(seqNoPropertyNames)}). Fix by either configuring both or removing both`);
      // }
    } else { // No key property names configured, but ...
      if (sequencingRequired && sequencingPerKey) {
        const errMsg = `FATAL - Mis-configured with sequencingPerKey (${sequencingPerKey}), but WITHOUT keyPropertyNames (${JSON.stringify(keyPropertyNames)}). Fix conflict ASAP by setting keyPropertyNames (for sequencing per key per shard) or setting sequencingPerKey to false (for sequencing per shard)!`;
        error(errMsg);
        throw new FatalError(errMsg);
      }
      // if (hasSeqNoPropertyNames) {
      //   warn(`Configured with seqNoPropertyNames (${JSON.stringify(seqNoPropertyNames)}), but WITHOUT keyPropertyNames (${JSON.stringify(keyPropertyNames)}). Fix by either configuring both or removing both`);
      // }
    }

    // if (!hasSeqNoPropertyNames) {
    //   warn(`Configured WITHOUT seqNoPropertyNames (${JSON.stringify(seqNoPropertyNames)}), which is NOT recommended (forces use of kinesis.sequenceNumber properties to sequence messages). Fix by configuring seqNoPropertyNames.`);
    // }
  }
}

/**
 * A default `extractMessagesFromRecord` function for extracting a single message from the given normal (i.e. non-KPL
 * encoded) Kinesis stream event record that uses the given `extractMessageFromRecord` function to convert the record
 * into a corresponding message and then adds the successfully extracted message, rejected message or unusable record to
 * the given batch.
 * @see {@link ExtractMessagesFromRecord}
 * @see {@link ExtractMessageFromRecord}
 * @param {KinesisEventRecord} record - a Kinesis stream event record
 * @param {Batch} batch - the batch to which to add the extracted message or unusable record
 * @param {ExtractMessageFromRecord|undefined} [extractMessageFromRecord] - the actual function to use to extract a message from the record or user record (if any)
 * @param {StreamProcessing} context - the context to use
 * @return {Promise.<MsgOrUnusableRec[]>} a promise of an array containing one message, rejected message or unusable record
 */
function extractMessagesFromKinesisRecord(record, batch, extractMessageFromRecord, context) {
  const messageOutcome = Try.try(() => extractMessageFromRecord(record, undefined, context));

  // Add the message (if any) or unusable record to the batch
  return messageOutcome.map(
    message => {
      return [batch.addMessage(message, record, undefined, context)];
    },
    err => {
      return [{unusableRec: batch.addUnusableRecord(record, undefined, err.message, context)}];
    }
  ).toPromise();
}

// noinspection JSUnusedLocalSymbols
/**
 * A default `extractMessageFromRecord` function that attempts to extract and parse the original JSON message object
 * from the given non-KPL encoded Kinesis stream event record and returns the message or throws an error (if unparseable).
 * @see {@link ExtractMessageFromRecord}
 * @param {KinesisEventRecord} record - a Kinesis stream event record
 * @param {undefined} [userRecord] - always regarded as undefined, since user records are not applicable for non-KPL encoded records
 * @param {StreamProcessing} context - the context to use
 * @return {Message} the message object (if successfully extracted)
 * @throws {Error} an error if a message could not be successfully extracted from the given record
 */
function extractJsonMessageFromKinesisRecord(record, userRecord, context) {
  // First convert the Kinesis record's kinesis.data field back from Base 64 to UTF-8
  const dataInBase64 = record && record.kinesis ? record.kinesis.data : undefined;
  const data = dataInBase64 && new Buffer(dataInBase64, 'base64').toString('utf-8');

  if (context.traceEnabled) context.trace(`Parsing Kinesis record (${record.eventID}) data (${data})`);

  try {
    // Convert the decoded Kinesis record's data back into its original JSON message object form
    return JSON.parse(data);

  } catch (err) {
    context.error(`Failed to parse decoded Kinesis user record (${record.eventID}) data (${data}) back to a JSON message object`, err);
    throw err;
  }
}

/**
 * A default `extractMessagesFromRecord` function for extracting one or more messages from a Kinesis Producer Library
 * (KPL) encoded Kinesis stream event record (with or without aggregation) that first extracts one or more user records
 * from the record using the `aws-kinesis-agg` module and then uses the given `extractMessageFromRecord` function to
 * convert each of the user records into a corresponding message and then finally adds each extracted message, rejected
 * message or unusable record to the given batch.
 * @see {@link ExtractMessagesFromRecord}
 * @see {@link ExtractMessageFromRecord}
 * @param {KinesisEventRecord} record - a Kinesis stream event record
 * @param {Batch} batch - the batch to which to add the extracted messages or unusable records
 * @param {ExtractMessageFromRecord|undefined} [extractMessageFromRecord] - the actual function to use to extract a message from the record or user record (if any)
 * @param {StreamProcessing} context - the context to use
 * @return {Promise.<MsgOrUnusableRec[]>} a promise of an array containing one or more messages, rejected messages and/or unusable records
 */
function extractMessagesFromKplEncodedRecord(record, batch, extractMessageFromRecord, context) {
  // Extract one or more UserRecords from the KPL stream event record
  if (!deaggregateAsync) {
    // if (!agg) agg = require('aws-kinesis-agg');
    // const agg = require('aws-kinesis-agg');
    deaggregateAsync = require('./kpl-deagg-async').deaggregateAsync;
  }
  const computeChecksums = false;
  return deaggregateAsync(record.kinesis, computeChecksums).then(
    userRecordOutcomes => {
      // Extract all of the messages from the User Records
      return userRecordOutcomes.map(userRecordOutcome => {
        return userRecordOutcome.map(
          userRecord => {
            const messageOutcome = Try.try(() => extractMessageFromRecord(record, userRecord, context));

            // Add the message (if any) or unusable record to the batch
            return messageOutcome.map(
              message => {
                return batch.addMessage(message, record, userRecord, context);
              },
              err => {
                return {unusableRec: batch.addUnusableRecord(record, userRecord, err.message, context)};
              }
            );
          },
          err => {
            return {unusableRec: batch.addUnusableRecord(record, undefined, err.message, context)};
          }
        ).get();
      });
    },
    err => {
      context.error(`Failed to extract message(s) from record (${record.eventID})`, err);
      return [{unusableRec: batch.addUnusableRecord(record, undefined, err.message, context)}];
    }
  );
}

/**
 * A default `extractMessageFromRecord` function that attempts to extract and parse the original JSON message object
 * from the given Kinesis stream event record and/or given user record and returns the message or throws an error (if
 * unparseable).
 * @see {@link ExtractMessageFromRecord}
 * @param {KinesisEventRecord} record - a Kinesis stream event record
 * @param {UserRecord|undefined} [userRecord] - a user record extracted from the Kinesis stream event record using the `aws-kinesis-agg` module
 * @param {StreamProcessing} context - the context to use
 * @return {Message} the message object (if successfully extracted)
 * @throws {Error} an error if a message could not be successfully extracted from the given record
 */
function extractJsonMessageFromKplUserRecord(record, userRecord, context) {
  // First convert the Kinesis user record's data field (or, if none, the record's kinesis.data field) back from Base 64 to UTF-8
  const dataInBase64 = userRecord ? userRecord.data : record && record.kinesis ? record.kinesis.data : undefined;
  const data = dataInBase64 && new Buffer(dataInBase64, 'base64').toString('utf-8');

  if (context.traceEnabled) context.trace(`Parsing Kinesis user record (${record.eventID}${userRecord && userRecord.subSequenceNumber !== undefined ? `, ${userRecord.subSequenceNumber}` : ''}) data (${data})`);

  try {
    // Convert the decoded Kinesis user record data back into its original JSON message object form
    return JSON.parse(data);

  } catch (err) {
    context.error(`Failed to parse decoded Kinesis user record (${record.eventID}${userRecord && userRecord.subSequenceNumber !== undefined ? `, ${userRecord.subSequenceNumber}` : ''}) data (${data}) back to a JSON message object`, err);
    throw err;
  }
}

/**
 * Discards the given unusable stream event record to the DRQ (i.e. Dead Record Queue).
 * Default implementation of a {@link DiscardUnusableRecord} function.
 * @param {UnusableRecord|Record} unusableRecord - the unusable record to discard
 * @param {Batch} batch - the batch being processed
 * @param {StreamProcessing} context - the context to use
 * @return {Promise} a promise that will complete when the unusable record is discarded
 */
function discardUnusableRecordToDRQ(unusableRecord, batch, context) {
  return streamProcessing.discardUnusableRecordToDRQ(unusableRecord, batch, toDRQPutRequestFromKinesisUnusableRecord, context);
}

// noinspection JSUnusedLocalSymbols
function toDRQPutRequestFromKinesisUnusableRecord(unusableRecord, batch, deadRecordQueueName, context) {
  const recState = batch.states.get(unusableRecord);

  const record = recState.record;
  const userRecord = recState.userRecord;
  const kinesis = (record && record.kinesis) || (unusableRecord && unusableRecord.kinesis);

  // const eventID = recState.eventID || (record && record.eventID) || unusableRecord.eventID;
  // const eventSeqNo = recState.eventSeqNo || (kinesis && kinesis.sequenceNumber);
  // const eventSubSeqNo = recState.eventSubSeqNo;
  const reasonUnusable = recState.reasonUnusable;

  const state = copy(recState, deep);
  delete state.record;
  delete state.userRecord;

  // delete state.eventID;
  // delete state.eventSeqNo;
  // delete state.eventSubSeqNo;
  // delete state.reasonUnusable;

  const deadRecord = {
    streamConsumerId: batch.streamConsumerId,
    shardOrEventID: batch.shardOrEventID,
    ver: 'DR|K|2.0',
    // eventID: eventID,
    // eventSeqNo: eventSeqNo,
    // eventSubSeqNo: eventSubSeqNo,
    unusableRecord: unusableRecord,
    record: record !== unusableRecord ? record : undefined, // don't need BOTH unusableRecord & record if same
    userRecord: userRecord !== unusableRecord ? userRecord : undefined, // don't need BOTH unusableRecord & userRecord if same
    state: state,
    reasonUnusable: reasonUnusable,
    discardedAt: new Date().toISOString()
  };

  // Generate a partition key to use for the DRQ request
  const partitionKey = generatePartitionKey(kinesis, batch);

  const request = {
    StreamName: deadRecordQueueName,
    PartitionKey: partitionKey,
    Data: JSON.stringify(deadRecord)
  };

  const explicitHashKey = kinesis && kinesis.explicitHashKey;
  if (explicitHashKey) {
    request.ExplicitHashKey = explicitHashKey;
  }

  return request;
}

/**
 * Routes the given rejected message to the DMQ (i.e. Dead Message Queue).
 * Default implementation of a {@link DiscardRejectedMessage} function.
 * @param {Message} rejectedMessage - the rejected message to discard
 * @param {Batch} batch - the batch being processed
 * @param {StreamProcessing} context the context to use
 * @return {Promise}
 */
function discardRejectedMessageToDMQ(rejectedMessage, batch, context) {
  return streamProcessing.discardRejectedMessageToDMQ(rejectedMessage, batch, toDMQPutRequestFromKinesisRejectedMessage, context);
}

// noinspection JSUnusedLocalSymbols
function toDMQPutRequestFromKinesisRejectedMessage(message, batch, deadMessageQueueName, context) {
  const msgState = batch.states.get(message);

  const record = msgState.record;
  const userRecord = msgState.userRecord;
  const kinesis = record && record.kinesis;

  // const eventID = msgState.eventID || record.eventID;
  // const eventSeqNo = msgState.eventSeqNo || (kinesis && kinesis.sequenceNumber);
  // const eventSubSeqNo = msgState.eventSubSeqNo;

  const state = copy(msgState, deep);
  delete state.record;
  delete state.userRecord;

  // delete state.eventID;
  // delete state.eventSeqNo;
  // delete state.eventSubSeqNo;
  // delete state.reasonRejected;

  // // Replace ids, keys & seqNos with enumerable versions
  // delete state.ids;
  // delete state.keys;
  // delete state.seqNos;
  // state.ids = msgState.ids;
  // state.keys = msgState.keys;
  // state.seqNos = msgState.seqNos;

  // Wrap the message in a rejected message "envelope" with metadata
  const rejectedMessage = {
    streamConsumerId: batch.streamConsumerId,
    shardOrEventID: batch.shardOrEventID,
    ver: 'DM|K|2.0',
    // eventID: eventID,
    // eventSeqNo: eventSeqNo,
    // eventSubSeqNo: eventSubSeqNo,
    // ids: state.ids,
    // keys: state.keys,
    // seqNos: state.seqNos,
    message: message,
    record: record,
    userRecord: userRecord,
    state: state,
    reasonRejected: batch.findReasonRejected(message),
    discardedAt: new Date().toISOString()
  };

  // Generate a partition key to use for the DMQ request
  const partitionKey = generatePartitionKey(kinesis, batch);

  const request = {
    StreamName: deadMessageQueueName,
    PartitionKey: partitionKey,
    Data: JSON.stringify(rejectedMessage)
  };

  const explicitHashKey = kinesis && kinesis.explicitHashKey;
  if (explicitHashKey) {
    request.ExplicitHashKey = explicitHashKey;
  }

  return request;
}

function generatePartitionKey(kinesis, batch) {
  return (kinesis && isNotBlank(kinesis.partitionKey) ? kinesis.partitionKey :
    isNotBlank(batch.streamConsumerId) ? batch.streamConsumerId : LAST_RESORT_KEY).substring(0, MAX_PARTITION_KEY_SIZE);
}