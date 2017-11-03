'use strict';

// AWS core utilities
const stages = require('aws-core-utils/stages');

const copying = require('core-functions/copying');
const copy = copying.copy;
const deep = copying.defaultCopyOpts.deep;

const Strings = require('core-functions/strings');
const isNotBlank = Strings.isNotBlank;

// Task utilities
const taskUtils = require('task-utils');

const logging = require('logging-utils');
const LogLevel = logging.LogLevel;
const log = logging.log;

const kinesisStreamProcessing = require('./kinesis-processing');
const streamConsumer = require('aws-stream-consumer-core/stream-consumer');
const streamProcessing = require('aws-stream-consumer-core/stream-processing');

/**
 * Utilities and functions to be used to robustly consume messages from an AWS Kinesis event stream.
 * @module kinesis-stream-consumer/kinesis-consumer
 * @author Byron du Preez
 */
exports._ = '_'; //IDE workaround

// Configuration
exports.isStreamConsumerConfigured = isStreamConsumerConfigured;
exports.configureStreamConsumer = configureStreamConsumer;
exports.generateHandlerFunction = generateHandlerFunction;

// Processing
exports.processStreamEvent = processStreamEvent;


// =====================================================================================================================
// Kinesis stream consumer configuration - configures the runtime settings for a Kinesis stream consumer on a given
// context from a given AWS event and AWS context
// =====================================================================================================================

/**
 * Returns true if the Kinesis stream consumer's dependencies and runtime settings have been configured on the given
 * context; otherwise returns false.
 * @param {StreamConsumerContext|StreamProcessing|Object} context - the context to check
 * @returns {boolean} true if configured; false otherwise
 */
function isStreamConsumerConfigured(context) {
  return !!context && logging.isLoggingConfigured(context) && stages.isStageHandlingConfigured(context) &&
    context.region && context.stage && context.awsContext && context.taskFactory &&
    kinesisStreamProcessing.isStreamProcessingConfigured(context);
}

/**
 * Configures the dependencies and settings for the Kinesis stream consumer on the given context from the given settings,
 * the given options, the given AWS event and the given AWS context in preparation for processing of a batch of Kinesis
 * stream records. Any error thrown must subsequently trigger a replay of all the records in the current batch until the
 * Lambda can be fixed.
 *
 * Note that if the given event or AWS context are undefined, then everything other than the event and/or AWS context
 * and stage will be configured. This missing configuration can be configured at a later point in your code by invoking
 * {@linkcode module:aws-stream-consumer-core/stream-consumer#configureEventAwsContextAndStage}. This separation of
 * configuration is primarily useful for unit testing.
 *
 * @param {Object|StreamProcessing|StandardContext|StreamConsumerContext} context - the context onto which to configure stream consumer settings
 * @param {StreamConsumerSettings|undefined} [settings] - optional stream consumer settings to use
 * @param {StreamConsumerOptions|undefined} [options] - optional stream consumer options to use
 * @param {AWSEvent|undefined} [event] - the AWS event, which was passed to your lambda
 * @param {AWSContext|undefined} [awsContext] - the AWS context, which was passed to your lambda
 * @return {StreamConsumerContext} the given context object configured with full or partial stream consumer settings
 * @throws {Error} an error if event and awsContext are specified and the region and/or stage cannot be resolved or if
 * a task factory cannot be constructed from the given `settings.taskFactorySettings`
 */
function configureStreamConsumer(context, settings, options, event, awsContext) {
  // Configure stream processing and all of the standard settings (including logging, stage handling, etc)
  kinesisStreamProcessing.configureStreamProcessing(context, settings ? settings.streamProcessingSettings : undefined,
    options ? options.streamProcessingOptions : undefined, settings, options, event, awsContext, false);

  streamProcessing.configureConsumerId(context, awsContext);

  // Configure the task factory to be used to create tasks (NB: must be configured after configuring logging above, so
  // that the context can be used as the task factory's logger)
  const taskFactorySettings = settings && typeof settings.taskFactorySettings === 'object' ?
    copy(settings.taskFactorySettings, {deep: false}) : {};

  if (!taskFactorySettings.logger || !logging.isMinimumViableLogger(taskFactorySettings.logger)) {
    taskFactorySettings.logger = context;
  }
  taskUtils.configureTaskFactory(context, taskFactorySettings, options ? options.taskFactoryOptions : undefined);

  return context;
}

/**
 * Generates a handler function for your Kinesis stream consumer Lambda.
 *
 * @param {undefined|function(): (Object|StreamConsumerContext|StreamProcessing|StandardContext)} generateContext - a optional function that will be used to generate the initial context to be configured & used
 * @param {undefined|StreamConsumerSettings|function(): StreamConsumerSettings} [generateSettings] - an optional function that will be used to generate initial stream consumer settings to use; OR optional module-scoped stream consumer settings from which to copy initial stream consumer settings to use
 * @param {undefined|StreamConsumerOptions|function(): StreamConsumerOptions} [generateOptions] - an optional function that will be used to generate initial stream consumer options to use; OR optional module-scoped stream consumer options from which to copy initial stream consumer options to use
 * @param {undefined|function(): TaskDef[]} [generateProcessOneTaskDefs] - an "optional" function that must generate a new list of "processOne" task definitions, which will be subsequently used to generate the tasks to be executed on each message independently
 * @param {undefined|function(): TaskDef[]} [generateProcessAllTaskDefs] - an "optional" function that must generate a new list of "processAll" task definitions, which will be subsequently used to generate the tasks to be executed on all of the event's messages collectively
 * @param {Object|undefined} [opts] - optional options to use to configure the generated handler function
 * @param {LogLevel|string|undefined} [opts.logEventResultAtLogLevel] - an optional log level at which to log the AWS stream event and result; if log level is undefined or invalid, then logs neither
 * @param {string|undefined} [opts.failureMsg] - an optional message to log at error level on failure
 * @param {string|undefined} [opts.successMsg] an optional message to log at info level on success
 * @returns {AwsLambdaHandlerFunction} a handler function for your stream consumer Lambda
 */
function generateHandlerFunction(generateContext, generateSettings, generateOptions, generateProcessOneTaskDefs,
  generateProcessAllTaskDefs, opts) {

  const logEventResultAtLogLevel = opts && logging.isValidLogLevel(opts.logEventResultAtLogLevel) ?
    opts.logEventResultAtLogLevel : undefined;
  const failureMsg = opts && opts.failureMsg;
  const successMsg = opts && opts.successMsg;

  /**
   * A stream consumer Lambda handler function.
   * @param {AWSEvent} event - the AWS stream event passed to your handler
   * @param {AWSContext} awsContext - the AWS context passed to your handler
   * @param {Callback} callback - the AWS Lambda callback function passed to your handler
   */
  function handler(event, awsContext, callback) {
    let context = undefined;
    try {
      // Configure the context as a stream consumer context
      context = typeof generateContext === 'function' ? generateContext() : {};

      const settings = typeof generateSettings === 'function' ? copy(generateSettings(), deep) :
        generateSettings && typeof generateSettings === 'object' ? copy(generateSettings, deep) : undefined;

      const options = typeof generateOptions === 'function' ? copy(generateOptions(), deep) :
        generateOptions && typeof generateOptions === 'object' ? copy(generateOptions, deep) : undefined;

      context = configureStreamConsumer(context, settings, options, event, awsContext);

      // Optionally log the event at the given log level
      if (logEventResultAtLogLevel) {
        context.log(logEventResultAtLogLevel, 'Event:', JSON.stringify(event));
      }

      // Generate the "process one" and/or "process all" task definitions using the given functions
      const processOneTaskDefs = typeof generateProcessOneTaskDefs === 'function' ? generateProcessOneTaskDefs() : [];
      const processAllTaskDefs = typeof generateProcessAllTaskDefs === 'function' ? generateProcessAllTaskDefs() : [];

      // Process the stream event with the generated "process one" and/or "process all" task definitions
      processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context)
        .then(result => {
          // Optionally log the result at the given log level
          if (logEventResultAtLogLevel) {
            context.log(logEventResultAtLogLevel, 'Result:', JSON.stringify(result));
          }
          // Log the given success message (if any)
          if (isNotBlank(successMsg)) context.info(successMsg);

          // Succeed the Lambda callback
          callback(null, result);
        })
        .catch(err => {
          // Log the error encountered
          context.error(isNotBlank(failureMsg) ? failureMsg : 'Failed to process stream event', err);
          // Fail the Lambda callback
          callback(err, null);
        });

    } catch (err) {
      // Log the error encountered
      log(context, LogLevel.ERROR, isNotBlank(failureMsg) ? failureMsg : 'Failed to process stream event', err);
      // Fail the Lambda callback
      callback(err, null);
    }
  }

  return handler;
}

// =====================================================================================================================
// Process stream event
// =====================================================================================================================

/**
 * Processes the given Kinesis stream event using the given AWS context and context by applying each of the tasks
 * defined by the task definitions in the given processOneTaskDefs and processAllTaskDefs to each message extracted from
 * the event.
 *
 * @param {KinesisEvent|*} event - the AWS Kinesis stream event (or any other garbage passed as an event)
 * @param {TaskDef[]|undefined} [processOneTaskDefsOrNone] - an "optional" list of "processOne" task definitions that
 * will be used to generate the tasks to be executed on each message independently
 * @param {TaskDef[]|undefined} [processAllTaskDefsOrNone] - an "optional" list of "processAll" task definitions that
 * will be used to generate the tasks to be executed on all of the event's messages collectively
 * @param {StreamConsumerContext} context - the context to use with Kinesis stream consumer configuration
 * @returns {Promise.<Batch|BatchError>} a promise that will resolve with the batch processed or reject with an error
 */
function processStreamEvent(event, processOneTaskDefsOrNone, processAllTaskDefsOrNone, context) {
  // Ensure that the stream consumer is fully configured before proceeding, and if not, trigger a replay of all the
  // records until it can be fixed
  if (!isStreamConsumerConfigured(context)) {
    const errMsg = `FATAL - Your Kinesis stream consumer MUST be configured before invoking processStreamEvent (see kinesis-consumer#configureStreamConsumer & kinesis-processing). Fix your Lambda and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    log(context, LogLevel.ERROR, errMsg);
    return Promise.reject(new Error(errMsg));
  }
  return streamConsumer.processStreamEvent(event, processOneTaskDefsOrNone, processAllTaskDefsOrNone, context);
}