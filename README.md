# kinesis-stream-consumer v2.0.0

Utilities for building robust AWS Lambda consumers of stream events from Amazon Web Services (AWS) Kinesis streams.

## Modules:
- `kinesis-consumer.js` module
  - Utilities and functions to be used to configure and robustly consume messages from an AWS Kinesis stream
- `kinesis-processing.js` module 
  - Utilities for configuring Kinesis stream processing, which configures and determines the processing behaviour of a 
    Kinesis stream consumer
- `kinesis-identify.js` module
  - Utilities and functions to be used by a Kinesis stream consumer to identify messages
- `kpl-deagg-async.js` module
  -  Copy of `kpl-deagg.js` with a new promise-returning `deaggregateAsync` function based on `deaggregate` function  
## Dependencies
- `aws-stream-consumer-core` module - Common AWS stream consumer libraries used by this Kinesis stream consumer module

## Purpose

The goal of the AWS Kinesis stream consumer functions is to make the process of consuming records from an AWS Kinesis 
stream more robust for an AWS Lambda stream consumer by providing solutions to and workarounds for common AWS stream 
consumption issues (see `Background` section of `aws-stream-consumer-core/README.md`). 

## Installation
This module is exported as a [Node.js](https://nodejs.org/) module.

Using npm:
```bash
$ {sudo -H} npm i -g npm
$ npm i --save kinesis-stream-consumer
```

## Usage 

To use the `kinesis-stream-consumer` module:

* Define the tasks that you want to execute on individual messages and/or on the entire batch of messages
```js
// Assuming the following example functions are meant to be used during processing:
// noinspection JSUnusedLocalSymbols
function saveMessageToDynamoDB(message, batch, context) { /* ... */ }
// noinspection JSUnusedLocalSymbols
function sendPushNotification(notification, recipients, context) { /* ... */ }
// noinspection JSUnusedLocalSymbols
function sendEmail(from, to, email, context) { /* ... */ }
// noinspection JSUnusedLocalSymbols
function logMessagesToS3(batch, incompleteMessages, context) { /* ... */ }

// Import TaskDef
const TaskDef = require('task-utils/task-defs');

// Example of creating a task definition to be used to process each message one at a time
const saveMessageTaskDef = TaskDef.defineTask(saveMessageToDynamoDB.name, saveMessageToDynamoDB);

// Example of adding optional sub-task definition(s) to your task definitions as needed
saveMessageTaskDef.defineSubTask(sendPushNotification.name, sendPushNotification);
saveMessageTaskDef.defineSubTask(sendEmail.name, sendEmail);

// Example of creating a task definition to be used to process the entire batch of messages all at once 
const logMessagesToS3TaskDef = TaskDef.defineTask(logMessagesToS3.name, logMessagesToS3); // ... with any sub-task definitions needed

const processOneTaskDefs = [saveMessageTaskDef]; // ... and/or more "process one" task definitions
const processAllTaskDefs = [logMessagesToS3TaskDef]; // ... and/or more "process all" task definitions
```

* Generate an AWS Lambda handler function that will configure and process stream events according to the given settings & options
```js
const streamConsumer = require('kinesis-stream-consumer');

const logging = require('logging-utils');
const LogLevel = logging.LogLevel;
const Settings = require('kinesis-stream-consumer/settings');

// Create a context object
const context = {}; // ... or your own pre-configured context object

// Use undefined to use the default settings, if the default behaviour is adequate
const settings = undefined; 
// OR - Use your own settings for custom configuration of any or all logging, stage handling and/or stream processing settings & functions
const settings2 = {/* ... */};

// Use your own options json file for custom configuration of any or all logging, stage handling and/or stream processing options
// - For sequenced Kinesis stream consumption, copy `default-kinesis-seq-options.json` as a starting point for your options file
// - For unsequenced Kinesis stream consumption, copy `default-kinesis-unseq-options.json` as a starting point for your options file
// - For DynamoDB stream consumption, copy `default-dynamodb-options.json` as a starting point for your options file 
const options = require('./default-kinesis-options.json'); // your-custom-options.json

// Generate an AWS Lambda handler function that will configure and process stream events 
// according to the given settings & options (and use defaults for optional arguments)
exports.handler = streamConsumer.generateHandlerFunction(context, settings, options, processOneTaskDefs, processAllTaskDefs);

// OR ... with optional arguments included
exports.handler = streamConsumer.generateHandlerFunction(context, settings, options, processOneTaskDefs, processAllTaskDefs, 
  LogLevel.DEBUG, 'Failed to do Xyz', 'Finished doing Xyz');
```

* ALTERNATIVELY, configure your own AWS Lambda handler function using the following functions:
  (See `kinesis-consumer.js` `generateHandlerFunction` for an example handler function)

  * Configure the stream consumer
```js
const streamConsumer = require('kinesis-stream-consumer');
// ...

// Configure the stream consumer's dependencies and runtime settings
streamConsumer.configureStreamConsumer(context, settings, options, awsEvent, awsContext);
```
  * Process the AWS Kinesis or DynamoDB stream event
```js
const streamConsumer = require('kinesis-stream-consumer');
// ...

const promise = streamConsumer.processStreamEvent(awsEvent, processOneTaskDefs, processAllTaskDefs, context);
```

* Within your custom task execute function(s), update the message's (or messages') tasks' and/or sub-tasks' states
* Example custom "process one at a time" task execute function for processing one message at a time
```js
// noinspection JSUnusedLocalSymbols
function saveMessageToDynamoDB(message, batch, context) {
  // Note that 'this' will be the currently executing task witin your custom task execute function
  const task = this; 
  const sendPushNotificationTask = task.getSubTask(sendPushNotification.name);
  const sendEmailTask = task.getSubTask(sendEmail.name);
  
  // ... OR alternatively from anywhere in the flow of your custom execute code
  const task1 = batch.getProcessOneTask(message, saveMessageToDynamoDB.name);
  const subTaskA = task1.getSubTask(sendPushNotification.name);
  const subTaskB = task1.getSubTask(sendEmail.name);
  
  // ... execute your actual logic (e.g. save the message to DynamoDB) 
  
  // If your logic succeeds, then start executing your task's sub-tasks, e.g.
  sendPushNotificationTask.execute('Notification ...', ['a@bc.com'], context);
  
  sendEmailTask.execute(from, to, email, context);

  context.debug(`subTask.state = ${subTaskA.state.type}, subTaskB.state = ${subTaskB.state.type}`);
    
  // If necessary, change the task's and/or sub-tasks' states based on outcomes, e.g.
  task.fail(new Error('Task failed'));
  
  // ...
}

// noinspection JSUnusedLocalSymbols
function sendPushNotification(notification, recipients, context) {
  const task = this;
  
  // ... execute your actual send push notification logic 
  
  // If necessary, change the task's state based on the outcome, e.g.
  task.succeed(result);

  // ...
}

// noinspection JSUnusedLocalSymbols
function sendEmail(from, to, email, context) {
  const task = this;
  
  // ... execute your actual send email logic 
  console.log(`Sending email from (${from}) to (${to}) email (${email}) for stage (${context.stage})`);
  
  // If necessary, change the task's state based on the outcome, e.g.
  task.reject('Invalid email address', new Error('Invalid email address'), true);

  // ...
}

console.log(`DEBUG saveMessageToDynamoDB.name = ${saveMessageToDynamoDB.name}`);
```

* Example custom "process all at once" task execute function for processing the entire batch of messages
```js
// noinspection JSUnusedLocalSymbols
function logMessagesToS3(batch, incompleteMessages, context) {
  // Note that 'this' will be the currently executing master task within your custom task execute function
  // NB: Master tasks and sub-tasks will apply any state changes made to them to every message in the batch
  const masterTask = this; 
  const masterSubTask = masterTask.getSubTask('doX');
  
  context.debug(`masterSubTask has ${masterSubTask.slaveTasks.length} slave tasks`);
  
  // ... or alternatively from anywhere in the flow of your custom execute code
  const masterTask1 = batch.getProcessAllTask(batch, logMessagesToS3.name);
  const masterSubTask1 = masterTask1.getSubTask('doX');
  
  const masterSubTask2 = masterTask1.getSubTask('doY');
  
  // ...
  context.debug(`Logging batch of ${incompleteMessages.length} incomplete of ${batch.messages.length} messages to S3`);
  
  // Change the master task's and/or sub-tasks' states based on outcomes, e.g.
  masterSubTask1.succeed({ok:true});
  
  // ...
  
  masterSubTask2.reject('Cannot do X', new Error('X is un-doable'), true);
  
  // ...
  
  masterTask.fail(new Error('Task failed'));
    
  // ...
  
  // ALTERNATIVELY (or in addition) change the task state of individual messages
  const firstMessage = messages[0]; // e.g. working with the first message in the batch
  const messageTask1 = batch.getProcessAllTask(firstMessage, logMessagesToS3.name, context);
  const messageSubTask1 = messageTask1.getSubTask('doX');
  messageSubTask1.reject('Cannot do X on first message', new Error('X is un-doable on first message'), true);
  messageTask1.fail(new Error('Task failed on first message'));
  
  // ...
}

console.log(`DEBUG logMessagesToS3.name = ${logMessagesToS3.name}`);
```

## Unit tests
This module's unit tests were developed with and must be run with [tape](https://www.npmjs.com/package/tape). The unit tests have been tested on [Node.js v4.3.2](https://nodejs.org/en/blog/release/v4.3.2/).  

Install tape globally if you want to run multiple tests at once:
```bash
$ npm install tape -g
```

Run all unit tests with:
```bash
$ npm test
```
or with tape:
```bash
$ tape test/*.js
```

See the [package source](https://github.com/byron-dupreez/kinesis-stream-consumer) for more details.

## Changes
See [release_notes.md](./release_notes.md)