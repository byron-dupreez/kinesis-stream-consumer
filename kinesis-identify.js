'use strict';

const objects = require('core-functions/objects');
const hasOwnPropertyWithCompoundName = objects.hasOwnPropertyWithCompoundName;
const getPropertyValueByCompoundName = objects.getPropertyValueByCompoundName;

const settings = require('aws-stream-consumer-core/settings');

const crypto = require('crypto');

const errors = require('core-functions/errors');
const FatalError = errors.FatalError;

/**
 * Utilities and functions to be used by a Kinesis stream consumer to identify messages.
 * @module kinesis-stream-consumer/kinesis-identify
 * @author Byron du Preez
 */
exports._$_ = '_$_'; //IDE workaround

exports.generateKinesisMD5s = generateKinesisMD5s;
exports.resolveKinesisEventIdAndSeqNos = resolveKinesisEventIdAndSeqNos;
exports.resolveKinesisMessageIdsAndSeqNos = resolveKinesisMessageIdsAndSeqNos;

/**
 * Generates MD5 message digest(s) for the given message, its record and/or its optional user record.
 * An implementation of {@link GenerateMD5s}.
 * @param {Message} message - the message for which to generate MD5 message digest(s)
 * @param {Record} record - the record for which to generate MD5 message digest(s)
 * @param {UserRecord|undefined} [userRecord] - the de-aggregated user record from which the message originated (if any)
 * @return {MD5s} the generated MD5 message digest(s)
 */
function generateKinesisMD5s(message, record, userRecord) {
  // Resolve the record's kinesis property
  const kinesis = record && record.kinesis;

  // Generate MD5 message digests
  return {
    msg: message && md5Sum(JSON.stringify(message)),
    rec: record && md5Sum(JSON.stringify(record)),
    userRec: userRecord && md5Sum(JSON.stringify(userRecord)),
    data: kinesis && kinesis.data && md5Sum(kinesis.data)
  };
}

/**
 * Attempts to resolve the event ID, event sequence number and event sub-sequence number of the given Kinesis stream
 * event record. An implementation of {@link ResolveEventIdAndSeqNos}.
 *
 * @param {Record} record - the record from which to extract an eventID and event sequence number(s)
 * @param {UserRecord|undefined} [userRecord] - the de-aggregated user record from which the message originated (if any)
 * @returns {EventIdAndSeqNos} the given record's resolved: eventID; eventSeqNo; and an optional eventSubSeqNo (sourced from the user record's sub-sequence number if any)
 */
function resolveKinesisEventIdAndSeqNos(record, userRecord) {
  // Resolve the record's eventID
  const eventID = record && record.eventID;

  // Resolve the record's kinesis property
  const kinesis = record && record.kinesis;

  // Resolve the record's "event" sequence number (i.e. record.kinesis.sequenceNumber)
  const eventSeqNo = kinesis && kinesis.sequenceNumber;

  // Resolve the user record's "event" sub-sequence number (if any)
  const eventSubSeqNo = userRecord && userRecord.subSequenceNumber;

  return {eventID, eventSeqNo, eventSubSeqNo};
}

// noinspection JSUnusedLocalSymbols
/**
 * Attempts to resolve the ids, keys, and sequence numbers of the given Kinesis stream event message. If message
 * sequencing is required (default) then the resolved seqNos must be non-empty and must not contain any
 * undefined values. If sequencing per key is required then the resolved keys (if any) must also not contain any
 * undefined values. An implementation of {@link ResolveMessageIdsAndSeqNos}.
 * @param {Message} message - the message for which to resolve its ids, keys & sequence numbers
 * @param {Record} record - the record from which the message originated
 * @param {UserRecord|undefined} [userRecord] - the de-aggregated user record from which the message originated (if any)
 * @param {EventIdAndSeqNos} eventIdAndSeqNos - the message's record's eventID, eventSeqNo & eventSubSeqNo (if applicable)
 * @param {MD5s} md5s - the MD5 message digests generated for the message & its record
 * @param {StreamConsumerContext} context - the context to use
 * @returns {MessageIdsAndSeqNos} the given message's resolved: id(s); key(s); and sequence number(s)
 * @throws {Error} an error if no usable keys, ids or sequence numbers can be resolved
 */
function resolveKinesisMessageIdsAndSeqNos(message, record, userRecord, eventIdAndSeqNos, md5s, context) {
  const desc = JSON.stringify(eventIdAndSeqNos);
  const sequencingRequired = settings.isSequencingRequired(context);
  const sequencingPerKey = sequencingRequired && settings.isSequencingPerKey(context);

  // Resolve the configured id, key & sequence property names (if any)
  const idPropertyNames = settings.getIdPropertyNames(context);
  const keyPropertyNames = settings.getKeyPropertyNames(context);
  const seqNoPropertyNames = settings.getSeqNoPropertyNames(context);

  // Resolve the message's id(s) using idPropertyNames if configured; otherwise an empty array
  const ids = idPropertyNames.length > 0 ?
    getPropertyValues(idPropertyNames, message, record, userRecord, false, desc, 'ids', context) : [];

  // Resolve the message's key(s): using keyPropertyNames if configured; otherwise an empty array
  const hasKeyPropertyNames = keyPropertyNames.length > 0;

  if (sequencingPerKey && !hasKeyPropertyNames) {
    throw new FatalError(`FATAL - sequencingPerKey is ${sequencingPerKey}, but keyPropertyNames is NOT configured [${keyPropertyNames.join(', ')}]. Fix misconfiguration ASAP!`);
  }

  const keys = hasKeyPropertyNames ?
    getPropertyValues(keyPropertyNames, message, record, userRecord, sequencingPerKey, desc, 'keys', context) : [];

  if (sequencingPerKey && keys.length <= 0) {
    const errMsg = `Failed to resolve any keys for message (${desc}) - keyPropertyNames [${keyPropertyNames.join(', ')}]`;
    context.error(errMsg);
    throw errorWithReason(new Error(errMsg), 'Sequencing per key, but failed to resolve any keys');
  }

  // Resolve the message's sequence number(s): using seqNoPropertyNames if configured; otherwise using the kinesis
  // record's event sequence number(s) if present; otherwise use an empty array
  const hasSeqNoPropertyNames = seqNoPropertyNames.length > 0;
  const seqNos = hasSeqNoPropertyNames ?
    getPropertyValues(seqNoPropertyNames, message, record, userRecord, sequencingRequired, desc, 'seqNos', context) :
    eventIdAndSeqNos && eventIdAndSeqNos.eventSeqNo ?
      eventIdAndSeqNos.eventSubSeqNo ?
        [['eventSeqNo', eventIdAndSeqNos.eventSeqNo], ['eventSubSeqNo', eventIdAndSeqNos.eventSubSeqNo]] :
        [['eventSeqNo', eventIdAndSeqNos.eventSeqNo]] :
      [];

  if (sequencingRequired && seqNos.length <= 0) {
    const errMsg = `Failed to resolve any seqNos for message (${desc}) - seqNoPropertyNames [${seqNoPropertyNames.join(', ')}]`;
    context.error(errMsg);
    throw errorWithReason(new Error(errMsg), 'Sequencing is required, but failed to resolve any seqNos');
  }

  return {ids, keys, seqNos};
}

function md5Sum(data) {
  return crypto.createHash('md5').update(data).digest("hex");
}

/**
 * Attempts to get the values for each of the given property names from the given message & other sources.
 * @param {string[]} propertyNames
 * @param {Message} message
 * @param {Record} record
 * @param {UserRecord|undefined} [userRecord]
 * @param {boolean} throwErrorIfPropertyMissing
 * @param {string} desc
 * @param {string} forName
 * @param {StreamConsumerContext} context
 * @returns {KeyValuePair[]} an array of key-value pairs
 */
function getPropertyValues(propertyNames, message, record, userRecord, throwErrorIfPropertyMissing, desc, forName, context) {
  const missingProperties = [];
  const keyValuePairs = propertyNames.map(propertyName => {
    const value = getPropertyValue(propertyName, message, record, userRecord, missingProperties);
    return [propertyName, value];
  });

  if (missingProperties.length > 0) {
    const reason = `${missingProperties.length !== 1 ? 'Missing properties' : 'Missing property'} [${missingProperties.join(', ')}] for ${forName}`;
    const errMsg = `${reason} for message (${desc})`;
    if (throwErrorIfPropertyMissing) {
      context.error(errMsg);
      throw errorWithReason(new Error(errMsg), reason);
    }
    context.warn(errMsg);
  }
  return keyValuePairs;
}

function getPropertyValue(propertyName, message, record, userRecord, missingProperties) {
  // First look for the property on the message
  if (hasOwnPropertyWithCompoundName(message, propertyName)) {
    return getPropertyValueByCompoundName(message, propertyName);
  }

  // Next look for the property on the user record (if any)
  if (userRecord && hasOwnPropertyWithCompoundName(userRecord, propertyName)) {
    return getPropertyValueByCompoundName(userRecord, propertyName);
  }

  // Last look for the property on the record
  if (record && hasOwnPropertyWithCompoundName(record, propertyName)) {
    return getPropertyValueByCompoundName(record, propertyName);
  }

  // Add the missing property's name to the list
  missingProperties.push(propertyName);
  return undefined;
}

function errorWithReason(error, reason) {
  Object.defineProperty(error, 'reason', {value: reason, enumerable: false, writable: true, configurable: true});
  return error;
}