'use strict';
// Copied from kpl-deagg.js as at 2017-02-20 - Added promise-returning deaggregateAsync function based on deaggregate function
/*
 Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

 Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

 http://aws.amazon.com/asl/

 or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
// const ProtoBuf = require("protobufjs");
const crypto = require("crypto");
// const libPath = ".";
const common = require('aws-kinesis-agg/common');
require('aws-kinesis-agg/constants');

const tries = require('core-functions/tries');
const Success = tries.Success;
const Failure = tries.Failure;

// global AggregatedRecord object which will hold the protocol buffer model
let AggregatedRecord;
// let magic = new Buffer(kplConfig[useKplVersion].magicNumber, 'hex');

// /** synchronous deaggregation interface */
// exports.deaggregateSync = function(kinesisRecord, computeChecksums,
//   afterRecordCallback) {
//   "use strict";
//
//   const userRecords = [];
//
//   // use the async deaggregation interface, and accumulate user records into
//   // the userRecords array
//   exports.deaggregate(kinesisRecord, computeChecksums, function(err,
//     userRecord) {
//     if (err) {
//       afterRecordCallback(err);
//     } else {
//       userRecords.push(userRecord);
//     }
//   }, function(err) {
//     afterRecordCallback(err, userRecords);
//   });
// };

exports._$_ = '_$_'; //IDE workaround

/** asynchronous deaggregation interface */
exports.deaggregateAsync = function deaggregateAsync(kinesisRecord, computeChecksums) {
  "use strict";
  /* jshint -W069 */// suppress warnings about dot notation (use of
  // underscores in protobuf model)
  //
  // we receive the record data as a base64 encoded string
  const recordBuffer = new Buffer(kinesisRecord.data, 'base64');

  // first 4 bytes are the kpl assigned magic number
  // https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
  if (recordBuffer.slice(0, 4).toString('hex') === kplConfig[useKplVersion].magicNumber) {
    try {
      if (!AggregatedRecord) {
        AggregatedRecord = common.loadBuilder();
      }

      // decode the protobuf binary from byte offset 4 to length-16 (last
      // 16 are checksum)
      const protobufMessage = AggregatedRecord.decode(recordBuffer.slice(4,
        recordBuffer.length - 16));

      // extract the kinesis record checksum
      const recordChecksum = recordBuffer.slice(recordBuffer.length - 16,
        recordBuffer.length).toString('base64');

      if (computeChecksums === true) {
        // compute a checksum from the serialised protobuf message
        const md5 = crypto.createHash('md5');
        md5.update(recordBuffer.slice(4, recordBuffer.length - 16));
        const calculatedChecksum = md5.digest('base64');

        // validate that the checksum is correct for the transmitted
        // data
        if (calculatedChecksum !== recordChecksum) {
          if (debug) {
            console.log("Record Checksum: " + recordChecksum);
            console.log("Calculated Checksum: "
              + calculatedChecksum);
          }
          // throw new Error("Invalid record checksum");
          return Promise.reject(new Error("Invalid record checksum")); //TODO or Promise.resolve(new Failure(new Error("Invalid record checksum"))) ???
        }
      } else {
        if (debug) {
          console
            .log("WARN: Record Checksum Verification turned off");
        }
      }

      const n = protobufMessage.records.length;
      if (debug) {
        console.log("Found " + n
          + " KPL Encoded Messages");
      }

      // iterate over each User Record in order
      const outcomes = new Array(n);
      for (let i = 0; i < n; i++) {
        try {
          const item = protobufMessage.records[i];

          // create a user record with the extracted partition keys and sequence information
          outcomes[i] = new Success({
            partitionKey : protobufMessage["partition_key_table"][item["partition_key_index"]],
            explicitPartitionKey : protobufMessage["explicit_hash_key_table"][item["explicit_hash_key_index"]],
            sequenceNumber : kinesisRecord.sequenceNumber,
            subSequenceNumber : i,
            data : item.data.toString('base64')
          });

          // // emit the per-record callback with the extracted partition
          // // keys and sequence information
          // perRecordCallback(
          //   null,
          //   {
          //     partitionKey : protobufMessage["partition_key_table"][item["partition_key_index"]],
          //     explicitPartitionKey : protobufMessage["explicit_hash_key_table"][item["explicit_hash_key_index"]],
          //     sequenceNumber : kinesisRecord.sequenceNumber,
          //     subSequenceNumber : i,
          //     data : item.data.toString('base64')
          //   });
        } catch (e) {
          e.eventSubSeqNo = i;
          outcomes[i] = new Failure(e);

          // call the after record callback, indicating the enclosing
          // kinesis record information and the subsequence number of
          // the erroring user record

          // afterRecordCallback(
          //   e,
          //   {
          //     partitionKey : kinesisRecord.partitionKey,
          //     explicitPartitionKey : kinesisRecord.explicitPartitionKey,
          //     sequenceNumber : kinesisRecord.sequenceNumber,
          //     subSequenceNumber : i,
          //     data : kinesisRecord.data
          //   });
        }
      }

      // finished processing the kinesis record
      return Promise.resolve(outcomes);
      // afterRecordCallback();
    } catch (e) {
      return Promise.reject(e); //TODO or Promise.resolve(new Failure(e)) ???
      // afterRecordCallback(e);
    }
  } else {
    // not a KPL encoded message - no biggie - emit the record with
    // the same interface as if it was. Customers can differentiate KPL
    // user records vs plain Kinesis Records on the basis of the
    // sub-sequence number
    if (debug) {
      console
        .log("WARN: Non KPL Aggregated Message Processed for DeAggregation: "
          + kinesisRecord.partitionKey
          + "-"
          + kinesisRecord.sequenceNumber);
    }
    // create a user record with the extracted partition keys and sequence information
    const outcome = new Success({
      partitionKey : kinesisRecord.partitionKey,
      explicitPartitionKey : kinesisRecord.explicitPartitionKey,
      sequenceNumber : kinesisRecord.sequenceNumber,
      data : kinesisRecord.data
    });
    return Promise.resolve([outcome]);
    // perRecordCallback(null, {
    //   partitionKey : kinesisRecord.partitionKey,
    //   explicitPartitionKey : kinesisRecord.explicitPartitionKey,
    //   sequenceNumber : kinesisRecord.sequenceNumber,
    //   data : kinesisRecord.data
    // });
    // afterRecordCallback();
  }
};
