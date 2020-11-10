/*********************************************************************************************************************
 *  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           *
 *                                                                                                                    *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance    *
 *  with the License. A copy of the License is located at                                                             *
 *                                                                                                                    *
 *      http://www.apache.org/licenses/LICENSE-2.0                                                                    *
 *                                                                                                                    *
 *  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES *
 *  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    *
 *  and limitations under the License.                                                                                *
 *********************************************************************************************************************/

const moment = require('moment');
const AWS = require('aws-sdk');
if (process.env.TESTING === 'true') {
  AWS.config.update({
    region: 'ap-southeast-2',
    credentials: new AWS.SharedIniFileCredentials({profile: 'sandpit'})
  }); 
}
const error = require('./lib/error');
const lambda = new AWS.Lambda();

exports.handler = async (event) => {
  console.log(`REQUEST:: ${JSON.stringify(event, null, 2)}`);

  const s3 = new AWS.S3();
  let data;

  try {
    // Default configuration for the workflow is built using the enviroment variables.
    // Any parameter in config can be overwriten using a metadata file.
    data = {
      guid: event.guid,
      startTime: moment().utc().toISOString(),
      workflowTrigger: event.workflowTrigger,
      workflowStatus: 'Ingest',
      workflowName: process.env.WorkflowName,
      srcBucket: process.env.Source,
      destBucket: process.env.Destination,
      cloudFront: process.env.CloudFront,
      frameCapture: JSON.parse(process.env.FrameCapture),
      archiveSource: process.env.ArchiveSource,
      jobTemplate_2160p: process.env.MediaConvert_Template_2160p,
      jobTemplate_1080p: process.env.MediaConvert_Template_1080p,
      jobTemplate_720p: process.env.MediaConvert_Template_720p,
      inputRotate: process.env.InputRotate,
      acceleratedTranscoding: process.env.AcceleratedTranscoding,
      enableSns: JSON.parse(process.env.EnableSns),
      enableSqs: JSON.parse(process.env.EnableSqs)
    };

    switch (event.workflowTrigger) {
      case 'Metadata':
        console.log('Validating Metadata file::');

        const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
        data.srcMetadataFile = key;

        // Download json metadata file from s3
        const metadata = await s3.getObject({Bucket: data.srcBucket, Key: key}).promise();

        const metadataFile = JSON.parse(metadata.Body);
        if (!metadataFile.srcVideo) {
          throw new Error('srcVideo is not defined in metadata::', metadataFile);
        }

        // https://github.com/awslabs/video-on-demand-on-aws/pull/23
        // Normalize key in order to support different casing
        Object.keys(metadataFile).forEach((key) => {
          const normalizedKey = key.charAt(0).toLowerCase() + key.substr(1);
          data[normalizedKey] = metadataFile[key];
        });

        // Check source file is accessible in s3
        await s3.headObject({Bucket: data.srcBucket, Key: data.srcVideo}).promise();
        console.log(data.srcVideo);
        data.thumbnailFrameOffset = await getThumbnailOffset(data.srcVideo);
        
        break;

      case 'Video':
        data.srcVideo = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
        data.thumbnailFrameOffset = await getThumbnailOffset(data.srcVideo);
        break;

      default:
        throw new Error('event.workflowTrigger is not defined.');
    }

    // The MediaPackage setting is configured at the stack level, and it cannot be updated via metadata.
    data['enableMediaPackage'] = JSON.parse(process.env.EnableMediaPackage);
  } catch (err) {
    await error.handler(event, err);
    throw err;
  }

  return data;
};

/**
 * Find MediaResource by srcVideo name and return the thumbnailFrameOffset value if not null
 * Attempts to find an existing MediaResource up to 3 times (with a 10 second pause between each try)
 * @param fileName
 */
const getThumbnailOffset = async (fileName) => {
  const params = {
    FunctionName: process.env.MediaResourceFunction, // grafa-media-stack-GrafaMediaFunction-9QZFFTICT47C
    InvocationType: 'RequestResponse',
    LogType: 'Tail',
    Payload: JSON.stringify({
      httpMethod: 'GET',
      queryStringParameters: {
        videoFileName: fileName
      }
    })
  };
  let returnVal = 0;
  let tries = 1;
  do {
    const response = await lambda.invoke(params).promise().catch(err => {
      console.log('Could not fetch %s::%o', process.env.MediaResourceFunction, err);
      return null;
    });
    if (response) {
      const payload = JSON.parse(response.Payload);
      console.log('Status code: %d', payload.statusCode);
      if (payload.statusCode === 200) {
        const body = JSON.parse(payload.body);
        const offset = parseInt(body.ThumbNailFrameOffset);
        if (offset >= 0) {
          returnVal = offset;
        }
        break;
      } else {
        // Wait 10 seconds before trying again
        await sleep(10000);
        tries++;
      }
    }
  } while (tries <= 3);
  return returnVal;
};

/**
 * Sleep function to pause execution of a function
 * @param delay
 * @returns {Promise<unknown>}
 */
const sleep = (delay) => new Promise((resolve) => setTimeout(resolve, delay));