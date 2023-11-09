/*
 * MinIO Javascript Library for Amazon S3 Compatible Cloud Storage, (C) 2015 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Note: YOUR-ACCESSKEYID, YOUR-SECRETACCESSKEY and my-bucketname are
// dummy values, please replace them with original values.

import * as Minio from 'minio'

const s3Client = new Minio.Client({
  endPoint: 's3.amazonaws.com',
  accessKey: 'YOUR-ACCESSKEYID',
  secretKey: 'YOUR-SECRETACCESSKEY',
})
// Create a bucket name my-bucketname.
s3Client.makeBucket('my-bucketname', 'us-west-1', function (e) {
  if (e) {
    return console.log(e)
  }
  console.log('Success')
})

// Create a bucket with object locking enabled.
s3Client.makeBucket('mybucket', 'us-east-1', { ObjectLocking: true }, function (err) {
  if (err) {
    return console.log('Error creating bucket with lock .', err)
  }
  console.log('Bucket created successfully in "us-east-1" and enabled object lock')
})
