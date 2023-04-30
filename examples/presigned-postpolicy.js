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

 // Note: YOUR-ACCESSKEYID, YOUR-SECRETACCESSKEY, my-objectname and my-bucketname
 // are dummy values, please replace them with original values.

var Minio = require('minio')

var s3Client = new Minio.Client({
  endPoint: 's3.amazonaws.com',
  accessKey: 'YOUR-ACCESSKEYID',
  secretKey: 'YOUR-SECRETACCESSKEY',
  useSSL: true // Default is true.
})

const superagent = require('superagent')
const _ = require('lodash')


var s3Client = new Minio.Client({
  useSSL:false,
  endPoint:"localhost",
  port:22000,
  accessKey:"minio",
  secretKey:"minio123"
})

// s3Client.traceOn()
function testViaHttpClient () {

  // Construct a new postPolicy.
  var policy = s3Client.newPostPolicy()
  // Set the object name my-objectname.
  policy.setKey("infra-policy-new.txt")
  // Set the bucket to my-bucketname.
  policy.setBucket("post-policy-test")

  var expires = new Date()
  expires.setSeconds(24 * 60 * 60 * 10) // 10 days
  policy.setExpires(expires)

  policy.setContentLengthRange(1, 1024 * 1024) // Min upload length is 1KB Max upload size is 1MB

  policy.setContentTypeStartsWith('image/jpeg')
  policy.setContentDisposition('attachment; filename=äöüex ®©µÄÆÐÕæŒƕƩǅ 01000000 0x40 \u0040 amȡȹɆple&0a!-_.*\'()&$@=;:+,?<>.pdf')

  policy.setUserMetaData({
    "sph-meta": 'sph-value',
    "filename-meta": 'My* Docume nt.json'
  })


  s3Client.presignedPostPolicy(policy, (e, data) => {
    if (e) {
      return console.log(e)
    }
    var req = superagent.post(data.postURL)
    _.each(data.formData, (value, key) => req.field(key, value))
    req.attach('file', Buffer.from(Buffer.alloc(1, 0)), 'test')
    req.end(function (e) {
      if (e) {
        console.log(e)
      }
      // s3Client.removeObject(bucketName, objectName, done)
    })
    req.on('error', e => {
      console.log(e)
    })
  })

}

function generateCurl(){
  var policy = s3Client.newPostPolicy()
  // Set the object name my-objectname.
  policy.setKey("Cust_Mobile_View.png")
  // Set the bucket to my-bucketname.
  policy.setBucket("post-policy-test")

  var expires = new Date
  expires.setSeconds(24 * 60 * 60 * 10) // 10 days
  policy.setExpires(expires)

  policy.setContentLengthRange(1, 1024*1024) // Min upload length is 1KB Max upload size is 1MB

  policy.setUserMetaData({
    "sph-meta": 'sph-value',
    "filename-meta": 'My* Docume nt.json'
  })

  policy.setContentType('image/png')

  // this seems to cause trouble in curl.
  // while the following sends the metadata. it is stored in unexpected format in server.
  // policy.setContentDisposition(`attachment; filename="Cust_Mobile_View.png"`)


  s3Client.presignedPostPolicy(policy, function(e, data) {
    if (e) return console.log(e)
    var curl = []
    curl.push(`curl ${data.postURL} -X POST `)

    for (var key in data.formData) {
      if (data.formData.hasOwnProperty(key)) {
        var value = data.formData[key]
        curl.push(`-F ${key}='${value}'`)
      }
    }
    // Print curl command to upload files.
    // curl.push('-F file=@"/Pictures/Cust_Mobile_View.png"')
    curl.push('-F file=@"<FILE_PATH>"')
    console.log(curl.join(' '))
  })

}

testViaHttpClient()

//generateCurl()