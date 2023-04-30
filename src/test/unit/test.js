/*
 * MinIO Javascript Library for Amazon S3 Compatible Cloud Storage, (C) 2016 MinIO, Inc.
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

require('source-map-support').install()

import { assert } from 'chai'
import Nock from 'nock'
import Stream from 'stream'
import * as Minio from '../../../dist/main/minio'
import { isValidEndpoint,
  isValidIP, makeDateLong,
  makeDateShort,partsRequired,
  CopySourceOptions,
  CopyDestinationOptions,
  isArray,
  calculateEvenSplits,
  parseXml,
} from '../../../dist/main/helpers'

var Package = require('../../../package.json')

const { XMLParser } = require("fast-xml-parser")
const  fxp = new XMLParser()

describe('Helpers', () => {
  it('should validate for s3 endpoint', () => {
    assert.equal(isValidEndpoint('s3.amazonaws.com'), true)
  })
  it('should validate for s3 china', () => {
    assert.equal(isValidEndpoint('s3.cn-north-1.amazonaws.com.cn'), true)
  })
  it('should validate for us-west-2', () => {
    assert.equal(isValidEndpoint('s3-us-west-2.amazonaws.com'), true)
  })
  it('should fail for invalid endpoint characters', () => {
    assert.equal(isValidEndpoint('111.#2.11'), false)
  })

  it('should make date short', () => {
    let date = new Date('2012-12-03T17:25:36.331Z')

    assert.equal(makeDateShort(date), '20121203')
  })
  it('should make date long', () => {
    let date = new Date('2017-08-11T17:26:34.935Z')

    assert.equal(makeDateLong(date), '20170811T172634Z')
  })

  it('should object key equal 0xf2', () => {
    let xml = '<Key xmlns="http://s3.amazonaws.com/doc/2006-03-01/">0xf2</Key>'
    let xmlobj = parseXml(xml)
    let originxmlobj = fxp.parse(xml)
    assert.equal(xmlobj.Key, '0xf2')
    assert.equal(originxmlobj.Key, '242')
  })

  // Adopted from minio-go sdk
  const oneGB =1024 * 1024 * 1024
  const fiveGB = (5*oneGB)

  const OBJ_SIZES = {
    gb1 : oneGB,
    gb5 : fiveGB,
    gb5p1 : fiveGB + 1,
    gb10p1 : 2 * fiveGB + 1,
    gb10p2 : 2 * fiveGB + 2
  }

  const maxMultipartPutObjectSize = 1024 * 1024 * 1024 * 1024 * 5

  it('Parts Required Test cases ', () =>{

    const expectedPartsRequiredTestCases = [
      {value:0, expected:0},
      {value:1, expected:1},
      {value:fiveGB, expected:10},
      {value:OBJ_SIZES.gb5p1, expected:10},
      {value:2 * fiveGB, expected:20},
      {value:OBJ_SIZES.gb10p1, expected:20},
      {value:OBJ_SIZES.gb10p2, expected:20},
      {value:OBJ_SIZES.gb10p1 + OBJ_SIZES.gb10p2, expected:40},
      {value:maxMultipartPutObjectSize, expected:10000}
    ]

    expectedPartsRequiredTestCases.forEach((testCase)=>{
      const fnResult = partsRequired(testCase.value)
      assert.equal(fnResult,testCase.expected)
    })
  })
  it("Even split of Sizes Test cases ", ()=>{
    // Adopted from minio-go sdk
    const expectedSplitsTestCases =[
      {size:0, sourceConfig:new CopySourceOptions({Start: -1}), expectedStart:null, expectedEnd:null}
      ,{size:1, sourceConfig:new CopySourceOptions({Start: -1}), expectedStart:[undefined], expectedEnd:[NaN]}
      ,{size:1,sourceConfig:new  CopySourceOptions({Start: 0}), expectedStart:[0], expectedEnd:[0]}
      ,{size:OBJ_SIZES.gb1, sourceConfig:new CopySourceOptions({Start: -1}), expectedStart:[0, 536870912],expectedEnd:[536870911, 1073741823]}
      ,{size:OBJ_SIZES.gb5, sourceConfig:new CopySourceOptions({Start: -1}),
        expectedStart:[0, 536870912, 1073741824, 1610612736, 2147483648, 2684354560,
                       3221225472, 3758096384, 4294967296, 4831838208],
        expectedEnd:[536870911, 1073741823, 1610612735, 2147483647, 2684354559, 3221225471,
                     3758096383, 4294967295, 4831838207, 5368709119]
      },

      // 2 part splits
      {size:OBJ_SIZES.gb5p1,  sourceConfig:new CopySourceOptions({Start: -1}),
       expectedStart:[0, 536870913, 1073741825, 1610612737, 2147483649, 2684354561,
                      3221225473, 3758096385, 4294967297, 4831838209],
       expectedEnd:[536870912, 1073741824, 1610612736, 2147483648, 2684354560, 3221225472,
                    3758096384, 4294967296, 4831838208, 5368709120],
      },
      {size:OBJ_SIZES.gb5p1,  sourceConfig:new CopySourceOptions({Start: -1}),
       expectedStart:[0, 536870913, 1073741825, 1610612737, 2147483649, 2684354561,
                      3221225473, 3758096385, 4294967297, 4831838209],
       expectedEnd:[536870912, 1073741824, 1610612736, 2147483648, 2684354560, 3221225472,
                    3758096384, 4294967296, 4831838208, 5368709120],
      },

      // 3 part splits
      {size:OBJ_SIZES.gb10p1,sourceConfig:new CopySourceOptions({Start: -1}),
       expectedStart:[0, 536870913, 1073741825, 1610612737, 2147483649, 2684354561,
                      3221225473, 3758096385, 4294967297, 4831838209, 5368709121,
                      5905580033, 6442450945, 6979321857, 7516192769, 8053063681,
                      8589934593, 9126805505, 9663676417, 10200547329],
       expectedEnd:[536870912, 1073741824, 1610612736, 2147483648, 2684354560,
                    3221225472, 3758096384, 4294967296, 4831838208, 5368709120,
                    5905580032, 6442450944, 6979321856, 7516192768, 8053063680,
                    8589934592, 9126805504, 9663676416, 10200547328, 10737418240],
      },
      {size:OBJ_SIZES.gb10p2,sourceConfig:new CopySourceOptions({Start: -1}),
       expectedStart:[0, 536870913, 1073741826, 1610612738, 2147483650, 2684354562,
                      3221225474, 3758096386, 4294967298, 4831838210, 5368709122,
                      5905580034, 6442450946, 6979321858, 7516192770, 8053063682,
                      8589934594, 9126805506, 9663676418, 10200547330],
       expectedEnd:[536870912, 1073741825, 1610612737, 2147483649, 2684354561,
                    3221225473, 3758096385, 4294967297, 4831838209, 5368709121,
                    5905580033, 6442450945, 6979321857, 7516192769, 8053063681,
                    8589934593, 9126805505, 9663676417, 10200547329, 10737418241]
      }
    ]


    expectedSplitsTestCases.forEach((testCase)=>{
      const fnResult = calculateEvenSplits(testCase.size, testCase)
      const {
        startIndex,
        endIndex
      } = fnResult ||{}

      if(isArray(startIndex) && isArray(endIndex)) {
        const isExpectedResult = (startIndex.length === testCase.expectedStart.length) && (endIndex.length === testCase.expectedEnd.length)
        assert.equal(isExpectedResult, true)
      } else{ // null cases.
        assert.equal(startIndex, expectedSplitsTestCases.expectedStart)
        assert.equal(endIndex, expectedSplitsTestCases.expectedEnd)
      }
    })

  })

})

describe('CopyConditions', () => {
  let date = 'Fri, 11 Aug 2017 19:34:18 GMT'

  let cc = new Minio.CopyConditions()

  describe('#setModified', () => {
    it('should take a date argument', () => {
      cc.setModified(new Date(date))

      assert.equal(cc.modified, date)
    })

    it('should throw without date', () => {
      assert.throws(() => {
        cc.setModified()
      }, /date must be of type Date/)

      assert.throws(() => {
        cc.setModified({ hi: 'there' })
      }, /date must be of type Date/)
    })
  })

  describe('#setUnmodified', () => {
    it('should take a date argument', () => {
      cc.setUnmodified(new Date(date))

      assert.equal(cc.unmodified, date)
    })

    it('should throw without date', () => {
      assert.throws(() => {
        cc.setUnmodified()
      }, /date must be of type Date/)

      assert.throws(() => {
        cc.setUnmodified({ hi: 'there' })
      }, /date must be of type Date/)
    })
  })
})

describe('Client', function() {
  var nockRequests = []
  this.timeout(5000)
  beforeEach(() => {
    Nock.cleanAll()
    nockRequests = []
  })
  afterEach(() => {
    nockRequests.forEach(element => {
      if (!element.request.isDone()) {
        element.request.done()
      }
    })
  })
  var client = new Minio.Client({
    endPoint: 'localhost',
    port: 9000,
    accessKey: 'accesskey',
    secretKey: 'secretkey',
    useSSL: false
  })
  describe('new client', () => {
    it('should work with https', () => {
      var client = new Minio.Client({
        endPoint: 'localhost',
        accessKey: 'accesskey',
        secretKey: 'secretkey'
      })
      assert.equal(client.port, 443)
    })
    it('should override port with http', () => {
      var client = new Minio.Client({
        endPoint: 'localhost',
        port: 9000,
        accessKey: 'accesskey',
        secretKey: 'secretkey',
        useSSL: false
      })
      assert.equal(client.port, 9000)
    })
    it('should work with http', () => {
      var client = new Minio.Client({
        endPoint: 'localhost',
        accessKey: 'accesskey',
        secretKey: 'secretkey',
        useSSL: false
      })
      assert.equal(client.port, 80)
    })
    it('should override port with https', () => {
      var client = new Minio.Client({
        endPoint: 'localhost',
        port: 9000,
        accessKey: 'accesskey',
        secretKey: 'secretkey'
      })
      assert.equal(client.port, 9000)
    })
    it('should fail with url', (done) => {
      try {
        new Minio.Client({
          endPoint: 'http://localhost:9000',
          accessKey: 'accesskey',
          secretKey: 'secretkey'
        })
      } catch (e) {
        done()
      }
    })
    it('should fail with alphanumeric', (done) => {
      try {
        new Minio.Client({
          endPoint: 'localhost##$@3',
          accessKey: 'accesskey',
          secretKey: 'secretkey'
        })
      } catch (e) {
        done()
      }
    })
    it('should fail with no url', (done) => {
      try {
        new Minio.Client({
          accessKey: 'accesskey',
          secretKey: 'secretkey'
        })
      } catch (e) {
        done()
      }
    })
    it('should fail with bad port', (done) => {
      try {
        new Minio.Client({
          endPoint: 'localhost',
          port: -1,
          accessKey: 'accesskey',
          secretKey: 'secretkey'
        })
      } catch (e) {
        done()
      }
    })
    it('should fail when secure param is passed', (done) => {
      try {
        new Minio.Client({
          endPoint: 'localhost',
          secure: false,
          port: 9000,
          accessKey: 'accesskey',
          secretKey: 'secretkey'
        })
      } catch (e) {
        done()
      }
    })
    it('should fail when secure param is passed', (done) => {
      try {
        new Minio.Client({
          endPoint: 'localhost',
          secure: true,
          port: 9000,
          accessKey: 'accesskey',
          secretKey: 'secretkey'
        })
      } catch (e) {
        done()
      }
    })
  })
  describe('Presigned URL', () => {
    describe('presigned-get', () => {
      it('should not generate presigned url with no access key', (done) => {
        try {
          var client = new Minio.Client({
            endPoint: 'localhost',
            port: 9000,
            useSSL: false
          })
          client.presignedGetObject('bucket', 'object', 1000, function() {})
        } catch (e) {
          done()
        }
      })
      it('should not generate presigned url with wrong expires param', (done) => {
        try {
          client.presignedGetObject('bucket', 'object', '0', function() {})
        } catch (e) {
          done()
        }
      })
    })
    describe('presigned-put', () => {
      it('should not generate presigned url with no access key', (done) => {
        try {
          var client = new Minio.Client({
            endPoint: 'localhost',
            port: 9000,
            useSSL: false
          })
          client.presignedPutObject('bucket', 'object', 1000, function() {})
        } catch (e) {
          done()
        }
      })
      it('should not generate presigned url with wrong expires param', (done) => {
        try {
          client.presignedPutObject('bucket', 'object', '0', function() {})
        } catch (e) {
          done()
        }
      })
    })
    describe('presigned-post-policy', () => {
      it('should not generate content type for undefined value', () => {
        assert.throws(() => {
          var policy = client.newPostPolicy()
          policy.setContentType()
        }, /content-type cannot be null/)
      })
      it('should not generate content disposition for undefined value', () => {
        assert.throws(() => {
          var policy = client.newPostPolicy()
          policy.setContentDisposition()
        }, /content-disposition cannot be null/)
      })
      it('should not generate user defined metadata for string value', () => {
        assert.throws(() => {
          var policy = client.newPostPolicy()
          policy.setUserMetaData('123')
        }, /metadata should be of type "object"/)
      })
      it('should not generate user defined metadata for null value', () => {
        assert.throws(() => {
          var policy = client.newPostPolicy()
          policy.setUserMetaData(null)
        }, /metadata should be of type "object"/)
      })
      it('should not generate user defined metadata for undefined value', () => {
        assert.throws(() => {
          var policy = client.newPostPolicy()
          policy.setUserMetaData()
        }, /metadata should be of type "object"/)
      })
    })
  })
  describe('User Agent', () => {
    it('should have a default user agent', () => {
      var client = new Minio.Client({
        endPoint: 'localhost',
        accessKey: 'accesskey',
        secretKey: 'secretkey'
      })
      assert.equal(`MinIO (${process.platform}; ${process.arch}) minio-js/${Package.version}`,
                   client.userAgent)
    })
    it('should set user agent', () => {
      var client = new Minio.Client({
        endPoint: 'localhost',
        accessKey: 'accesskey',
        secretKey: 'secretkey'
      })
      client.setAppInfo('test', '3.2.1')
      assert.equal(`MinIO (${process.platform}; ${process.arch}) minio-js/${Package.version} test/3.2.1`,
                   client.userAgent)
    })
    it('should set user agent without comments', () => {
      var client = new Minio.Client({
        endPoint: 'localhost',
        accessKey: 'accesskey',
        secretKey: 'secretkey'
      })
      client.setAppInfo('test', '3.2.1')
      assert.equal(`MinIO (${process.platform}; ${process.arch}) minio-js/${Package.version} test/3.2.1`,
                   client.userAgent)
    })
    it('should not set user agent without name', (done) => {
      try {
        var client = new Minio.Client({
          endPoint: 'localhost',
          accessKey: 'accesskey',
          secretKey: 'secretkey'
        })
        client.setAppInfo(null, '3.2.1')
      } catch (e) {
        done()
      }
    })
    it('should not set user agent with empty name', (done) => {
      try {
        var client = new Minio.Client({
          endPoint: 'localhost',
          accessKey: 'accesskey',
          secretKey: 'secretkey'
        })
        client.setAppInfo('', '3.2.1')
      } catch (e) {
        done()
      }
    })
    it('should not set user agent without version', (done) => {
      try {
        var client = new Minio.Client({
          endPoint: 'localhost',
          accessKey: 'accesskey',
          secretKey: 'secretkey'
        })
        client.setAppInfo('test', null)
      } catch (e) {
        done()
      }
    })
    it('should not set user agent with empty version', (done) => {
      try {
        var client = new Minio.Client({
          endPoint: 'localhost',
          accessKey: 'accesskey',
          secretKey: 'secretkey'
        })
        client.setAppInfo('test', '')
      } catch (e) {
        done()
      }
    })
  })

  describe('object level', () => {
    describe('#getObject(bucket, object, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.getObject(null, 'hello', function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.getObject('', 'hello', function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.getObject('  \n  \t  ', 'hello', function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on null object', (done) => {
        try {
          client.getObject('hello', null, function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on empty object', (done) => {
        try {
          client.getObject('hello', '', function() {})
        } catch (e) {
          done()
        }
      })
    })

    describe('#putObject(bucket, object, source, size, contentType, callback)', () => {
      describe('with small objects using single put', () => {
        it('should fail when data is smaller than specified', (done) => {
          var s = new Stream.Readable()
          s._read = function() {}
          s.push('hello world')
          s.push(null)
          client.putObject('bucket', 'object', s, 12, '', (e) => {
            if (e) {
              done()
            }
          })
        })
        it('should fail when data is larger than specified', (done) => {
          var s = new Stream.Readable()
          s._read = function() {}
          s.push('hello world')
          s.push(null)
          client.putObject('bucket', 'object', s, 10, '', (e) => {
            if (e) {
              done()
            }
          })
        })
        it('should fail with invalid bucket name', () => {
          assert.throws(() => {
            client.putObject('ab', 'object', () => {})
          }, /Invalid bucket name/)
        })
        it('should fail with invalid object name', () => {
          assert.throws(() => {
            client.putObject('bucket', '', () => {})
          }, /Invalid object name/)
        })
        it('should error with size > maxObjectSize', () => {
          assert.throws(() => {
            client.putObject('bucket', 'object', new Stream.Readable(), client.maxObjectSize + 1, () => {})
          }, /size should not be more than/)
        })
        it('should fail on null bucket', (done) => {
          try {
            client.putObject(null, 'hello', null, 1, '', function() {})
          } catch (e) {
            done()
          }
        })
        it('should fail on empty bucket', (done) => {
          try {
            client.putObject(' \n \t ', 'hello', null, 1, '', function() {})
          } catch (e) {
            done()
          }
        })
        it('should fail on empty bucket', (done) => {
          try {
            client.putObject('', 'hello', null, 1, '', function() {})
          } catch (e) {
            done()
          }
        })
        it('should fail on null object', (done) => {
          try {
            client.putObject('hello', null, null, 1, '', function() {})
          } catch (e) {
            done()
          }
        })
        it('should fail on empty object', (done) => {
          try {
            client.putObject('hello', '', null, 1, '', function() {})
          } catch (e) {
            done()
          }
        })
      })
    })

    describe('#removeAllBucketNotification()', () => {
      it('should error on invalid arguments', () => {
        assert.throws(() => {
          client.removeAllBucketNotification('ab', () => {}, function() {})
        }, /Invalid bucket name/)
      })
    })

    describe('#setBucketNotification()', () => {
      it('should error on invalid arguments', () => {
        assert.throws(() => {
          client.setBucketNotification('ab', () => {})
        }, /Invalid bucket name/)
        assert.throws(() => {
          client.setBucketNotification('bucket', 49, () => {})
        }, /notification config should be of type "Object"/)
      })
    })

    describe('#getBucketNotification()', () => {
      it('should error on invalid arguments', () => {
        assert.throws(() => {
          client.getBucketNotification('ab', () => {})
        }, /Invalid bucket name/)
      })
    })

    describe('#listenBucketNotification', () => {
      it('should error on invalid arguments', () => {
        assert.throws(() => {
          client.listenBucketNotification('ab', 'prefix', 'suffix', ['events'])
        }, /Invalid bucket name/)
        assert.throws(() => {
          client.listenBucketNotification('bucket', {}, 'suffix', ['events'])
        }, /prefix must be of type string/)
        assert.throws(() => {
          client.listenBucketNotification('bucket', '', {}, ['events'])
        }, /suffix must be of type string/)
        assert.throws(() => {
          client.listenBucketNotification('bucket', '', '', {})
        }, /events must be of type Array/)
      })
    })

    describe('#statObject(bucket, object, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.statObject(null, 'hello', function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.statObject('', 'hello', function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.statObject('  \n  \t  ', 'hello', function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on null object', (done) => {
        try {
          client.statObject('hello', null, function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on empty object', (done) => {
        try {
          client.statObject('hello', '', function() {})
        } catch (e) {
          done()
        }
      })

      it('should fail on incompatible argument type (number) for statOpts object', (done) => {
        try {
          client.statObject('hello', 'testStatOpts', 1, function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on incompatible argument type (null) for statOpts object', (done) => {
        try {
          client.statObject('hello', 'testStatOpts', null, function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on incompatible argument type (sting) for statOpts object', (done) => {
        try {
          client.statObject('hello', 'testStatOpts', '  ', function() {})
        } catch (e) {
          done()
        }
      })

    })

    describe('#removeObject(bucket, object, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.removeObject(null, 'hello', function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.removeObject('', 'hello', function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.removeObject('  \n  \t  ', 'hello', function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on null object', (done) => {
        try {
          client.removeObject('hello', null, function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on empty object', (done) => {
        try {
          client.removeObject('hello', '', function() {})
        } catch (e) {
          done()
        }
      })
      // Versioning related options as removeOpts
      it('should fail on empty (null) removeOpts object', (done) => {
        try {
          client.removeObject('hello', 'testRemoveOpts',null, function() {})
        } catch (e) {
          done()
        }
      })

      it('should fail on empty (string) removeOpts', (done) => {
        try {
          client.removeObject('hello', 'testRemoveOpts','', function() {})
        } catch (e) {
          done()
        }
      })
    })

    describe('#removeIncompleteUpload(bucket, object, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.removeIncompleteUpload(null, 'hello', function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.removeIncompleteUpload('', 'hello', function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.removeIncompleteUpload('  \n  \t  ', 'hello', function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on null object', (done) => {
        try {
          client.removeIncompleteUpload('hello', null, function() {})
        } catch (e) {
          done()
        }
      })
      it('should fail on empty object', (done) => {
        try {
          client.removeIncompleteUpload('hello', '', function() {})
        } catch (e) {
          done()
        }
      })
    })
  })

  describe('Bucket Versioning APIs', ()=>{
    describe('getBucketVersioning(bucket, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.getBucketVersioning(null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.getBucketVersioning('', function () {
          })
        } catch (e) {
          done()
        }
      })
    })

    describe('setBucketVersioning(bucket, versionConfig, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.setBucketVersioning(null, {},function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.setBucketVersioning('', {},function () {
          })
        } catch (e) {
          done()
        }
      })

      it('should fail on empty versionConfig', (done) => {
        try {
          client.setBucketVersioning('', null,function () {
          })
        } catch (e) {
          done()
        }
      })
    })
  })


  describe('Bucket and Object Tags APIs', () => {
    describe('Set Bucket Tags ', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.setBucketTagging(null, {}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.setBucketTagging('', {}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail if tags are more than 50', (done) => {
        const _50_plus_key_tags={}
        for(let i=0;i<51;i+=1){
          _50_plus_key_tags[i]=i
        }
        try {
          client.setBucketTagging('', _50_plus_key_tags, function () {
          })
        } catch (e) {
          done()
        }
      })

    })
    describe('Get Bucket Tags', () => {
      it('should fail on invalid bucket', (done) => {
        try {
          client.getBucketTagging('nv',null,  function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on null bucket', (done) => {
        try {
          client.getBucketTagging(null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.getBucketTagging('', function () {
          })
        } catch (e) {
          done()
        }
      })


    })
    describe('Remove Bucket Tags', () => {
      it('should fail on null object', (done) => {
        try {
          client.removeBucketTagging(null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.removeBucketTagging('',  function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on invalid bucket name', (done) => {
        try {
          client.removeBucketTagging('198.51.100.24', function () {
          })
        } catch (e) {
          done()
        }
      })

      it('should fail on invalid bucket name', (done) => {
        try {
          client.removeBucketTagging('xy', function () {
          })
        } catch (e) {
          done()
        }
      })
    })
    describe('Put Object Tags', () => {
      it('should fail on null object', (done) => {
        try {
          client.putObjectTagging('my-bucket-name',null, {}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty object', (done) => {
        try {
          client.putObjectTagging('my-bucket-name',null, {}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on non object tags', (done) => {
        try {
          client.putObjectTagging('my-bucket-name',null, 'non-obj-tag', function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail if tags are more than 50 on an object', (done) => {
        const _50_plus_key_tags={}
        for(let i=0;i<51;i+=1){
          _50_plus_key_tags[i]=i
        }
        try {
          client.putObjectTagging('my-bucket-name',null, _50_plus_key_tags, function () {
          })
        } catch (e) {
          done()
        }
      })

    })
    describe('Get Object Tags', () => {
      it('should fail on invalid bucket', (done) => {
        try {
          client.getObjectTagging('nv',null,  function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on null object', (done) => {
        try {
          client.getObjectTagging('my-bucket-name',null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty object', (done) => {
        try {
          client.getObjectTagging('my-bucket-name',null, function () {
          })
        } catch (e) {
          done()
        }
      })


    })
    describe('Remove Object Tags', () => {
      it('should fail on null object', (done) => {
        try {
          client.removeObjectTagging('my-bucket',null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.removeObjectTagging('my-bucket', '',  function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on invalid bucket name', (done) => {
        try {
          client.removeObjectTagging('198.51.100.24', function () {
          })
        } catch (e) {
          done()
        }
      })

      it('should fail on invalid bucket name', (done) => {
        try {
          client.removeObjectTagging('xy', function () {
          })
        } catch (e) {
          done()
        }
      })
    })
  })


  describe('setBucketLifecycle(bucket, lifecycleConfig, callback)', () => {
    it('should fail on null bucket', (done) => {
      try {
        client.setBucketLifecycle(null, null,function () {
        })
      } catch (e) {
        done()
      }
    })

    it('should fail on empty bucket', (done) => {
      try {
        client.setBucketLifecycle('', null,function () {
        })
      } catch (e) {
        done()
      }
    })

  })

  describe('getBucketLifecycle(bucket, callback)', () => {
    it('should fail on null bucket', (done) => {
      try {
        client.getBucketLifecycle(null, function () {
        })
      } catch (e) {
        done()
      }
    })

    it('should fail on empty bucket', (done) => {
      try {
        client.getBucketLifecycle('', function () {
        })
      } catch (e) {
        done()
      }
    })

  })
  describe('removeBucketLifecycle(bucket, callback)', () => {
    it('should fail on null bucket', (done) => {
      try {
        client.removeBucketLifecycle(null, null,function () {
        })
      } catch (e) {
        done()
      }
    })

    it('should fail on empty bucket', (done) => {
      try {
        client.removeBucketLifecycle('', null,function () {
        })
      } catch (e) {
        done()
      }
    })

  })

  describe('Object Locking APIs', ()=> {
    describe('getObjectLockConfig(bucket, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.getObjectLockConfig(null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.getObjectLockConfig('', function () {
          })
        } catch (e) {
          done()
        }
      })
    })

    describe('setObjectLockConfig(bucket, lockConfig, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.setObjectLockConfig(null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.setObjectLockConfig('', function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on passing invalid mode ', (done) => {
        try {
          client.setObjectLockConfig('my-bucket',{mode:"invalid_mode"}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on passing invalid unit ', (done) => {
        try {
          client.setObjectLockConfig('my-bucket',{ mode:"COMPLIANCE",unit:"invalid_unit"}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on passing invalid validity ', (done) => {
        try {
          client.setObjectLockConfig('my-bucket',{mode:"COMPLIANCE",unit:"invalid_unit", validity:''}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on passing  invalid config ', (done) => {
        try {
          client.setObjectLockConfig('my-bucket',{mode:"COMPLIANCE", randomProp:true, nonExisting:false,}, function () {
          })
        } catch (e) {
          done()
        }
      })
    })
  })

  describe('Object retention APIs', ()=> {
    describe('getObjectRetention(bucket, objectName, getRetentionOpts,callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.getObjectRetention(null,'','', function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.getObjectRetention('', '','',function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on invalid  object name', (done) => {
        try {
          client.getObjectRetention('my-bucket', null, '',function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on invalid  versionId', (done) => {
        try {
          client.getObjectRetention('my-bucket', 'objectname', {versionId:123},function () {
          })
        } catch (e) {
          done()
        }
      })
    })

    describe('putObjectRetention(bucket, objectName, retentionConfig, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.putObjectRetention(null,'',{}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.putObjectRetention('','',{}, function () {
          })
        } catch (e) {
          done()
        }
      })

      it('should fail on null object', (done) => {
        try {
          client.putObjectRetention('my-bucket',null,{}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty object', (done) => {
        try {
          client.putObjectRetention('my-bucket', '', {}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on passing invalid mode ', (done) => {
        try {
          client.putObjectRetention('my-bucket', 'my-object', {mode:"invalid_mode"}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on passing invalid governanceBypass ', (done) => {
        try {
          client.putObjectRetention('my-bucket', 'my-object', { governanceBypass:"nonbool"}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on passing invalid (null) retainUntilDate ', (done) => {
        try {
          client.putObjectRetention('my-bucket', 'my-object', { retainUntilDate:12345}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on passing invalid versionId ', (done) => {
        try {
          client.putObjectRetention('my-bucket',{ versionId:"COMPLIANCE" }, function () {
          })
        } catch (e) {
          done()
        }
      })
    })
  })


  describe('Bucket Encryption APIs', ()=> {

    describe('setBucketEncryption(bucket, encryptionConfig, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.setBucketEncryption(null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.setBucketEncryption('', function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on multiple rules', (done) => {
        try {
          client.setBucketEncryption('my-bucket', {
            // Default Rule
            Rule:[
              {
                ApplyServerSideEncryptionByDefault: {
                  SSEAlgorithm:"AES256"
                }
              },
              {
                ApplyServerSideEncryptionByDefault: {
                  SSEAlgorithm:"AES256"
                }
              }
            ]

          },function () {
          })
        } catch (e) {
          done()
        }
      })

    })

    describe('getBucketEncryption(bucket, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.getBucketEncryption(null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.getBucketEncryption('', function () {
          })
        } catch (e) {
          done()
        }
      })
    })

    describe('removeBucketEncryption(bucket, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.removeBucketEncryption(null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.removeBucketEncryption('', function () {
          })
        } catch (e) {
          done()
        }
      })
    })

  })
  describe('Bucket Replication APIs', ()=> {
    describe('setBucketReplication(bucketName, replicationConfig, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.setBucketReplication(null, {}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.setBucketReplication('', {}, function () {
          })
        } catch (e) {
          done()
        }
      })

      it('should fail on empty replicationConfig', (done) => {
        try {
          client.setBucketReplication('my-bucket', {}, function () {
          })
        } catch (e) {
          done()
        }
      })

      it('should fail on empty replicationConfig role', (done) => {
        try {
          client.setBucketReplication('my-bucket', {role:''}, function () {
          })
        } catch (e) {
          done()
        }
      })

      it('should fail on  invalid value for replicationConfig role', (done) => {
        try {
          client.setBucketReplication('my-bucket', {role:12}, function () {
          })
        } catch (e) {
          done()
        }
      })

      it('should fail on  empty value for replicationConfig rules', (done) => {
        try {
          client.setBucketReplication('my-bucket', {role:"arn:",rules:[]}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on  null value for replicationConfig rules', (done) => {
        try {
          client.setBucketReplication('my-bucket', {role:"arn:",rules:null}, function () {
          })
        } catch (e) {
          done()
        }
      })

    })

    describe('getBucketReplication(bucketName, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.getBucketReplication(null, {}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.getBucketReplication('', {},function () {
          })
        } catch (e) {
          done()
        }
      })
    })

    describe('removeBucketReplication(bucketName, callback)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.removeBucketReplication(null, {}, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.removeBucketReplication('', {},function () {
          })
        } catch (e) {
          done()
        }
      })
    })
  })


  describe('Object Legal Hold APIs', ()=> {
    describe('getObjectLegalHold(bucketName, objectName, getOpts={}, cb)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.getObjectLegalHold(null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.getObjectLegalHold('', function () {
          })
        } catch (e) {
          done()
        }
      })

      it('should fail on null objectName', (done) => {
        try {
          client.getObjectLegalHold('my-bucket', null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on null getOpts', (done) => {
        try {
          client.getObjectLegalHold('my-bucker', 'my-object', null,  function () {
          })
        } catch (e) {
          done()
        }
      })
    })

    describe('setObjectLegalHold(bucketName, objectName, setOpts={}, cb)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.setObjectLegalHold(null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.setObjectLegalHold('', function () {
          })
        } catch (e) {
          done()
        }
      })

      it('should fail on null objectName', (done) => {
        try {
          client.setObjectLegalHold('my-bucket', null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on null setOpts', (done) => {
        try {
          client.setObjectLegalHold('my-bucker', 'my-object', null,  function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty versionId', (done) => {
        try {
          client.setObjectLegalHold('my-bucker', 'my-object', {}, function () {
          })
        } catch (e) {
          done()
        }
      })
    })
  })

  describe('Compose Object APIs', ()=> {
    describe('composeObject(destObjConfig, sourceObjectList,cb)', () => {
      it('should fail on null destination config', (done) => {
        try {
          client.composeObject(null, function () {
          })
        } catch (e) {
          done()
        }
      })

      it('should fail on no array source config', (done) => {
        try {
          const destOptions = new CopyDestinationOptions({Bucket:'test-bucket', Object:'test-object'})
          client.composeObject(destOptions, 'non-array', function () {
          })
        } catch (e) {
          done()
        }
      })

      it('should fail on null source config', (done) => {
        try {
          const destOptions = new CopyDestinationOptions({Bucket:'test-bucket', Object:'test-object'})
          client.composeObject(destOptions, null, function () {
          })
        } catch (e) {
          done()
        }
      })

    })
  })
  describe('Select Object Content APIs', ()=> {
    describe('selectObjectContent(bucketName, objectName, selectOpts={}, cb)', () => {
      it('should fail on null bucket', (done) => {
        try {
          client.selectObjectContent(null, function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on empty bucket', (done) => {
        try {
          client.selectObjectContent('', function () {
          })
        } catch (e) {
          done()
        }
      })

      it('should fail on empty object', (done) => {
        try {
          client.selectObjectContent('my-bucket','', function () {
          })
        } catch (e) {
          done()
        }
      })
      it('should fail on null object', (done) => {
        try {
          client.selectObjectContent('my-bucket',null, function () {
          })
        } catch (e) {
          done()
        }
      })

    })
  })

})


describe('IP Address Validations', ()=>{

  it('should validate for valid ip', () => {
    assert.equal(isValidIP('1.1.1.1'), true)
  })

  it('Check list of IPV4 Invalid addresses', () => {
    const invalidIpv4 = [' 127.0.0.1', '127.0.0.1 ', '127.0.0.1 127.0.0.1', '127.0.0.256', '127.0.0.1//1', '127.0.0.1/0x1', '127.0.0.1/-1', '127.0.0.1/ab', '127.0.0.1/', '127.0.0.256/32', '127.0.0.1/33']
    invalidIpv4.map(ip=>{
      assert.equal(isValidIP(ip), false)
    })
  })

  it('Check list of IPV4 Valid addresses', () => {
    const validIpv4 =['001.002.003.004', '127.0.0.1', '255.255.255.255','192.168.1.10']
    validIpv4.map(ip=>{
      assert.equal(isValidIP(ip), true)
    })

  })

  it('Check list of IPV6 Invalid addresses', () => {
    const invalidIpV6 =[
      "':10.0.0.1",
      "-1",
      "::1 ::1",
      "1.2.3.4:1111:2222:3333:4444::5555",
      "1.2.3.4:1111:2222:3333::5555",
      "1.2.3.4:1111:2222::5555",
      "1.2.3.4:1111::5555",
      "1.2.3.4::",
      "1.2.3.4::5555",
      "11112222:3333:4444:5555:6666:1.2.3.4",
      "11112222:3333:4444:5555:6666:7777:8888",
      "::1//64",
      "::1/0001",
      "1111:",
      "1111:1.2.3.4",
      "1111:2222",
      "1111:22223333:4444:5555:6666:1.2.3.4",
      "1111:22223333:4444:5555:6666:7777:8888",
      "1111:2222:",
      "1111:2222:1.2.3.4",
      "1111:2222:3333",
      "1111:2222:33334444:5555:6666:1.2.3.4",
      "1111:2222:33334444:5555:6666:7777:8888",
      "1111:2222:3333:",
      "1111:2222:3333:1.2.3.4",
      "1111:2222:3333:4444",
      "1111:2222:3333:44445555:6666:1.2.3.4",
      "1111:2222:3333:44445555:6666:7777:8888",
      "1111:2222:3333:4444:",
      "1111:2222:3333:4444:1.2.3.4",
      "1111:2222:3333:4444:5555",
      "1111:2222:3333:4444:55556666:1.2.3.4",
      "1111:2222:3333:4444:55556666:7777:8888",
      "1111:2222:3333:4444:5555:",
      "1111:2222:3333:4444:5555:1.2.3.4",
      "1111:2222:3333:4444:5555:6666",
      "1111:2222:3333:4444:5555:66661.2.3.4",
      "1111:2222:3333:4444:5555:66667777:8888",
      "1111:2222:3333:4444:5555:6666:",
      "1111:2222:3333:4444:5555:6666:1.2.3.4.5",
      "1111:2222:3333:4444:5555:6666:255.255.255255",
      "1111:2222:3333:4444:5555:6666:255.255255.255",
      "1111:2222:3333:4444:5555:6666:255255.255.255",
      "1111:2222:3333:4444:5555:6666:256.256.256.256",
      "1111:2222:3333:4444:5555:6666:7777",
      "1111:2222:3333:4444:5555:6666:77778888",
      "1111:2222:3333:4444:5555:6666:7777:",
      "1111:2222:3333:4444:5555:6666:7777:1.2.3.4",
      "1111:2222:3333:4444:5555:6666:7777:::",
      "1111:2222:3333:4444:5555:6666::8888:",
      "1111:2222:3333:4444:5555:6666:::",
      "1111:2222:3333:4444:5555:6666:::8888",
      "1111:2222:3333:4444:5555::7777:8888:",
      "1111:2222:3333:4444:5555::7777::",
      "1111:2222:3333:4444:5555::8888:",
      "1111:2222:3333:4444:5555:::",
      "1111:2222:3333:4444:5555:::1.2.3.4",
      "1111:2222:3333:4444:5555:::7777:8888",
      "1111:2222:3333:4444::5555:",
      "1111:2222:3333:4444::6666:7777:8888:",
      "1111:2222:3333:4444::6666:7777::",
      "1111:2222:3333:4444::6666::8888",
      "1111:2222:3333:4444::7777:8888:",
      "1111:2222:3333:4444::8888:",
      "1111:2222:3333:4444:::",
      "1111:2222:3333:4444:::6666:1.2.3.4",
      "1111:2222:3333:4444:::6666:7777:8888",
      "1111:2222:3333::5555:",
      "1111:2222:3333::5555:6666:7777:8888:",
      "1111:2222:3333::5555:6666:7777::",
      "1111:2222:3333::5555:6666::8888",
      "1111:2222:3333::5555::1.2.3.4",
      "1111:2222:3333::5555::7777:8888",
      "1111:2222:3333::6666:7777:8888:",
      "1111:2222:3333::7777:8888:",
      "1111:2222:3333::8888:",
      "1111:2222:3333:::",
      "1111:2222:3333:::5555:6666:1.2.3.4",
      "1111:2222:3333:::5555:6666:7777:8888",
      "1111:2222::4444:5555:6666:7777:8888:",
      "1111:2222::4444:5555:6666:7777::",
      "1111:2222::4444:5555:6666::8888",
      "1111:2222::4444:5555::1.2.3.4",
      "1111:2222::4444:5555::7777:8888",
      "1111:2222::4444::6666:1.2.3.4",
      "1111:2222::4444::6666:7777:8888",
      "1111:2222::5555:",
      "1111:2222::5555:6666:7777:8888:",
      "1111:2222::6666:7777:8888:",
      "1111:2222::7777:8888:",
      "1111:2222::8888:",
      "1111:2222:::",
      "1111:2222:::4444:5555:6666:1.2.3.4",
      "1111:2222:::4444:5555:6666:7777:8888",
      "1111::3333:4444:5555:6666:7777:8888:",
      "1111::3333:4444:5555:6666:7777::",
      "1111::3333:4444:5555:6666::8888",
      "1111::3333:4444:5555::1.2.3.4",
      "1111::3333:4444:5555::7777:8888",
      "1111::3333:4444::6666:1.2.3.4",
      "1111::3333:4444::6666:7777:8888",
      "1111::3333::5555:6666:1.2.3.4",
      "1111::3333::5555:6666:7777:8888",
      "1111::4444:5555:6666:7777:8888:",
      "1111::5555:",
      "1111::5555:6666:7777:8888:",
      "1111::6666:7777:8888:",
      "1111::7777:8888:",
      "1111::8888:",
      "1111:::",
      "1111:::3333:4444:5555:6666:1.2.3.4",
      "1111:::3333:4444:5555:6666:7777:8888",
      "12345::6:7:8",
      "124.15.6.89/60",
      "1:2:3:4:5:6:7:8:9",
      "1:2:3::4:5:6:7:8:9",
      "1:2:3::4:5::7:8",
      "1::1.2.256.4",
      "1::1.2.3.256",
      "1::1.2.3.300",
      "1::1.2.3.900",
      "1::1.2.300.4",
      "1::1.2.900.4",
      "1::1.256.3.4",
      "1::1.300.3.4",
      "1::1.900.3.4",
      "1::256.2.3.4",
      "1::260.2.3.4",
      "1::2::3",
      "1::300.2.3.4",
      "1::300.300.300.300",
      "1::3000.30.30.30",
      "1::400.2.3.4",
      "1::5:1.2.256.4",
      "1::5:1.2.3.256",
      "1::5:1.2.3.300",
      "1::5:1.2.3.900",
      "1::5:1.2.300.4",
      "1::5:1.2.900.4",
      "1::5:1.256.3.4",
      "1::5:1.300.3.4",
      "1::5:1.900.3.4",
      "1::5:256.2.3.4",
      "1::5:260.2.3.4",
      "1::5:300.2.3.4",
      "1::5:300.300.300.300",
      "1::5:3000.30.30.30",
      "1::5:400.2.3.4",
      "1::5:900.2.3.4",
      "1::900.2.3.4",
      "1:::3:4:5",
      "2001:0000:1234: 0000:0000:C1C0:ABCD:0876",
      "2001:0000:1234:0000:0000:C1C0:ABCD:0876  0",
      "2001:1:1:1:1:1:255Z255X255Y255",
      "2001::FFD3::57ab",
      "2001:DB8:0:0:8:800:200C:417A:221",
      "2001:db8:85a3::8a2e:37023:7334",
      "2001:db8:85a3::8a2e:370k:7334",
      "3ffe:0b00:0000:0001:0000:0000:000a",
      "3ffe:b00::1::a",
      ":",
      ":1.2.3.4",
      ":1111:2222:3333:4444:5555:6666:1.2.3.4",
      ":1111:2222:3333:4444:5555:6666:7777:8888",
      ":1111:2222:3333:4444:5555:6666:7777::",
      ":1111:2222:3333:4444:5555:6666::",
      ":1111:2222:3333:4444:5555:6666::8888",
      ":1111:2222:3333:4444:5555::",
      ":1111:2222:3333:4444:5555::1.2.3.4",
      ":1111:2222:3333:4444:5555::7777:8888",
      ":1111:2222:3333:4444:5555::8888",
      ":1111:2222:3333:4444::",
      ":1111:2222:3333:4444::1.2.3.4",
      ":1111:2222:3333:4444::5555",
      ":1111:2222:3333:4444::6666:1.2.3.4",
      ":1111:2222:3333:4444::6666:7777:8888",
      ":1111:2222:3333:4444::7777:8888",
      ":1111:2222:3333:4444::8888",
      ":1111:2222:3333::",
      ":1111:2222:3333::1.2.3.4",
      ":1111:2222:3333::5555",
      ":1111:2222:3333::5555:6666:1.2.3.4",
      ":1111:2222:3333::5555:6666:7777:8888",
      ":1111:2222:3333::6666:1.2.3.4",
      ":1111:2222:3333::6666:7777:8888",
      ":1111:2222:3333::7777:8888",
      ":1111:2222:3333::8888",
      ":1111:2222::",
      ":1111:2222::1.2.3.4",
      ":1111:2222::4444:5555:6666:1.2.3.4",
      ":1111:2222::4444:5555:6666:7777:8888",
      ":1111:2222::5555",
      ":1111:2222::5555:6666:1.2.3.4",
      ":1111:2222::5555:6666:7777:8888",
      ":1111:2222::6666:1.2.3.4",
      ":1111:2222::6666:7777:8888",
      ":1111:2222::7777:8888",
      ":1111:2222::8888",
      ":1111::",
      ":1111::1.2.3.4",
      ":1111::3333:4444:5555:6666:1.2.3.4",
      ":1111::3333:4444:5555:6666:7777:8888",
      ":1111::4444:5555:6666:1.2.3.4",
      ":1111::4444:5555:6666:7777:8888",
      ":1111::5555",
      ":1111::5555:6666:1.2.3.4",
      ":1111::5555:6666:7777:8888",
      ":1111::6666:1.2.3.4",
      ":1111::6666:7777:8888",
      ":1111::7777:8888",
      ":1111::8888",
      ":2222:3333:4444:5555:6666:1.2.3.4",
      ":2222:3333:4444:5555:6666:7777:8888",
      ":3333:4444:5555:6666:1.2.3.4",
      ":3333:4444:5555:6666:7777:8888",
      ":4444:5555:6666:1.2.3.4",
      ":4444:5555:6666:7777:8888",
      ":5555:6666:1.2.3.4",
      ":5555:6666:7777:8888",
      ":6666:1.2.3.4",
      ":6666:7777:8888",
      ":7777:8888",
      ":8888",
      "::-1",
      "::.",
      "::..",
      "::...",
      "::...4",
      "::..3.",
      "::..3.4",
      "::.2..",
      "::.2.3.",
      "::.2.3.4",
      "::1...",
      "::1.2..",
      "::1.2.256.4",
      "::1.2.3.",
      "::1.2.3.256",
      "::1.2.3.300",
      "::1.2.3.900",
      "::1.2.300.4",
      "::1.2.900.4",
      "::1.256.3.4",
      "::1.300.3.4",
      "::1.900.3.4",
      "::1111:2222:3333:4444:5555:6666::",
      "::2222:3333:4444:5555:6666:7777:8888:",
      "::2222:3333:4444:5555:7777:8888::",
      "::2222:3333:4444:5555:7777::8888",
      "::2222:3333:4444:5555::1.2.3.4",
      "::2222:3333:4444:5555::7777:8888",
      "::2222:3333:4444::6666:1.2.3.4",
      "::2222:3333:4444::6666:7777:8888",
      "::2222:3333::5555:6666:1.2.3.4",
      "::2222:3333::5555:6666:7777:8888",
      "::2222::4444:5555:6666:1.2.3.4",
      "::2222::4444:5555:6666:7777:8888",
      "::256.2.3.4",
      "::260.2.3.4",
      "::300.2.3.4",
      "::300.300.300.300",
      "::3000.30.30.30",
      "::3333:4444:5555:6666:7777:8888:",
      "::400.2.3.4",
      "::4444:5555:6666:7777:8888:",
      "::5555:",
      "::5555:6666:7777:8888:",
      "::6666:7777:8888:",
      "::7777:8888:",
      "::8888:",
      "::900.2.3.4",
      ":::",
      ":::1.2.3.4",
      ":::2222:3333:4444:5555:6666:1.2.3.4",
      ":::2222:3333:4444:5555:6666:7777:8888",
      ":::3333:4444:5555:6666:7777:8888",
      ":::4444:5555:6666:1.2.3.4",
      ":::4444:5555:6666:7777:8888",
      ":::5555",
      ":::5555:6666:1.2.3.4",
      ":::5555:6666:7777:8888",
      ":::6666:1.2.3.4",
      ":::6666:7777:8888",
      ":::7777:8888",
      ":::8888",
      "::ffff:192x168.1.26",
      "::ffff:2.3.4",
      "::ffff:257.1.2.3",
      "FF01::101::2",
      "FF02:0000:0000:0000:0000:0000:0000:0000:0001",
      "XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:1.2.3.4",
      "XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX",
      "a::b::c",
      "a::g",
      "a:a:a:a:a:a:a:a:a",
      "a:aaaaa::",
      "a:b",
      "a:b:c:d:e:f:g:0",
      "ffff:",
      "ffff::ffff::ffff",
      "ffgg:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
      "ldkfj",
      "::/129",
      "1000:://32",
      "::/"
    ]
    invalidIpV6.map(ip=>{
      const valid=  isValidIP(ip)
      assert.equal(valid, false)
    })
  })

  it('Check list of IPV6 Valid addresses', () => {
    const validIpv6 =[
      "0000:0000:0000:0000:0000:0000:0000:0000",
      "0000:0000:0000:0000:0000:0000:0000:0001",
      "0:0:0:0:0:0:0:0",
      "0:0:0:0:0:0:0:1",
      "0:0:0:0:0:0:0::",
      "0:0:0:0:0:0:13.1.68.3",
      "0:0:0:0:0:0::",
      "0:0:0:0:0::",
      "0:0:0:0:0:FFFF:129.144.52.38",
      "0:0:0:0:1:0:0:0",
      "0:0:0:0::",
      "0:0:0::",
      "0:0::",
      "0:1:2:3:4:5:6:7",
      "0::",
      "0:a:b:c:d:e:f::",
      "1080:0:0:0:8:800:200c:417a",
      "1080::8:800:200c:417a",
      "1111:2222:3333:4444:5555:6666:123.123.123.123",
      "1111:2222:3333:4444:5555:6666:7777:8888",
      "1111:2222:3333:4444:5555:6666:7777::",
      "1111:2222:3333:4444:5555:6666::",
      "1111:2222:3333:4444:5555:6666::8888",
      "1111:2222:3333:4444:5555::",
      "1111:2222:3333:4444:5555::7777:8888",
      "1111:2222:3333:4444:5555::8888",
      "1111:2222:3333:4444::",
      "1111:2222:3333:4444::6666:123.123.123.123",
      "1111:2222:3333:4444::6666:7777:8888",
      "1111:2222:3333:4444::7777:8888",
      "1111:2222:3333:4444::8888",
      "1111:2222:3333::",
      "1111:2222:3333::5555:6666:123.123.123.123",
      "1111:2222:3333::5555:6666:7777:8888",
      "1111:2222:3333::6666:123.123.123.123",
      "1111:2222:3333::6666:7777:8888",
      "1111:2222:3333::7777:8888",
      "1111:2222:3333::8888",
      "1111:2222::",
      "1111:2222::4444:5555:6666:123.123.123.123",
      "1111:2222::4444:5555:6666:7777:8888",
      "1111:2222::5555:6666:123.123.123.123",
      "1111:2222::5555:6666:7777:8888",
      "1111:2222::6666:123.123.123.123",
      "1111:2222::6666:7777:8888",
      "1111:2222::7777:8888",
      "1111:2222::8888",
      "1111::",
      "1111::3333:4444:5555:6666:123.123.123.123",
      "1111::3333:4444:5555:6666:7777:8888",
      "1111::4444:5555:6666:123.123.123.123",
      "1111::4444:5555:6666:7777:8888",
      "1111::5555:6666:123.123.123.123",
      "1111::5555:6666:7777:8888",
      "1111::6666:123.123.123.123",
      "1111::6666:7777:8888",
      "1111::7777:8888",
      "1111::8888",
      "1:2:3:4:5:6:1.2.3.4",
      "1:2:3:4:5:6:7:8",
      "1:2:3:4:5:6::",
      "1:2:3:4:5:6::8",
      "1:2:3:4:5::",
      "1:2:3:4:5::7:8",
      "1:2:3:4:5::8",
      "1:2:3:4::",
      "1:2:3:4::5:1.2.3.4",
      "1:2:3:4::7:8",
      "1:2:3:4::8",
      "1:2:3::",
      "1:2:3::5:1.2.3.4",
      "1:2:3::7:8",
      "1:2:3::8",
      "1:2::",
      "1:2::5:1.2.3.4",
      "1:2::7:8",
      "1:2::8",
      "1::",
      "1::2:3",
      "1::2:3:4",
      "1::2:3:4:5",
      "1::2:3:4:5:6",
      "1::2:3:4:5:6:7",
      "1::5:1.2.3.4",
      "1::5:11.22.33.44",
      "1::7:8",
      "1::8",
      "2001:0000:1234:0000:0000:C1C0:ABCD:0876",
      "2001:0000:4136:e378:8000:63bf:3fff:fdd2",
      "2001:0db8:0000:0000:0000:0000:1428:57ab",
      "2001:0db8:0000:0000:0000::1428:57ab",
      "2001:0db8:0:0:0:0:1428:57ab",
      "2001:0db8:0:0::1428:57ab",
      "2001:0db8:1234:0000:0000:0000:0000:0000",
      "2001:0db8:1234::",
      "2001:0db8:1234:ffff:ffff:ffff:ffff:ffff",
      "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
      "2001:0db8::1428:57ab",
      "2001::CE49:7601:2CAD:DFFF:7C94:FFFE",
      "2001::CE49:7601:E866:EFFF:62C3:FFFE",
      "2001:DB8:0:0:8:800:200C:417A",
      "2001:DB8::8:800:200C:417A",
      "2001:db8:85a3:0:0:8a2e:370:7334",
      "2001:db8:85a3::8a2e:370:7334",
      "2001:db8::",
      "2001:db8::1428:57ab",
      "2001:db8:a::123",
      "2002::",
      "2608::3:5",
      "2608:af09:30:0:0:0:0:134",
      "2608:af09:30::102a:7b91:c239:baff",
      "2::10",
      "3ffe:0b00:0000:0000:0001:0000:0000:000a",
      "7:6:5:4:3:2:1:0",
      "::",
      "::0",
      "::0:0",
      "::0:0:0",
      "::0:0:0:0",
      "::0:0:0:0:0",
      "::0:0:0:0:0:0",
      "::0:0:0:0:0:0:0",
      "::0:a:b:c:d:e:f",
      "::1",
      "::123.123.123.123",
      "::13.1.68.3",
      "::2222:3333:4444:5555:6666:123.123.123.123",
      "::2222:3333:4444:5555:6666:7777:8888",
      "::2:3",
      "::2:3:4",
      "::2:3:4:5",
      "::2:3:4:5:6",
      "::2:3:4:5:6:7",
      "::2:3:4:5:6:7:8",
      "::3333:4444:5555:6666:7777:8888",
      "::4444:5555:6666:123.123.123.123",
      "::4444:5555:6666:7777:8888",
      "::5555:6666:123.123.123.123",
      "::5555:6666:7777:8888",
      "::6666:123.123.123.123",
      "::6666:7777:8888",
      "::7777:8888",
      "::8",
      "::8888",
      "::FFFF:129.144.52.38",
      "::ffff:0:0",
      "::ffff:0c22:384e",
      "::ffff:12.34.56.78",
      "::ffff:192.0.2.128",
      "::ffff:192.168.1.1",
      "::ffff:192.168.1.26",
      "::ffff:c000:280",
      "FF01:0:0:0:0:0:0:101",
      "FF01::101",
      "FF02:0000:0000:0000:0000:0000:0000:0001",
      "a:b:c:d:e:f:0::",
      "fe80:0000:0000:0000:0204:61ff:fe9d:f156",
      "fe80:0:0:0:204:61ff:254.157.241.86",
      "fe80:0:0:0:204:61ff:fe9d:f156",
      "fe80::",
      "fe80::1",
      "fe80::204:61ff:254.157.241.86",
      "fe80::204:61ff:fe9d:f156",
      "fe80::217:f2ff:254.7.237.98",
      "fe80::217:f2ff:fe07:ed62",
      "fedc:ba98:7654:3210:fedc:ba98:7654:3210",
      "ff02::1",
      "ffff::",
      "ffff::3:5",
      "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
      "a:0::0:b",
      "a:0:0::0:b",
      "a:0::0:0:b",
      "a::0:0:b",
      "a::0:b",
      "a:0::b",
      "a:0:0::b"
    ]
    validIpv6.map(ip=>{
      const valid=  isValidIP(ip)
      assert.equal(valid, true)

    })
  })
})
