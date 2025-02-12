import * as crypto from 'node:crypto'
import * as fs from 'node:fs'
import type { IncomingHttpHeaders } from 'node:http'
import * as http from 'node:http'
import * as https from 'node:https'
import * as path from 'node:path'
import * as stream from 'node:stream'

import * as async from 'async'
import BlockStream2 from 'block-stream2'
import { isBrowser } from 'browser-or-node'
import _ from 'lodash'
import * as qs from 'query-string'
import xml2js from 'xml2js'

import { CredentialProvider } from '../CredentialProvider.ts'
import * as errors from '../errors.ts'
import type { SelectResults } from '../helpers.ts'
import {
  CopyDestinationOptions,
  CopySourceOptions,
  DEFAULT_REGION,
  LEGAL_HOLD_STATUS,
  PRESIGN_EXPIRY_DAYS_MAX,
  RETENTION_MODES,
  RETENTION_VALIDITY_UNITS,
} from '../helpers.ts'
import type { PostPolicyResult } from '../minio.ts'
import { postPresignSignatureV4, presignSignatureV4, signV4 } from '../signing.ts'
import { fsp, streamPromise } from './async.ts'
import { CopyConditions } from './copy-conditions.ts'
import { Extensions } from './extensions.ts'
import {
  calculateEvenSplits,
  extractMetadata,
  getContentLength,
  getScope,
  getSourceVersionId,
  getVersionId,
  hashBinary,
  insertContentType,
  isAmazonEndpoint,
  isBoolean,
  isDefined,
  isEmpty,
  isNumber,
  isObject,
  isReadableStream,
  isString,
  isValidBucketName,
  isValidEndpoint,
  isValidObjectName,
  isValidPort,
  isValidPrefix,
  isVirtualHostStyle,
  makeDateLong,
  PART_CONSTRAINTS,
  partsRequired,
  prependXAMZMeta,
  readableStream,
  sanitizeETag,
  toMd5,
  toSha256,
  uriEscape,
  uriResourceEscape,
} from './helper.ts'
import { joinHostPort } from './join-host-port.ts'
import { PostPolicy } from './post-policy.ts'
import { request } from './request.ts'
import { drainResponse, readAsBuffer, readAsString } from './response.ts'
import type { Region } from './s3-endpoints.ts'
import { getS3Endpoint } from './s3-endpoints.ts'
import type {
  Binary,
  BucketItemFromList,
  BucketItemStat,
  BucketStream,
  BucketVersioningConfiguration,
  CopyObjectParams,
  CopyObjectResult,
  CopyObjectResultV2,
  EncryptionConfig,
  GetObjectLegalHoldOptions,
  GetObjectOpts,
  GetObjectRetentionOpts,
  IncompleteUploadedBucketItem,
  IRequest,
  ItemBucketMetadata,
  LifecycleConfig,
  LifeCycleConfigParam,
  ListObjectQueryOpts,
  ListObjectQueryRes,
  ObjectInfo,
  ObjectLockConfigParam,
  ObjectLockInfo,
  ObjectMetaData,
  ObjectRetentionInfo,
  PreSignRequestParams,
  PutObjectLegalHoldOptions,
  PutTaggingParams,
  RemoveObjectsParam,
  RemoveObjectsRequestEntry,
  RemoveObjectsResponse,
  RemoveTaggingParams,
  ReplicationConfig,
  ReplicationConfigOpts,
  RequestHeaders,
  ResponseHeader,
  ResultCallback,
  Retention,
  SelectOptions,
  StatObjectOpts,
  Tag,
  TaggingOpts,
  Tags,
  Transport,
  UploadedObjectInfo,
  UploadPartConfig,
} from './type.ts'
import type { ListMultipartResult, UploadedPart } from './xml-parser.ts'
import {
  parseCompleteMultipart,
  parseInitiateMultipart,
  parseListObjects,
  parseObjectLegalHoldConfig,
  parseSelectObjectContentResponse,
  uploadPartParser,
} from './xml-parser.ts'
import * as xmlParsers from './xml-parser.ts'

const xml = new xml2js.Builder({ renderOpts: { pretty: false }, headless: true })

// will be replaced by bundler.
const Package = { version: process.env.MINIO_JS_PACKAGE_VERSION || 'development' }

const requestOptionProperties = [
  'agent',
  'ca',
  'cert',
  'ciphers',
  'clientCertEngine',
  'crl',
  'dhparam',
  'ecdhCurve',
  'family',
  'honorCipherOrder',
  'key',
  'passphrase',
  'pfx',
  'rejectUnauthorized',
  'secureOptions',
  'secureProtocol',
  'servername',
  'sessionIdContext',
] as const

export interface ClientOptions {
  endPoint: string
  accessKey?: string
  secretKey?: string
  useSSL?: boolean
  port?: number
  region?: Region
  transport?: Transport
  sessionToken?: string
  partSize?: number
  pathStyle?: boolean
  credentialsProvider?: CredentialProvider
  s3AccelerateEndpoint?: string
  transportAgent?: http.Agent
}

export type RequestOption = Partial<IRequest> & {
  method: string
  bucketName?: string
  objectName?: string
  query?: string
  pathStyle?: boolean
}

export type NoResultCallback = (error: unknown) => void

export interface MakeBucketOpt {
  ObjectLocking?: boolean
}

export interface RemoveOptions {
  versionId?: string
  governanceBypass?: boolean
  forceDelete?: boolean
}

type Part = {
  part: number
  etag: string
}

export class TypedClient {
  protected transport: Transport
  protected host: string
  protected port: number
  protected protocol: string
  protected accessKey: string
  protected secretKey: string
  protected sessionToken?: string
  protected userAgent: string
  protected anonymous: boolean
  protected pathStyle: boolean
  protected regionMap: Record<string, string>
  public region?: string
  protected credentialsProvider?: CredentialProvider
  partSize: number = 64 * 1024 * 1024
  protected overRidePartSize?: boolean

  protected maximumPartSize = 5 * 1024 * 1024 * 1024
  protected maxObjectSize = 5 * 1024 * 1024 * 1024 * 1024
  public enableSHA256: boolean
  protected s3AccelerateEndpoint?: string
  protected reqOptions: Record<string, unknown>

  protected transportAgent: http.Agent
  private readonly clientExtensions: Extensions

  constructor(params: ClientOptions) {
    // @ts-expect-error deprecated property
    if (params.secure !== undefined) {
      throw new Error('"secure" option deprecated, "useSSL" should be used instead')
    }
    // Default values if not specified.
    if (params.useSSL === undefined) {
      params.useSSL = true
    }
    if (!params.port) {
      params.port = 0
    }
    // Validate input params.
    if (!isValidEndpoint(params.endPoint)) {
      throw new errors.InvalidEndpointError(`Invalid endPoint : ${params.endPoint}`)
    }
    if (!isValidPort(params.port)) {
      throw new errors.InvalidArgumentError(`Invalid port : ${params.port}`)
    }
    if (!isBoolean(params.useSSL)) {
      throw new errors.InvalidArgumentError(
        `Invalid useSSL flag type : ${params.useSSL}, expected to be of type "boolean"`,
      )
    }

    // Validate region only if its set.
    if (params.region) {
      if (!isString(params.region)) {
        throw new errors.InvalidArgumentError(`Invalid region : ${params.region}`)
      }
    }

    const host = params.endPoint.toLowerCase()
    let port = params.port
    let protocol: string
    let transport
    let transportAgent: http.Agent
    // Validate if configuration is not using SSL
    // for constructing relevant endpoints.
    if (params.useSSL) {
      // Defaults to secure.
      transport = https
      protocol = 'https:'
      port = port || 443
      transportAgent = https.globalAgent
    } else {
      transport = http
      protocol = 'http:'
      port = port || 80
      transportAgent = http.globalAgent
    }

    // if custom transport is set, use it.
    if (params.transport) {
      if (!isObject(params.transport)) {
        throw new errors.InvalidArgumentError(
          `Invalid transport type : ${params.transport}, expected to be type "object"`,
        )
      }
      transport = params.transport
    }

    // if custom transport agent is set, use it.
    if (params.transportAgent) {
      if (!isObject(params.transportAgent)) {
        throw new errors.InvalidArgumentError(
          `Invalid transportAgent type: ${params.transportAgent}, expected to be type "object"`,
        )
      }

      transportAgent = params.transportAgent
    }

    // User Agent should always following the below style.
    // Please open an issue to discuss any new changes here.
    //
    //       MinIO (OS; ARCH) LIB/VER APP/VER
    //
    const libraryComments = `(${process.platform}; ${process.arch})`
    const libraryAgent = `MinIO ${libraryComments} minio-js/${Package.version}`
    // User agent block ends.

    this.transport = transport
    this.transportAgent = transportAgent
    this.host = host
    this.port = port
    this.protocol = protocol
    this.userAgent = `${libraryAgent}`

    // Default path style is true
    if (params.pathStyle === undefined) {
      this.pathStyle = true
    } else {
      this.pathStyle = params.pathStyle
    }

    this.accessKey = params.accessKey ?? ''
    this.secretKey = params.secretKey ?? ''
    this.sessionToken = params.sessionToken
    this.anonymous = !this.accessKey || !this.secretKey

    if (params.credentialsProvider) {
      this.anonymous = false
      this.credentialsProvider = params.credentialsProvider
    }

    this.regionMap = {}
    if (params.region) {
      this.region = params.region
    }

    if (params.partSize) {
      this.partSize = params.partSize
      this.overRidePartSize = true
    }
    if (this.partSize < 5 * 1024 * 1024) {
      throw new errors.InvalidArgumentError(`Part size should be greater than 5MB`)
    }
    if (this.partSize > 5 * 1024 * 1024 * 1024) {
      throw new errors.InvalidArgumentError(`Part size should be less than 5GB`)
    }

    // SHA256 is enabled only for authenticated http requests. If the request is authenticated
    // and the connection is https we use x-amz-content-sha256=UNSIGNED-PAYLOAD
    // header for signature calculation.
    this.enableSHA256 = !this.anonymous && !params.useSSL

    this.s3AccelerateEndpoint = params.s3AccelerateEndpoint || undefined
    this.reqOptions = {}
    this.clientExtensions = new Extensions(this)
  }
  /**
   * Minio extensions that aren't necessary present for Amazon S3 compatible storage servers
   */
  get extensions() {
    return this.clientExtensions
  }

  /**
   * @param endPoint - valid S3 acceleration end point
   */
  setS3TransferAccelerate(endPoint: string) {
    this.s3AccelerateEndpoint = endPoint
  }

  /**
   * Sets the supported request options.
   */
  public setRequestOptions(options: Pick<https.RequestOptions, (typeof requestOptionProperties)[number]>) {
    if (!isObject(options)) {
      throw new TypeError('request options should be of type "object"')
    }
    this.reqOptions = _.pick(options, requestOptionProperties)
  }

  /**
   *  This is s3 Specific and does not hold validity in any other Object storage.
   */
  private getAccelerateEndPointIfSet(bucketName?: string, objectName?: string) {
    if (!isEmpty(this.s3AccelerateEndpoint) && !isEmpty(bucketName) && !isEmpty(objectName)) {
      // http://docs.aws.amazon.com/AmazonS3/latest/dev/transfer-acceleration.html
      // Disable transfer acceleration for non-compliant bucket names.
      if (bucketName.includes('.')) {
        throw new Error(`Transfer Acceleration is not supported for non compliant bucket:${bucketName}`)
      }
      // If transfer acceleration is requested set new host.
      // For more details about enabling transfer acceleration read here.
      // http://docs.aws.amazon.com/AmazonS3/latest/dev/transfer-acceleration.html
      return this.s3AccelerateEndpoint
    }
    return false
  }

  /**
   *   Set application specific information.
   *   Generates User-Agent in the following style.
   *   MinIO (OS; ARCH) LIB/VER APP/VER
   */
  setAppInfo(appName: string, appVersion: string) {
    if (!isString(appName)) {
      throw new TypeError(`Invalid appName: ${appName}`)
    }
    if (appName.trim() === '') {
      throw new errors.InvalidArgumentError('Input appName cannot be empty.')
    }
    if (!isString(appVersion)) {
      throw new TypeError(`Invalid appVersion: ${appVersion}`)
    }
    if (appVersion.trim() === '') {
      throw new errors.InvalidArgumentError('Input appVersion cannot be empty.')
    }
    this.userAgent = `${this.userAgent} ${appName}/${appVersion}`
  }

  /**
   * returns options object that can be used with http.request()
   * Takes care of constructing virtual-host-style or path-style hostname
   */
  protected getRequestOptions(
    opts: RequestOption & {
      region: string
    },
  ): IRequest & {
    host: string
    headers: Record<string, string>
  } {
    const method = opts.method
    const region = opts.region
    const bucketName = opts.bucketName
    let objectName = opts.objectName
    const headers = opts.headers
    const query = opts.query

    let reqOptions = {
      method,
      headers: {} as RequestHeaders,
      protocol: this.protocol,
      // If custom transportAgent was supplied earlier, we'll inject it here
      agent: this.transportAgent,
    }

    // Verify if virtual host supported.
    let virtualHostStyle
    if (bucketName) {
      virtualHostStyle = isVirtualHostStyle(this.host, this.protocol, bucketName, this.pathStyle)
    }

    let path = '/'
    let host = this.host

    let port: undefined | number
    if (this.port) {
      port = this.port
    }

    if (objectName) {
      objectName = uriResourceEscape(objectName)
    }

    // For Amazon S3 endpoint, get endpoint based on region.
    if (isAmazonEndpoint(host)) {
      const accelerateEndPoint = this.getAccelerateEndPointIfSet(bucketName, objectName)
      if (accelerateEndPoint) {
        host = `${accelerateEndPoint}`
      } else {
        host = getS3Endpoint(region)
      }
    }

    if (virtualHostStyle && !opts.pathStyle) {
      // For all hosts which support virtual host style, `bucketName`
      // is part of the hostname in the following format:
      //
      //  var host = 'bucketName.example.com'
      //
      if (bucketName) {
        host = `${bucketName}.${host}`
      }
      if (objectName) {
        path = `/${objectName}`
      }
    } else {
      // For all S3 compatible storage services we will fallback to
      // path style requests, where `bucketName` is part of the URI
      // path.
      if (bucketName) {
        path = `/${bucketName}`
      }
      if (objectName) {
        path = `/${bucketName}/${objectName}`
      }
    }

    if (query) {
      path += `?${query}`
    }
    reqOptions.headers.host = host
    if ((reqOptions.protocol === 'http:' && port !== 80) || (reqOptions.protocol === 'https:' && port !== 443)) {
      reqOptions.headers.host = joinHostPort(host, port)
    }

    reqOptions.headers['user-agent'] = this.userAgent
    if (headers) {
      // have all header keys in lower case - to make signing easy
      for (const [k, v] of Object.entries(headers)) {
        reqOptions.headers[k.toLowerCase()] = v
      }
    }

    // Use any request option specified in minioClient.setRequestOptions()
    reqOptions = Object.assign({}, this.reqOptions, reqOptions)

    return {
      ...reqOptions,
      headers: _.mapValues(_.pickBy(reqOptions.headers, isDefined), (v) => v.toString()),
      host,
      port,
      path,
    } satisfies https.RequestOptions
  }

  public async setCredentialsProvider(credentialsProvider: CredentialProvider) {
    if (!(credentialsProvider instanceof CredentialProvider)) {
      throw new Error('Unable to get credentials. Expected instance of CredentialProvider')
    }
    this.credentialsProvider = credentialsProvider
    await this.checkAndRefreshCreds()
  }

  private async checkAndRefreshCreds() {
    if (this.credentialsProvider) {
      try {
        const credentialsConf = await this.credentialsProvider.getCredentials()
        this.accessKey = credentialsConf.getAccessKey()
        this.secretKey = credentialsConf.getSecretKey()
        this.sessionToken = credentialsConf.getSessionToken()
      } catch (e) {
        throw new Error(`Unable to get credentials: ${e}`, { cause: e })
      }
    }
  }

  private logStream?: stream.Writable

  /**
   * log the request, response, error
   */
  private logHTTP(reqOptions: IRequest, response: http.IncomingMessage | null, err?: unknown) {
    // if no logStream available return.
    if (!this.logStream) {
      return
    }
    if (!isObject(reqOptions)) {
      throw new TypeError('reqOptions should be of type "object"')
    }
    if (response && !isReadableStream(response)) {
      throw new TypeError('response should be of type "Stream"')
    }
    if (err && !(err instanceof Error)) {
      throw new TypeError('err should be of type "Error"')
    }
    const logStream = this.logStream
    const logHeaders = (headers: RequestHeaders) => {
      Object.entries(headers).forEach(([k, v]) => {
        if (k == 'authorization') {
          if (isString(v)) {
            const redactor = new RegExp('Signature=([0-9a-f]+)')
            v = v.replace(redactor, 'Signature=**REDACTED**')
          }
        }
        logStream.write(`${k}: ${v}\n`)
      })
      logStream.write('\n')
    }
    logStream.write(`REQUEST: ${reqOptions.method} ${reqOptions.path}\n`)
    logHeaders(reqOptions.headers)
    if (response) {
      this.logStream.write(`RESPONSE: ${response.statusCode}\n`)
      logHeaders(response.headers as RequestHeaders)
    }
    if (err) {
      logStream.write('ERROR BODY:\n')
      const errJSON = JSON.stringify(err, null, '\t')
      logStream.write(`${errJSON}\n`)
    }
  }

  /**
   * Enable tracing
   */
  public traceOn(stream?: stream.Writable) {
    if (!stream) {
      stream = process.stdout
    }
    this.logStream = stream
  }

  /**
   * Disable tracing
   */
  public traceOff() {
    this.logStream = undefined
  }

  /**
   * makeRequest is the primitive used by the apis for making S3 requests.
   * payload can be empty string in case of no payload.
   * statusCode is the expected statusCode. If response.statusCode does not match
   * we parse the XML error and call the callback with the error message.
   *
   * A valid region is passed by the calls - listBuckets, makeBucket and getBucketRegion.
   *
   * @internal
   */
  async makeRequestAsync(
    options: RequestOption,
    payload: Binary = '',
    expectedCodes: number[] = [200],
    region = '',
  ): Promise<http.IncomingMessage> {
    if (!isObject(options)) {
      throw new TypeError('options should be of type "object"')
    }
    if (!isString(payload) && !isObject(payload)) {
      // Buffer is of type 'object'
      throw new TypeError('payload should be of type "string" or "Buffer"')
    }
    expectedCodes.forEach((statusCode) => {
      if (!isNumber(statusCode)) {
        throw new TypeError('statusCode should be of type "number"')
      }
    })
    if (!isString(region)) {
      throw new TypeError('region should be of type "string"')
    }
    if (!options.headers) {
      options.headers = {}
    }
    if (options.method === 'POST' || options.method === 'PUT' || options.method === 'DELETE') {
      options.headers['content-length'] = payload.length.toString()
    }
    const sha256sum = this.enableSHA256 ? toSha256(payload) : ''
    return this.makeRequestStreamAsync(options, payload, sha256sum, expectedCodes, region)
  }

  /**
   * new request with promise
   *
   * No need to drain response, response body is not valid
   */
  async makeRequestAsyncOmit(
    options: RequestOption,
    payload: Binary = '',
    statusCodes: number[] = [200],
    region = '',
  ): Promise<Omit<http.IncomingMessage, 'on'>> {
    const res = await this.makeRequestAsync(options, payload, statusCodes, region)
    await drainResponse(res)
    return res
  }

  /**
   * makeRequestStream will be used directly instead of makeRequest in case the payload
   * is available as a stream. for ex. putObject
   *
   * @internal
   */
  async makeRequestStreamAsync(
    options: RequestOption,
    body: stream.Readable | Binary,
    sha256sum: string,
    statusCodes: number[],
    region: string,
  ): Promise<http.IncomingMessage> {
    if (!isObject(options)) {
      throw new TypeError('options should be of type "object"')
    }
    if (!(Buffer.isBuffer(body) || typeof body === 'string' || isReadableStream(body))) {
      throw new errors.InvalidArgumentError(
        `stream should be a Buffer, string or readable Stream, got ${typeof body} instead`,
      )
    }
    if (!isString(sha256sum)) {
      throw new TypeError('sha256sum should be of type "string"')
    }
    statusCodes.forEach((statusCode) => {
      if (!isNumber(statusCode)) {
        throw new TypeError('statusCode should be of type "number"')
      }
    })
    if (!isString(region)) {
      throw new TypeError('region should be of type "string"')
    }
    // sha256sum will be empty for anonymous or https requests
    if (!this.enableSHA256 && sha256sum.length !== 0) {
      throw new errors.InvalidArgumentError(`sha256sum expected to be empty for anonymous or https requests`)
    }
    // sha256sum should be valid for non-anonymous http requests.
    if (this.enableSHA256 && sha256sum.length !== 64) {
      throw new errors.InvalidArgumentError(`Invalid sha256sum : ${sha256sum}`)
    }

    await this.checkAndRefreshCreds()

    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    region = region || (await this.getBucketRegionAsync(options.bucketName!))

    const reqOptions = this.getRequestOptions({ ...options, region })
    if (!this.anonymous) {
      // For non-anonymous https requests sha256sum is 'UNSIGNED-PAYLOAD' for signature calculation.
      if (!this.enableSHA256) {
        sha256sum = 'UNSIGNED-PAYLOAD'
      }
      const date = new Date()
      reqOptions.headers['x-amz-date'] = makeDateLong(date)
      reqOptions.headers['x-amz-content-sha256'] = sha256sum
      if (this.sessionToken) {
        reqOptions.headers['x-amz-security-token'] = this.sessionToken
      }
      reqOptions.headers.authorization = signV4(reqOptions, this.accessKey, this.secretKey, region, date, sha256sum)
    }

    const response = await request(this.transport, reqOptions, body)
    if (!response.statusCode) {
      throw new Error("BUG: response doesn't have a statusCode")
    }

    if (!statusCodes.includes(response.statusCode)) {
      // For an incorrect region, S3 server always sends back 400.
      // But we will do cache invalidation for all errors so that,
      // in future, if AWS S3 decides to send a different status code or
      // XML error code we will still work fine.
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      delete this.regionMap[options.bucketName!]

      const err = await xmlParsers.parseResponseError(response)
      this.logHTTP(reqOptions, response, err)
      throw err
    }

    this.logHTTP(reqOptions, response)

    return response
  }

  /**
   * gets the region of the bucket
   *
   * @param bucketName
   *
   * @internal
   */
  protected async getBucketRegionAsync(bucketName: string): Promise<string> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name : ${bucketName}`)
    }

    // Region is set with constructor, return the region right here.
    if (this.region) {
      return this.region
    }

    const cached = this.regionMap[bucketName]
    if (cached) {
      return cached
    }

    const extractRegionAsync = async (response: http.IncomingMessage) => {
      const body = await readAsString(response)
      const region = xmlParsers.parseBucketRegion(body) || DEFAULT_REGION
      this.regionMap[bucketName] = region
      return region
    }

    const method = 'GET'
    const query = 'location'
    // `getBucketLocation` behaves differently in following ways for
    // different environments.
    //
    // - For nodejs env we default to path style requests.
    // - For browser env path style requests on buckets yields CORS
    //   error. To circumvent this problem we make a virtual host
    //   style request signed with 'us-east-1'. This request fails
    //   with an error 'AuthorizationHeaderMalformed', additionally
    //   the error XML also provides Region of the bucket. To validate
    //   this region is proper we retry the same request with the newly
    //   obtained region.
    const pathStyle = this.pathStyle && !isBrowser
    let region: string
    try {
      const res = await this.makeRequestAsync({ method, bucketName, query, pathStyle }, '', [200], DEFAULT_REGION)
      return extractRegionAsync(res)
    } catch (e) {
      // make alignment with mc cli
      if (e instanceof errors.S3Error) {
        const errCode = e.code
        const errRegion = e.region
        if (errCode === 'AccessDenied' && !errRegion) {
          return DEFAULT_REGION
        }
      }
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      if (!(e.name === 'AuthorizationHeaderMalformed')) {
        throw e
      }
      // @ts-expect-error we set extra properties on error object
      region = e.Region as string
      if (!region) {
        throw e
      }
    }

    const res = await this.makeRequestAsync({ method, bucketName, query, pathStyle }, '', [200], region)
    return await extractRegionAsync(res)
  }

  /**
   * makeRequest is the primitive used by the apis for making S3 requests.
   * payload can be empty string in case of no payload.
   * statusCode is the expected statusCode. If response.statusCode does not match
   * we parse the XML error and call the callback with the error message.
   * A valid region is passed by the calls - listBuckets, makeBucket and
   * getBucketRegion.
   *
   * @deprecated use `makeRequestAsync` instead
   */
  makeRequest(
    options: RequestOption,
    payload: Binary = '',
    expectedCodes: number[] = [200],
    region = '',
    returnResponse: boolean,
    cb: (cb: unknown, result: http.IncomingMessage) => void,
  ) {
    let prom: Promise<http.IncomingMessage>
    if (returnResponse) {
      prom = this.makeRequestAsync(options, payload, expectedCodes, region)
    } else {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-expect-error compatible for old behaviour
      prom = this.makeRequestAsyncOmit(options, payload, expectedCodes, region)
    }

    prom.then(
      (result) => cb(null, result),
      (err) => {
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        cb(err)
      },
    )
  }

  /**
   * makeRequestStream will be used directly instead of makeRequest in case the payload
   * is available as a stream. for ex. putObject
   *
   * @deprecated use `makeRequestStreamAsync` instead
   */
  makeRequestStream(
    options: RequestOption,
    stream: stream.Readable | Buffer,
    sha256sum: string,
    statusCodes: number[],
    region: string,
    returnResponse: boolean,
    cb: (cb: unknown, result: http.IncomingMessage) => void,
  ) {
    const executor = async () => {
      const res = await this.makeRequestStreamAsync(options, stream, sha256sum, statusCodes, region)
      if (!returnResponse) {
        await drainResponse(res)
      }

      return res
    }

    executor().then(
      (result) => cb(null, result),
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      (err) => cb(err),
    )
  }

  /**
   * @deprecated use `getBucketRegionAsync` instead
   */
  getBucketRegion(bucketName: string, cb: (err: unknown, region: string) => void) {
    return this.getBucketRegionAsync(bucketName).then(
      (result) => cb(null, result),
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      (err) => cb(err),
    )
  }

  // Bucket operations

  /**
   * Creates the bucket `bucketName`.
   *
   */
  async makeBucket(bucketName: string, region: Region = '', makeOpts?: MakeBucketOpt): Promise<void> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    // Backward Compatibility
    if (isObject(region)) {
      makeOpts = region
      region = ''
    }

    if (!isString(region)) {
      throw new TypeError('region should be of type "string"')
    }
    if (makeOpts && !isObject(makeOpts)) {
      throw new TypeError('makeOpts should be of type "object"')
    }

    let payload = ''

    // Region already set in constructor, validate if
    // caller requested bucket location is same.
    if (region && this.region) {
      if (region !== this.region) {
        throw new errors.InvalidArgumentError(`Configured region ${this.region}, requested ${region}`)
      }
    }
    // sending makeBucket request with XML containing 'us-east-1' fails. For
    // default region server expects the request without body
    if (region && region !== DEFAULT_REGION) {
      payload = xml.buildObject({
        CreateBucketConfiguration: {
          $: { xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/' },
          LocationConstraint: region,
        },
      })
    }
    const method = 'PUT'
    const headers: RequestHeaders = {}

    if (makeOpts && makeOpts.ObjectLocking) {
      headers['x-amz-bucket-object-lock-enabled'] = true
    }

    // For custom region clients  default to custom region specified in client constructor
    const finalRegion = this.region || region || DEFAULT_REGION

    const requestOpt: RequestOption = { method, bucketName, headers }

    try {
      await this.makeRequestAsyncOmit(requestOpt, payload, [200], finalRegion)
    } catch (err: unknown) {
      if (region === '' || region === DEFAULT_REGION) {
        if (err instanceof errors.S3Error) {
          const errCode = err.code
          const errRegion = err.region
          if (errCode === 'AuthorizationHeaderMalformed' && errRegion !== '') {
            // Retry with region returned as part of error
            await this.makeRequestAsyncOmit(requestOpt, payload, [200], errCode)
          }
        }
      }
      throw err
    }
  }

  /**
   * To check if a bucket already exists.
   */
  async bucketExists(bucketName: string): Promise<boolean> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    const method = 'HEAD'
    try {
      await this.makeRequestAsyncOmit({ method, bucketName })
    } catch (err) {
      // @ts-ignore
      if (err.code === 'NoSuchBucket' || err.code === 'NotFound') {
        return false
      }
      throw err
    }

    return true
  }

  async removeBucket(bucketName: string): Promise<void>

  /**
   * @deprecated use promise style API
   */
  removeBucket(bucketName: string, callback: NoResultCallback): void

  async removeBucket(bucketName: string): Promise<void> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    const method = 'DELETE'
    await this.makeRequestAsyncOmit({ method, bucketName }, '', [204])
    delete this.regionMap[bucketName]
  }

  /**
   * Callback is called with readable stream of the object content.
   */
  async getObject(bucketName: string, objectName: string, getOpts?: GetObjectOpts): Promise<stream.Readable> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }
    return this.getPartialObject(bucketName, objectName, 0, 0, getOpts)
  }

  /**
   * Callback is called with readable stream of the partial object content.
   * @param bucketName
   * @param objectName
   * @param offset
   * @param length - length of the object that will be read in the stream (optional, if not specified we read the rest of the file from the offset)
   * @param getOpts
   */
  async getPartialObject(
    bucketName: string,
    objectName: string,
    offset: number,
    length = 0,
    getOpts?: GetObjectOpts,
  ): Promise<stream.Readable> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }
    if (!isNumber(offset)) {
      throw new TypeError('offset should be of type "number"')
    }
    if (!isNumber(length)) {
      throw new TypeError('length should be of type "number"')
    }

    let range = ''
    if (offset || length) {
      if (offset) {
        range = `bytes=${+offset}-`
      } else {
        range = 'bytes=0-'
        offset = 0
      }
      if (length) {
        range += `${+length + offset - 1}`
      }
    }

    let query = ''
    let headers: RequestHeaders = {
      ...(range !== '' && { range }),
    }

    if (getOpts) {
      const sseHeaders: Record<string, string> = {
        ...(getOpts.SSECustomerAlgorithm && {
          'X-Amz-Server-Side-Encryption-Customer-Algorithm': getOpts.SSECustomerAlgorithm,
        }),
        ...(getOpts.SSECustomerKey && { 'X-Amz-Server-Side-Encryption-Customer-Key': getOpts.SSECustomerKey }),
        ...(getOpts.SSECustomerKeyMD5 && {
          'X-Amz-Server-Side-Encryption-Customer-Key-MD5': getOpts.SSECustomerKeyMD5,
        }),
      }
      query = qs.stringify(getOpts)
      headers = {
        ...prependXAMZMeta(sseHeaders),
        ...headers,
      }
    }

    const expectedStatusCodes = [200]
    if (range) {
      expectedStatusCodes.push(206)
    }
    const method = 'GET'

    return await this.makeRequestAsync({ method, bucketName, objectName, headers, query }, '', expectedStatusCodes)
  }

  /**
   * download object content to a file.
   * This method will create a temp file named `${filename}.${base64(etag)}.part.minio` when downloading.
   *
   * @param bucketName - name of the bucket
   * @param objectName - name of the object
   * @param filePath - path to which the object data will be written to
   * @param getOpts - Optional object get option
   */
  async fGetObject(bucketName: string, objectName: string, filePath: string, getOpts?: GetObjectOpts): Promise<void> {
    // Input validation.
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }
    if (!isString(filePath)) {
      throw new TypeError('filePath should be of type "string"')
    }

    const downloadToTmpFile = async (): Promise<string> => {
      let partFileStream: stream.Writable
      const objStat = await this.statObject(bucketName, objectName, getOpts)
      const encodedEtag = Buffer.from(objStat.etag).toString('base64')
      const partFile = `${filePath}.${encodedEtag}.part.minio`

      await fsp.mkdir(path.dirname(filePath), { recursive: true })

      let offset = 0
      try {
        const stats = await fsp.stat(partFile)
        if (objStat.size === stats.size) {
          return partFile
        }
        offset = stats.size
        partFileStream = fs.createWriteStream(partFile, { flags: 'a' })
      } catch (e) {
        if (e instanceof Error && (e as unknown as { code: string }).code === 'ENOENT') {
          // file not exist
          partFileStream = fs.createWriteStream(partFile, { flags: 'w' })
        } else {
          // other error, maybe access deny
          throw e
        }
      }

      const downloadStream = await this.getPartialObject(bucketName, objectName, offset, 0, getOpts)

      await streamPromise.pipeline(downloadStream, partFileStream)
      const stats = await fsp.stat(partFile)
      if (stats.size === objStat.size) {
        return partFile
      }

      throw new Error('Size mismatch between downloaded file and the object')
    }

    const partFile = await downloadToTmpFile()
    await fsp.rename(partFile, filePath)
  }

  /**
   * Stat information of the object.
   */
  async statObject(bucketName: string, objectName: string, statOpts?: StatObjectOpts): Promise<BucketItemStat> {
    const statOptDef = statOpts || {}
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }

    if (!isObject(statOptDef)) {
      throw new errors.InvalidArgumentError('statOpts should be of type "object"')
    }

    const query = qs.stringify(statOptDef)
    const method = 'HEAD'
    const res = await this.makeRequestAsyncOmit({ method, bucketName, objectName, query })

    return {
      size: parseInt(res.headers['content-length'] as string),
      metaData: extractMetadata(res.headers as ResponseHeader),
      lastModified: new Date(res.headers['last-modified'] as string),
      versionId: getVersionId(res.headers as ResponseHeader),
      etag: sanitizeETag(res.headers.etag),
    }
  }

  async removeObject(bucketName: string, objectName: string, removeOpts?: RemoveOptions): Promise<void> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }

    if (removeOpts && !isObject(removeOpts)) {
      throw new errors.InvalidArgumentError('removeOpts should be of type "object"')
    }

    const method = 'DELETE'

    const headers: RequestHeaders = {}
    if (removeOpts?.governanceBypass) {
      headers['X-Amz-Bypass-Governance-Retention'] = true
    }
    if (removeOpts?.forceDelete) {
      headers['x-minio-force-delete'] = true
    }

    const queryParams: Record<string, string> = {}
    if (removeOpts?.versionId) {
      queryParams.versionId = `${removeOpts.versionId}`
    }
    const query = qs.stringify(queryParams)

    await this.makeRequestAsyncOmit({ method, bucketName, objectName, headers, query }, '', [200, 204])
  }

  // Calls implemented below are related to multipart.

  listIncompleteUploads(
    bucket: string,
    prefix: string,
    recursive: boolean,
  ): BucketStream<IncompleteUploadedBucketItem> {
    if (prefix === undefined) {
      prefix = ''
    }
    if (recursive === undefined) {
      recursive = false
    }
    if (!isValidBucketName(bucket)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucket)
    }
    if (!isValidPrefix(prefix)) {
      throw new errors.InvalidPrefixError(`Invalid prefix : ${prefix}`)
    }
    if (!isBoolean(recursive)) {
      throw new TypeError('recursive should be of type "boolean"')
    }
    const delimiter = recursive ? '' : '/'
    let keyMarker = ''
    let uploadIdMarker = ''
    const uploads: unknown[] = []
    let ended = false

    // TODO: refactor this with async/await and `stream.Readable.from`
    const readStream = new stream.Readable({ objectMode: true })
    readStream._read = () => {
      // push one upload info per _read()
      if (uploads.length) {
        return readStream.push(uploads.shift())
      }
      if (ended) {
        return readStream.push(null)
      }
      this.listIncompleteUploadsQuery(bucket, prefix, keyMarker, uploadIdMarker, delimiter).then(
        (result) => {
          // eslint-disable-next-line @typescript-eslint/ban-ts-comment
          // @ts-ignore
          result.prefixes.forEach((prefix) => uploads.push(prefix))
          async.eachSeries(
            result.uploads,
            (upload, cb) => {
              // for each incomplete upload add the sizes of its uploaded parts
              // eslint-disable-next-line @typescript-eslint/ban-ts-comment
              // @ts-ignore
              this.listParts(bucket, upload.key, upload.uploadId).then(
                (parts: Part[]) => {
                  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                  // @ts-ignore
                  upload.size = parts.reduce((acc, item) => acc + item.size, 0)
                  uploads.push(upload)
                  cb()
                },
                (err: Error) => cb(err),
              )
            },
            (err) => {
              if (err) {
                readStream.emit('error', err)
                return
              }
              if (result.isTruncated) {
                keyMarker = result.nextKeyMarker
                uploadIdMarker = result.nextUploadIdMarker
              } else {
                ended = true
              }

              // eslint-disable-next-line @typescript-eslint/ban-ts-comment
              // @ts-ignore
              readStream._read()
            },
          )
        },
        (e) => {
          readStream.emit('error', e)
        },
      )
    }
    return readStream
  }

  /**
   * Called by listIncompleteUploads to fetch a batch of incomplete uploads.
   */
  async listIncompleteUploadsQuery(
    bucketName: string,
    prefix: string,
    keyMarker: string,
    uploadIdMarker: string,
    delimiter: string,
  ): Promise<ListMultipartResult> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isString(prefix)) {
      throw new TypeError('prefix should be of type "string"')
    }
    if (!isString(keyMarker)) {
      throw new TypeError('keyMarker should be of type "string"')
    }
    if (!isString(uploadIdMarker)) {
      throw new TypeError('uploadIdMarker should be of type "string"')
    }
    if (!isString(delimiter)) {
      throw new TypeError('delimiter should be of type "string"')
    }
    const queries = []
    queries.push(`prefix=${uriEscape(prefix)}`)
    queries.push(`delimiter=${uriEscape(delimiter)}`)

    if (keyMarker) {
      queries.push(`key-marker=${uriEscape(keyMarker)}`)
    }
    if (uploadIdMarker) {
      queries.push(`upload-id-marker=${uploadIdMarker}`)
    }

    const maxUploads = 1000
    queries.push(`max-uploads=${maxUploads}`)
    queries.sort()
    queries.unshift('uploads')
    let query = ''
    if (queries.length > 0) {
      query = `${queries.join('&')}`
    }
    const method = 'GET'
    const res = await this.makeRequestAsync({ method, bucketName, query })
    const body = await readAsString(res)
    return xmlParsers.parseListMultipart(body)
  }

  /**
   * Initiate a new multipart upload.
   * @internal
   */
  async initiateNewMultipartUpload(bucketName: string, objectName: string, headers: RequestHeaders): Promise<string> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }
    if (!isObject(headers)) {
      throw new errors.InvalidObjectNameError('contentType should be of type "object"')
    }
    const method = 'POST'
    const query = 'uploads'
    const res = await this.makeRequestAsync({ method, bucketName, objectName, query, headers })
    const body = await readAsBuffer(res)
    return parseInitiateMultipart(body.toString())
  }

  /**
   * Internal Method to abort a multipart upload request in case of any errors.
   *
   * @param bucketName - Bucket Name
   * @param objectName - Object Name
   * @param uploadId - id of a multipart upload to cancel during compose object sequence.
   */
  async abortMultipartUpload(bucketName: string, objectName: string, uploadId: string): Promise<void> {
    const method = 'DELETE'
    const query = `uploadId=${uploadId}`

    const requestOptions = { method, bucketName, objectName: objectName, query }
    await this.makeRequestAsyncOmit(requestOptions, '', [204])
  }

  async findUploadId(bucketName: string, objectName: string): Promise<string | undefined> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }

    let latestUpload: ListMultipartResult['uploads'][number] | undefined
    let keyMarker = ''
    let uploadIdMarker = ''
    for (;;) {
      const result = await this.listIncompleteUploadsQuery(bucketName, objectName, keyMarker, uploadIdMarker, '')
      for (const upload of result.uploads) {
        if (upload.key === objectName) {
          if (!latestUpload || upload.initiated.getTime() > latestUpload.initiated.getTime()) {
            latestUpload = upload
          }
        }
      }
      if (result.isTruncated) {
        keyMarker = result.nextKeyMarker
        uploadIdMarker = result.nextUploadIdMarker
        continue
      }

      break
    }
    return latestUpload?.uploadId
  }

  /**
   * this call will aggregate the parts on the server into a single object.
   */
  async completeMultipartUpload(
    bucketName: string,
    objectName: string,
    uploadId: string,
    etags: {
      part: number
      etag?: string
    }[],
  ): Promise<{ etag: string; versionId: string | null }> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }
    if (!isString(uploadId)) {
      throw new TypeError('uploadId should be of type "string"')
    }
    if (!isObject(etags)) {
      throw new TypeError('etags should be of type "Array"')
    }

    if (!uploadId) {
      throw new errors.InvalidArgumentError('uploadId cannot be empty')
    }

    const method = 'POST'
    const query = `uploadId=${uriEscape(uploadId)}`

    const builder = new xml2js.Builder()
    const payload = builder.buildObject({
      CompleteMultipartUpload: {
        $: {
          xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/',
        },
        Part: etags.map((etag) => {
          return {
            PartNumber: etag.part,
            ETag: etag.etag,
          }
        }),
      },
    })

    const res = await this.makeRequestAsync({ method, bucketName, objectName, query }, payload)
    const body = await readAsBuffer(res)
    const result = parseCompleteMultipart(body.toString())
    if (!result) {
      throw new Error('BUG: failed to parse server response')
    }

    if (result.errCode) {
      // Multipart Complete API returns an error XML after a 200 http status
      throw new errors.S3Error(result.errMessage)
    }

    return {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      etag: result.etag as string,
      versionId: getVersionId(res.headers as ResponseHeader),
    }
  }

  /**
   * Get part-info of all parts of an incomplete upload specified by uploadId.
   */
  protected async listParts(bucketName: string, objectName: string, uploadId: string): Promise<UploadedPart[]> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }
    if (!isString(uploadId)) {
      throw new TypeError('uploadId should be of type "string"')
    }
    if (!uploadId) {
      throw new errors.InvalidArgumentError('uploadId cannot be empty')
    }

    const parts: UploadedPart[] = []
    let marker = 0
    let result
    do {
      result = await this.listPartsQuery(bucketName, objectName, uploadId, marker)
      marker = result.marker
      parts.push(...result.parts)
    } while (result.isTruncated)

    return parts
  }

  /**
   * Called by listParts to fetch a batch of part-info
   */
  private async listPartsQuery(bucketName: string, objectName: string, uploadId: string, marker: number) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }
    if (!isString(uploadId)) {
      throw new TypeError('uploadId should be of type "string"')
    }
    if (!isNumber(marker)) {
      throw new TypeError('marker should be of type "number"')
    }
    if (!uploadId) {
      throw new errors.InvalidArgumentError('uploadId cannot be empty')
    }

    let query = `uploadId=${uriEscape(uploadId)}`
    if (marker) {
      query += `&part-number-marker=${marker}`
    }

    const method = 'GET'
    const res = await this.makeRequestAsync({ method, bucketName, objectName, query })
    return xmlParsers.parseListParts(await readAsString(res))
  }

  async listBuckets(): Promise<BucketItemFromList[]> {
    const method = 'GET'
    const regionConf = this.region || DEFAULT_REGION
    const httpRes = await this.makeRequestAsync({ method }, '', [200], regionConf)
    const xmlResult = await readAsString(httpRes)
    return xmlParsers.parseListBucket(xmlResult)
  }

  /**
   * Calculate part size given the object size. Part size will be atleast this.partSize
   */
  calculatePartSize(size: number) {
    if (!isNumber(size)) {
      throw new TypeError('size should be of type "number"')
    }
    if (size > this.maxObjectSize) {
      throw new TypeError(`size should not be more than ${this.maxObjectSize}`)
    }
    if (this.overRidePartSize) {
      return this.partSize
    }
    let partSize = this.partSize
    for (;;) {
      // while(true) {...} throws linting error.
      // If partSize is big enough to accomodate the object size, then use it.
      if (partSize * 10000 > size) {
        return partSize
      }
      // Try part sizes as 64MB, 80MB, 96MB etc.
      partSize += 16 * 1024 * 1024
    }
  }

  /**
   * Uploads the object using contents from a file
   */
  async fPutObject(bucketName: string, objectName: string, filePath: string, metaData?: ObjectMetaData) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }

    if (!isString(filePath)) {
      throw new TypeError('filePath should be of type "string"')
    }
    if (metaData && !isObject(metaData)) {
      throw new TypeError('metaData should be of type "object"')
    }

    // Inserts correct `content-type` attribute based on metaData and filePath
    metaData = insertContentType(metaData || {}, filePath)
    const stat = await fsp.lstat(filePath)
    return await this.putObject(bucketName, objectName, fs.createReadStream(filePath), stat.size, metaData)
  }

  /**
   *  Uploading a stream, "Buffer" or "string".
   *  It's recommended to pass `size` argument with stream.
   */
  async putObject(
    bucketName: string,
    objectName: string,
    stream: stream.Readable | Buffer | string,
    size?: number,
    metaData?: ItemBucketMetadata,
  ): Promise<UploadedObjectInfo> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }

    // We'll need to shift arguments to the left because of metaData
    // and size being optional.
    if (isObject(size)) {
      metaData = size
    }
    // Ensures Metadata has appropriate prefix for A3 API
    const headers = prependXAMZMeta(metaData)
    if (typeof stream === 'string' || stream instanceof Buffer) {
      // Adapts the non-stream interface into a stream.
      size = stream.length
      stream = readableStream(stream)
    } else if (!isReadableStream(stream)) {
      throw new TypeError('third argument should be of type "stream.Readable" or "Buffer" or "string"')
    }

    if (isNumber(size) && size < 0) {
      throw new errors.InvalidArgumentError(`size cannot be negative, given size: ${size}`)
    }

    // Get the part size and forward that to the BlockStream. Default to the
    // largest block size possible if necessary.
    if (!isNumber(size)) {
      size = this.maxObjectSize
    }

    // Get the part size and forward that to the BlockStream. Default to the
    // largest block size possible if necessary.
    if (size === undefined) {
      const statSize = await getContentLength(stream)
      if (statSize !== null) {
        size = statSize
      }
    }

    if (!isNumber(size)) {
      // Backward compatibility
      size = this.maxObjectSize
    }

    const partSize = this.calculatePartSize(size)
    if (typeof stream === 'string' || Buffer.isBuffer(stream) || size <= partSize) {
      const buf = isReadableStream(stream) ? await readAsBuffer(stream) : Buffer.from(stream)
      return this.uploadBuffer(bucketName, objectName, headers, buf)
    }

    return this.uploadStream(bucketName, objectName, headers, stream, partSize)
  }

  /**
   * method to upload buffer in one call
   * @private
   */
  private async uploadBuffer(
    bucketName: string,
    objectName: string,
    headers: RequestHeaders,
    buf: Buffer,
  ): Promise<UploadedObjectInfo> {
    const { md5sum, sha256sum } = hashBinary(buf, this.enableSHA256)
    headers['Content-Length'] = buf.length
    if (!this.enableSHA256) {
      headers['Content-MD5'] = md5sum
    }
    const res = await this.makeRequestStreamAsync(
      {
        method: 'PUT',
        bucketName,
        objectName,
        headers,
      },
      buf,
      sha256sum,
      [200],
      '',
    )
    await drainResponse(res)
    return {
      etag: sanitizeETag(res.headers.etag),
      versionId: getVersionId(res.headers as ResponseHeader),
    }
  }

  /**
   * upload stream with MultipartUpload
   * @private
   */
  private async uploadStream(
    bucketName: string,
    objectName: string,
    headers: RequestHeaders,
    body: stream.Readable,
    partSize: number,
  ): Promise<UploadedObjectInfo> {
    // A map of the previously uploaded chunks, for resuming a file upload. This
    // will be null if we aren't resuming an upload.
    const oldParts: Record<number, Part> = {}

    // Keep track of the etags for aggregating the chunks together later. Each
    // etag represents a single chunk of the file.
    const eTags: Part[] = []

    const previousUploadId = await this.findUploadId(bucketName, objectName)
    let uploadId: string
    if (!previousUploadId) {
      uploadId = await this.initiateNewMultipartUpload(bucketName, objectName, headers)
    } else {
      uploadId = previousUploadId
      const oldTags = await this.listParts(bucketName, objectName, previousUploadId)
      oldTags.forEach((e) => {
        oldParts[e.part] = e
      })
    }

    const chunkier = new BlockStream2({ size: partSize, zeroPadding: false })

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [_, o] = await Promise.all([
      new Promise((resolve, reject) => {
        body.pipe(chunkier).on('error', reject)
        chunkier.on('end', resolve).on('error', reject)
      }),
      (async () => {
        let partNumber = 1

        for await (const chunk of chunkier) {
          const md5 = crypto.createHash('md5').update(chunk).digest()

          const oldPart = oldParts[partNumber]
          if (oldPart) {
            if (oldPart.etag === md5.toString('hex')) {
              eTags.push({ part: partNumber, etag: oldPart.etag })
              partNumber++
              continue
            }
          }

          partNumber++

          // now start to upload missing part
          const options: RequestOption = {
            method: 'PUT',
            query: qs.stringify({ partNumber, uploadId }),
            headers: {
              'Content-Length': chunk.length,
              'Content-MD5': md5.toString('base64'),
            },
            bucketName,
            objectName,
          }

          const response = await this.makeRequestAsyncOmit(options, chunk)

          let etag = response.headers.etag
          if (etag) {
            etag = etag.replace(/^"/, '').replace(/"$/, '')
          } else {
            etag = ''
          }

          eTags.push({ part: partNumber, etag })
        }

        return await this.completeMultipartUpload(bucketName, objectName, uploadId, eTags)
      })(),
    ])

    return o
  }

  async removeBucketReplication(bucketName: string): Promise<void>
  removeBucketReplication(bucketName: string, callback: NoResultCallback): void
  async removeBucketReplication(bucketName: string): Promise<void> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    const method = 'DELETE'
    const query = 'replication'
    await this.makeRequestAsyncOmit({ method, bucketName, query }, '', [200, 204], '')
  }

  setBucketReplication(bucketName: string, replicationConfig: ReplicationConfigOpts): void
  async setBucketReplication(bucketName: string, replicationConfig: ReplicationConfigOpts): Promise<void>
  async setBucketReplication(bucketName: string, replicationConfig: ReplicationConfigOpts) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isObject(replicationConfig)) {
      throw new errors.InvalidArgumentError('replicationConfig should be of type "object"')
    } else {
      if (_.isEmpty(replicationConfig.role)) {
        throw new errors.InvalidArgumentError('Role cannot be empty')
      } else if (replicationConfig.role && !isString(replicationConfig.role)) {
        throw new errors.InvalidArgumentError('Invalid value for role', replicationConfig.role)
      }
      if (_.isEmpty(replicationConfig.rules)) {
        throw new errors.InvalidArgumentError('Minimum one replication rule must be specified')
      }
    }
    const method = 'PUT'
    const query = 'replication'
    const headers: Record<string, string> = {}

    const replicationParamsConfig = {
      ReplicationConfiguration: {
        Role: replicationConfig.role,
        Rule: replicationConfig.rules,
      },
    }

    const builder = new xml2js.Builder({ renderOpts: { pretty: false }, headless: true })
    const payload = builder.buildObject(replicationParamsConfig)
    headers['Content-MD5'] = toMd5(payload)
    await this.makeRequestAsyncOmit({ method, bucketName, query, headers }, payload)
  }

  getBucketReplication(bucketName: string): void
  async getBucketReplication(bucketName: string): Promise<ReplicationConfig>
  async getBucketReplication(bucketName: string) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    const method = 'GET'
    const query = 'replication'

    const httpRes = await this.makeRequestAsync({ method, bucketName, query }, '', [200, 204])
    const xmlResult = await readAsString(httpRes)
    return xmlParsers.parseReplicationConfig(xmlResult)
  }

  getObjectLegalHold(
    bucketName: string,
    objectName: string,
    getOpts?: GetObjectLegalHoldOptions,
    callback?: ResultCallback<LEGAL_HOLD_STATUS>,
  ): Promise<LEGAL_HOLD_STATUS>
  async getObjectLegalHold(
    bucketName: string,
    objectName: string,
    getOpts?: GetObjectLegalHoldOptions,
  ): Promise<LEGAL_HOLD_STATUS> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }

    if (getOpts) {
      if (!isObject(getOpts)) {
        throw new TypeError('getOpts should be of type "Object"')
      } else if (Object.keys(getOpts).length > 0 && getOpts.versionId && !isString(getOpts.versionId)) {
        throw new TypeError('versionId should be of type string.:', getOpts.versionId)
      }
    }

    const method = 'GET'
    let query = 'legal-hold'

    if (getOpts?.versionId) {
      query += `&versionId=${getOpts.versionId}`
    }

    const httpRes = await this.makeRequestAsync({ method, bucketName, objectName, query }, '', [200])
    const strRes = await readAsString(httpRes)
    return parseObjectLegalHoldConfig(strRes)
  }

  setObjectLegalHold(bucketName: string, objectName: string, setOpts?: PutObjectLegalHoldOptions): void
  async setObjectLegalHold(
    bucketName: string,
    objectName: string,
    setOpts = {
      status: LEGAL_HOLD_STATUS.ENABLED,
    } as PutObjectLegalHoldOptions,
  ): Promise<void> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }

    if (!isObject(setOpts)) {
      throw new TypeError('setOpts should be of type "Object"')
    } else {
      if (![LEGAL_HOLD_STATUS.ENABLED, LEGAL_HOLD_STATUS.DISABLED].includes(setOpts?.status)) {
        throw new TypeError('Invalid status: ' + setOpts.status)
      }
      if (setOpts.versionId && !setOpts.versionId.length) {
        throw new TypeError('versionId should be of type string.:' + setOpts.versionId)
      }
    }

    const method = 'PUT'
    let query = 'legal-hold'

    if (setOpts.versionId) {
      query += `&versionId=${setOpts.versionId}`
    }

    const config = {
      Status: setOpts.status,
    }

    const builder = new xml2js.Builder({ rootName: 'LegalHold', renderOpts: { pretty: false }, headless: true })
    const payload = builder.buildObject(config)
    const headers: Record<string, string> = {}
    headers['Content-MD5'] = toMd5(payload)

    await this.makeRequestAsyncOmit({ method, bucketName, objectName, query, headers }, payload)
  }

  /**
   * Get Tags associated with a Bucket
   */
  async getBucketTagging(bucketName: string): Promise<Tag[]> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`)
    }

    const method = 'GET'
    const query = 'tagging'
    const requestOptions = { method, bucketName, query }

    const response = await this.makeRequestAsync(requestOptions)
    const body = await readAsString(response)
    return xmlParsers.parseTagging(body)
  }

  /**
   *  Get the tags associated with a bucket OR an object
   */
  async getObjectTagging(bucketName: string, objectName: string, getOpts?: GetObjectOpts): Promise<Tag[]> {
    const method = 'GET'
    let query = 'tagging'

    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidBucketNameError('Invalid object name: ' + objectName)
    }
    if (getOpts && !isObject(getOpts)) {
      throw new errors.InvalidArgumentError('getOpts should be of type "object"')
    }

    if (getOpts && getOpts.versionId) {
      query = `${query}&versionId=${getOpts.versionId}`
    }
    const requestOptions: RequestOption = { method, bucketName, query }
    if (objectName) {
      requestOptions['objectName'] = objectName
    }

    const response = await this.makeRequestAsync(requestOptions)
    const body = await readAsString(response)
    return xmlParsers.parseTagging(body)
  }

  /**
   *  Set the policy on a bucket or an object prefix.
   */
  async setBucketPolicy(bucketName: string, policy: string): Promise<void> {
    // Validate arguments.
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`)
    }
    if (!isString(policy)) {
      throw new errors.InvalidBucketPolicyError(`Invalid bucket policy: ${policy} - must be "string"`)
    }

    const query = 'policy'

    let method = 'DELETE'
    if (policy) {
      method = 'PUT'
    }

    await this.makeRequestAsyncOmit({ method, bucketName, query }, policy, [204], '')
  }

  /**
   * Get the policy on a bucket or an object prefix.
   */
  async getBucketPolicy(bucketName: string): Promise<string> {
    // Validate arguments.
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`)
    }

    const method = 'GET'
    const query = 'policy'
    const res = await this.makeRequestAsync({ method, bucketName, query })
    return await readAsString(res)
  }

  async putObjectRetention(bucketName: string, objectName: string, retentionOpts: Retention = {}): Promise<void> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }
    if (!isObject(retentionOpts)) {
      throw new errors.InvalidArgumentError('retentionOpts should be of type "object"')
    } else {
      if (retentionOpts.governanceBypass && !isBoolean(retentionOpts.governanceBypass)) {
        throw new errors.InvalidArgumentError(`Invalid value for governanceBypass: ${retentionOpts.governanceBypass}`)
      }
      if (
        retentionOpts.mode &&
        ![RETENTION_MODES.COMPLIANCE, RETENTION_MODES.GOVERNANCE].includes(retentionOpts.mode)
      ) {
        throw new errors.InvalidArgumentError(`Invalid object retention mode: ${retentionOpts.mode}`)
      }
      if (retentionOpts.retainUntilDate && !isString(retentionOpts.retainUntilDate)) {
        throw new errors.InvalidArgumentError(`Invalid value for retainUntilDate: ${retentionOpts.retainUntilDate}`)
      }
      if (retentionOpts.versionId && !isString(retentionOpts.versionId)) {
        throw new errors.InvalidArgumentError(`Invalid value for versionId: ${retentionOpts.versionId}`)
      }
    }

    const method = 'PUT'
    let query = 'retention'

    const headers: RequestHeaders = {}
    if (retentionOpts.governanceBypass) {
      headers['X-Amz-Bypass-Governance-Retention'] = true
    }

    const builder = new xml2js.Builder({ rootName: 'Retention', renderOpts: { pretty: false }, headless: true })
    const params: Record<string, string> = {}

    if (retentionOpts.mode) {
      params.Mode = retentionOpts.mode
    }
    if (retentionOpts.retainUntilDate) {
      params.RetainUntilDate = retentionOpts.retainUntilDate
    }
    if (retentionOpts.versionId) {
      query += `&versionId=${retentionOpts.versionId}`
    }

    const payload = builder.buildObject(params)

    headers['Content-MD5'] = toMd5(payload)
    await this.makeRequestAsyncOmit({ method, bucketName, objectName, query, headers }, payload, [200, 204])
  }

  getObjectLockConfig(bucketName: string, callback: ResultCallback<ObjectLockInfo>): void
  getObjectLockConfig(bucketName: string): void
  async getObjectLockConfig(bucketName: string): Promise<ObjectLockInfo>
  async getObjectLockConfig(bucketName: string) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    const method = 'GET'
    const query = 'object-lock'

    const httpRes = await this.makeRequestAsync({ method, bucketName, query })
    const xmlResult = await readAsString(httpRes)
    return xmlParsers.parseObjectLockConfig(xmlResult)
  }

  setObjectLockConfig(bucketName: string, lockConfigOpts: Omit<ObjectLockInfo, 'objectLockEnabled'>): void
  async setObjectLockConfig(
    bucketName: string,
    lockConfigOpts: Omit<ObjectLockInfo, 'objectLockEnabled'>,
  ): Promise<void>
  async setObjectLockConfig(bucketName: string, lockConfigOpts: Omit<ObjectLockInfo, 'objectLockEnabled'>) {
    const retentionModes = [RETENTION_MODES.COMPLIANCE, RETENTION_MODES.GOVERNANCE]
    const validUnits = [RETENTION_VALIDITY_UNITS.DAYS, RETENTION_VALIDITY_UNITS.YEARS]

    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }

    if (lockConfigOpts.mode && !retentionModes.includes(lockConfigOpts.mode)) {
      throw new TypeError(`lockConfigOpts.mode should be one of ${retentionModes}`)
    }
    if (lockConfigOpts.unit && !validUnits.includes(lockConfigOpts.unit)) {
      throw new TypeError(`lockConfigOpts.unit should be one of ${validUnits}`)
    }
    if (lockConfigOpts.validity && !isNumber(lockConfigOpts.validity)) {
      throw new TypeError(`lockConfigOpts.validity should be a number`)
    }

    const method = 'PUT'
    const query = 'object-lock'

    const config: ObjectLockConfigParam = {
      ObjectLockEnabled: 'Enabled',
    }
    const configKeys = Object.keys(lockConfigOpts)

    const isAllKeysSet = ['unit', 'mode', 'validity'].every((lck) => configKeys.includes(lck))
    // Check if keys are present and all keys are present.
    if (configKeys.length > 0) {
      if (!isAllKeysSet) {
        throw new TypeError(
          `lockConfigOpts.mode,lockConfigOpts.unit,lockConfigOpts.validity all the properties should be specified.`,
        )
      } else {
        config.Rule = {
          DefaultRetention: {},
        }
        if (lockConfigOpts.mode) {
          config.Rule.DefaultRetention.Mode = lockConfigOpts.mode
        }
        if (lockConfigOpts.unit === RETENTION_VALIDITY_UNITS.DAYS) {
          config.Rule.DefaultRetention.Days = lockConfigOpts.validity
        } else if (lockConfigOpts.unit === RETENTION_VALIDITY_UNITS.YEARS) {
          config.Rule.DefaultRetention.Years = lockConfigOpts.validity
        }
      }
    }

    const builder = new xml2js.Builder({
      rootName: 'ObjectLockConfiguration',
      renderOpts: { pretty: false },
      headless: true,
    })
    const payload = builder.buildObject(config)

    const headers: RequestHeaders = {}
    headers['Content-MD5'] = toMd5(payload)

    await this.makeRequestAsyncOmit({ method, bucketName, query, headers }, payload)
  }

  async getBucketVersioning(bucketName: string): Promise<BucketVersioningConfiguration> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    const method = 'GET'
    const query = 'versioning'

    const httpRes = await this.makeRequestAsync({ method, bucketName, query })
    const xmlResult = await readAsString(httpRes)
    return await xmlParsers.parseBucketVersioningConfig(xmlResult)
  }

  async setBucketVersioning(bucketName: string, versionConfig: BucketVersioningConfiguration): Promise<void> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!Object.keys(versionConfig).length) {
      throw new errors.InvalidArgumentError('versionConfig should be of type "object"')
    }

    const method = 'PUT'
    const query = 'versioning'
    const builder = new xml2js.Builder({
      rootName: 'VersioningConfiguration',
      renderOpts: { pretty: false },
      headless: true,
    })
    const payload = builder.buildObject(versionConfig)

    await this.makeRequestAsyncOmit({ method, bucketName, query }, payload)
  }

  private async setTagging(taggingParams: PutTaggingParams): Promise<void> {
    const { bucketName, objectName, tags, putOpts } = taggingParams
    const method = 'PUT'
    let query = 'tagging'

    if (putOpts && putOpts?.versionId) {
      query = `${query}&versionId=${putOpts.versionId}`
    }
    const tagsList = []
    for (const [key, value] of Object.entries(tags)) {
      tagsList.push({ Key: key, Value: value })
    }
    const taggingConfig = {
      Tagging: {
        TagSet: {
          Tag: tagsList,
        },
      },
    }
    const headers = {} as RequestHeaders
    const builder = new xml2js.Builder({ headless: true, renderOpts: { pretty: false } })
    const payloadBuf = Buffer.from(builder.buildObject(taggingConfig))
    const requestOptions = {
      method,
      bucketName,
      query,
      headers,

      ...(objectName && { objectName: objectName }),
    }

    headers['Content-MD5'] = toMd5(payloadBuf)

    await this.makeRequestAsyncOmit(requestOptions, payloadBuf)
  }

  private async removeTagging({ bucketName, objectName, removeOpts }: RemoveTaggingParams): Promise<void> {
    const method = 'DELETE'
    let query = 'tagging'

    if (removeOpts && Object.keys(removeOpts).length && removeOpts.versionId) {
      query = `${query}&versionId=${removeOpts.versionId}`
    }
    const requestOptions = { method, bucketName, objectName, query }

    if (objectName) {
      requestOptions['objectName'] = objectName
    }
    await this.makeRequestAsync(requestOptions, '', [200, 204])
  }

  async setBucketTagging(bucketName: string, tags: Tags): Promise<void> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isObject(tags)) {
      throw new errors.InvalidArgumentError('tags should be of type "object"')
    }
    if (Object.keys(tags).length > 10) {
      throw new errors.InvalidArgumentError('maximum tags allowed is 10"')
    }

    await this.setTagging({ bucketName, tags })
  }

  async removeBucketTagging(bucketName: string) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    await this.removeTagging({ bucketName })
  }

  async setObjectTagging(bucketName: string, objectName: string, tags: Tags, putOpts?: TaggingOpts) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidBucketNameError('Invalid object name: ' + objectName)
    }

    if (!isObject(tags)) {
      throw new errors.InvalidArgumentError('tags should be of type "object"')
    }
    if (Object.keys(tags).length > 10) {
      throw new errors.InvalidArgumentError('Maximum tags allowed is 10"')
    }

    await this.setTagging({ bucketName, objectName, tags, putOpts })
  }

  async removeObjectTagging(bucketName: string, objectName: string, removeOpts: TaggingOpts) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidBucketNameError('Invalid object name: ' + objectName)
    }
    if (removeOpts && Object.keys(removeOpts).length && !isObject(removeOpts)) {
      throw new errors.InvalidArgumentError('removeOpts should be of type "object"')
    }

    await this.removeTagging({ bucketName, objectName, removeOpts })
  }

  async selectObjectContent(
    bucketName: string,
    objectName: string,
    selectOpts: SelectOptions,
  ): Promise<SelectResults | undefined> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }
    if (!_.isEmpty(selectOpts)) {
      if (!isString(selectOpts.expression)) {
        throw new TypeError('sqlExpression should be of type "string"')
      }
      if (!_.isEmpty(selectOpts.inputSerialization)) {
        if (!isObject(selectOpts.inputSerialization)) {
          throw new TypeError('inputSerialization should be of type "object"')
        }
      } else {
        throw new TypeError('inputSerialization is required')
      }
      if (!_.isEmpty(selectOpts.outputSerialization)) {
        if (!isObject(selectOpts.outputSerialization)) {
          throw new TypeError('outputSerialization should be of type "object"')
        }
      } else {
        throw new TypeError('outputSerialization is required')
      }
    } else {
      throw new TypeError('valid select configuration is required')
    }

    const method = 'POST'
    const query = `select&select-type=2`

    const config: Record<string, unknown>[] = [
      {
        Expression: selectOpts.expression,
      },
      {
        ExpressionType: selectOpts.expressionType || 'SQL',
      },
      {
        InputSerialization: [selectOpts.inputSerialization],
      },
      {
        OutputSerialization: [selectOpts.outputSerialization],
      },
    ]

    // Optional
    if (selectOpts.requestProgress) {
      config.push({ RequestProgress: selectOpts?.requestProgress })
    }
    // Optional
    if (selectOpts.scanRange) {
      config.push({ ScanRange: selectOpts.scanRange })
    }

    const builder = new xml2js.Builder({
      rootName: 'SelectObjectContentRequest',
      renderOpts: { pretty: false },
      headless: true,
    })
    const payload = builder.buildObject(config)

    const res = await this.makeRequestAsync({ method, bucketName, objectName, query }, payload)
    const body = await readAsBuffer(res)
    return parseSelectObjectContentResponse(body)
  }

  private async applyBucketLifecycle(bucketName: string, policyConfig: LifeCycleConfigParam): Promise<void> {
    const method = 'PUT'
    const query = 'lifecycle'

    const headers: RequestHeaders = {}
    const builder = new xml2js.Builder({
      rootName: 'LifecycleConfiguration',
      headless: true,
      renderOpts: { pretty: false },
    })
    const payload = builder.buildObject(policyConfig)
    headers['Content-MD5'] = toMd5(payload)

    await this.makeRequestAsyncOmit({ method, bucketName, query, headers }, payload)
  }

  async removeBucketLifecycle(bucketName: string): Promise<void> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    const method = 'DELETE'
    const query = 'lifecycle'
    await this.makeRequestAsyncOmit({ method, bucketName, query }, '', [204])
  }

  async setBucketLifecycle(bucketName: string, lifeCycleConfig: LifeCycleConfigParam): Promise<void> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (_.isEmpty(lifeCycleConfig)) {
      await this.removeBucketLifecycle(bucketName)
    } else {
      await this.applyBucketLifecycle(bucketName, lifeCycleConfig)
    }
  }

  async getBucketLifecycle(bucketName: string): Promise<LifecycleConfig | null> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    const method = 'GET'
    const query = 'lifecycle'

    const res = await this.makeRequestAsync({ method, bucketName, query })
    const body = await readAsString(res)
    return xmlParsers.parseLifecycleConfig(body)
  }

  async setBucketEncryption(bucketName: string, encryptionConfig?: EncryptionConfig): Promise<void> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!_.isEmpty(encryptionConfig) && encryptionConfig.Rule.length > 1) {
      throw new errors.InvalidArgumentError('Invalid Rule length. Only one rule is allowed.: ' + encryptionConfig.Rule)
    }

    let encryptionObj = encryptionConfig
    if (_.isEmpty(encryptionConfig)) {
      encryptionObj = {
        // Default MinIO Server Supported Rule
        Rule: [
          {
            ApplyServerSideEncryptionByDefault: {
              SSEAlgorithm: 'AES256',
            },
          },
        ],
      }
    }

    const method = 'PUT'
    const query = 'encryption'
    const builder = new xml2js.Builder({
      rootName: 'ServerSideEncryptionConfiguration',
      renderOpts: { pretty: false },
      headless: true,
    })
    const payload = builder.buildObject(encryptionObj)

    const headers: RequestHeaders = {}
    headers['Content-MD5'] = toMd5(payload)

    await this.makeRequestAsyncOmit({ method, bucketName, query, headers }, payload)
  }

  async getBucketEncryption(bucketName: string) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    const method = 'GET'
    const query = 'encryption'

    const res = await this.makeRequestAsync({ method, bucketName, query })
    const body = await readAsString(res)
    return xmlParsers.parseBucketEncryptionConfig(body)
  }

  async removeBucketEncryption(bucketName: string) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    const method = 'DELETE'
    const query = 'encryption'

    await this.makeRequestAsyncOmit({ method, bucketName, query }, '', [204])
  }

  async getObjectRetention(
    bucketName: string,
    objectName: string,
    getOpts?: GetObjectRetentionOpts,
  ): Promise<ObjectRetentionInfo | null | undefined> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }
    if (getOpts && !isObject(getOpts)) {
      throw new errors.InvalidArgumentError('getOpts should be of type "object"')
    } else if (getOpts?.versionId && !isString(getOpts.versionId)) {
      throw new errors.InvalidArgumentError('versionId should be of type "string"')
    }

    const method = 'GET'
    let query = 'retention'
    if (getOpts?.versionId) {
      query += `&versionId=${getOpts.versionId}`
    }
    const res = await this.makeRequestAsync({ method, bucketName, objectName, query })
    const body = await readAsString(res)
    return xmlParsers.parseObjectRetentionConfig(body)
  }

  async removeObjects(bucketName: string, objectsList: RemoveObjectsParam): Promise<RemoveObjectsResponse[]> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!Array.isArray(objectsList)) {
      throw new errors.InvalidArgumentError('objectsList should be a list')
    }

    const runDeleteObjects = async (batch: RemoveObjectsParam): Promise<RemoveObjectsResponse[]> => {
      const delObjects: RemoveObjectsRequestEntry[] = batch.map((value) => {
        return isObject(value) ? { Key: value.name, VersionId: value.versionId } : { Key: value }
      })

      const remObjects = { Delete: { Quiet: true, Object: delObjects } }
      const payload = Buffer.from(new xml2js.Builder({ headless: true }).buildObject(remObjects))
      const headers: RequestHeaders = { 'Content-MD5': toMd5(payload) }

      const res = await this.makeRequestAsync({ method: 'POST', bucketName, query: 'delete', headers }, payload)
      const body = await readAsString(res)
      return xmlParsers.removeObjectsParser(body)
    }

    const maxEntries = 1000 // max entries accepted in server for DeleteMultipleObjects API.
    // Client side batching
    const batches = []
    for (let i = 0; i < objectsList.length; i += maxEntries) {
      batches.push(objectsList.slice(i, i + maxEntries))
    }

    const batchResults = await Promise.all(batches.map(runDeleteObjects))
    return batchResults.flat()
  }

  async removeIncompleteUpload(bucketName: string, objectName: string): Promise<void> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.IsValidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }
    const removeUploadId = await this.findUploadId(bucketName, objectName)
    const method = 'DELETE'
    const query = `uploadId=${removeUploadId}`
    await this.makeRequestAsyncOmit({ method, bucketName, objectName, query }, '', [204])
  }

  private async copyObjectV1(
    targetBucketName: string,
    targetObjectName: string,
    sourceBucketNameAndObjectName: string,
    conditions?: null | CopyConditions,
  ) {
    if (typeof conditions == 'function') {
      conditions = null
    }

    if (!isValidBucketName(targetBucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + targetBucketName)
    }
    if (!isValidObjectName(targetObjectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${targetObjectName}`)
    }
    if (!isString(sourceBucketNameAndObjectName)) {
      throw new TypeError('sourceBucketNameAndObjectName should be of type "string"')
    }
    if (sourceBucketNameAndObjectName === '') {
      throw new errors.InvalidPrefixError(`Empty source prefix`)
    }

    if (conditions != null && !(conditions instanceof CopyConditions)) {
      throw new TypeError('conditions should be of type "CopyConditions"')
    }

    const headers: RequestHeaders = {}
    headers['x-amz-copy-source'] = uriResourceEscape(sourceBucketNameAndObjectName)

    if (conditions) {
      if (conditions.modified !== '') {
        headers['x-amz-copy-source-if-modified-since'] = conditions.modified
      }
      if (conditions.unmodified !== '') {
        headers['x-amz-copy-source-if-unmodified-since'] = conditions.unmodified
      }
      if (conditions.matchETag !== '') {
        headers['x-amz-copy-source-if-match'] = conditions.matchETag
      }
      if (conditions.matchETagExcept !== '') {
        headers['x-amz-copy-source-if-none-match'] = conditions.matchETagExcept
      }
    }

    const method = 'PUT'

    const res = await this.makeRequestAsync({
      method,
      bucketName: targetBucketName,
      objectName: targetObjectName,
      headers,
    })
    const body = await readAsString(res)
    return xmlParsers.parseCopyObject(body)
  }

  private async copyObjectV2(
    sourceConfig: CopySourceOptions,
    destConfig: CopyDestinationOptions,
  ): Promise<CopyObjectResultV2> {
    if (!(sourceConfig instanceof CopySourceOptions)) {
      throw new errors.InvalidArgumentError('sourceConfig should of type CopySourceOptions ')
    }
    if (!(destConfig instanceof CopyDestinationOptions)) {
      throw new errors.InvalidArgumentError('destConfig should of type CopyDestinationOptions ')
    }
    if (!destConfig.validate()) {
      return Promise.reject()
    }
    if (!destConfig.validate()) {
      return Promise.reject()
    }

    const headers = Object.assign({}, sourceConfig.getHeaders(), destConfig.getHeaders())

    const bucketName = destConfig.Bucket
    const objectName = destConfig.Object

    const method = 'PUT'

    const res = await this.makeRequestAsync({ method, bucketName, objectName, headers })
    const body = await readAsString(res)
    const copyRes = xmlParsers.parseCopyObject(body)
    const resHeaders: IncomingHttpHeaders = res.headers

    const sizeHeaderValue = resHeaders && resHeaders['content-length']
    const size = typeof sizeHeaderValue === 'number' ? sizeHeaderValue : undefined

    return {
      Bucket: destConfig.Bucket,
      Key: destConfig.Object,
      LastModified: copyRes.lastModified,
      MetaData: extractMetadata(resHeaders as ResponseHeader),
      VersionId: getVersionId(resHeaders as ResponseHeader),
      SourceVersionId: getSourceVersionId(resHeaders as ResponseHeader),
      Etag: sanitizeETag(resHeaders.etag),
      Size: size,
    }
  }

  async copyObject(source: CopySourceOptions, dest: CopyDestinationOptions): Promise<CopyObjectResult>
  async copyObject(
    targetBucketName: string,
    targetObjectName: string,
    sourceBucketNameAndObjectName: string,
    conditions?: CopyConditions,
  ): Promise<CopyObjectResult>
  async copyObject(...allArgs: CopyObjectParams): Promise<CopyObjectResult> {
    if (typeof allArgs[0] === 'string') {
      const [targetBucketName, targetObjectName, sourceBucketNameAndObjectName, conditions] = allArgs as [
        string,
        string,
        string,
        CopyConditions?,
      ]
      return await this.copyObjectV1(targetBucketName, targetObjectName, sourceBucketNameAndObjectName, conditions)
    }
    const [source, dest] = allArgs as [CopySourceOptions, CopyDestinationOptions]
    return await this.copyObjectV2(source, dest)
  }

  async uploadPart(
    partConfig: {
      bucketName: string
      objectName: string
      uploadID: string
      partNumber: number
      headers: RequestHeaders
    },
    payload?: Binary,
  ) {
    const { bucketName, objectName, uploadID, partNumber, headers } = partConfig

    const method = 'PUT'
    const query = `uploadId=${uploadID}&partNumber=${partNumber}`
    const requestOptions = { method, bucketName, objectName: objectName, query, headers }
    const res = await this.makeRequestAsync(requestOptions, payload)
    const body = await readAsString(res)
    const partRes = uploadPartParser(body)
    return {
      etag: sanitizeETag(partRes.ETag),
      key: objectName,
      part: partNumber,
    }
  }

  async composeObject(
    destObjConfig: CopyDestinationOptions,
    sourceObjList: CopySourceOptions[],
  ): Promise<boolean | { etag: string; versionId: string | null } | Promise<void> | CopyObjectResult> {
    const sourceFilesLength = sourceObjList.length

    if (!Array.isArray(sourceObjList)) {
      throw new errors.InvalidArgumentError('sourceConfig should an array of CopySourceOptions ')
    }
    if (!(destObjConfig instanceof CopyDestinationOptions)) {
      throw new errors.InvalidArgumentError('destConfig should of type CopyDestinationOptions ')
    }

    if (sourceFilesLength < 1 || sourceFilesLength > PART_CONSTRAINTS.MAX_PARTS_COUNT) {
      throw new errors.InvalidArgumentError(
        `"There must be as least one and up to ${PART_CONSTRAINTS.MAX_PARTS_COUNT} source objects.`,
      )
    }

    for (let i = 0; i < sourceFilesLength; i++) {
      const sObj = sourceObjList[i] as CopySourceOptions
      if (!sObj.validate()) {
        return false
      }
    }

    if (!(destObjConfig as CopyDestinationOptions).validate()) {
      return false
    }

    const getStatOptions = (srcConfig: CopySourceOptions) => {
      let statOpts = {}
      if (!_.isEmpty(srcConfig.VersionID)) {
        statOpts = {
          versionId: srcConfig.VersionID,
        }
      }
      return statOpts
    }
    const srcObjectSizes: number[] = []
    let totalSize = 0
    let totalParts = 0

    const sourceObjStats = sourceObjList.map((srcItem) =>
      this.statObject(srcItem.Bucket, srcItem.Object, getStatOptions(srcItem)),
    )

    const srcObjectInfos = await Promise.all(sourceObjStats)

    const validatedStats = srcObjectInfos.map((resItemStat, index) => {
      const srcConfig: CopySourceOptions | undefined = sourceObjList[index]

      let srcCopySize = resItemStat.size
      // Check if a segment is specified, and if so, is the
      // segment within object bounds?
      if (srcConfig && srcConfig.MatchRange) {
        // Since range is specified,
        //    0 <= src.srcStart <= src.srcEnd
        // so only invalid case to check is:
        const srcStart = srcConfig.Start
        const srcEnd = srcConfig.End
        if (srcEnd >= srcCopySize || srcStart < 0) {
          throw new errors.InvalidArgumentError(
            `CopySrcOptions ${index} has invalid segment-to-copy [${srcStart}, ${srcEnd}] (size is ${srcCopySize})`,
          )
        }
        srcCopySize = srcEnd - srcStart + 1
      }

      // Only the last source may be less than `absMinPartSize`
      if (srcCopySize < PART_CONSTRAINTS.ABS_MIN_PART_SIZE && index < sourceFilesLength - 1) {
        throw new errors.InvalidArgumentError(
          `CopySrcOptions ${index} is too small (${srcCopySize}) and it is not the last part.`,
        )
      }

      // Is data to copy too large?
      totalSize += srcCopySize
      if (totalSize > PART_CONSTRAINTS.MAX_MULTIPART_PUT_OBJECT_SIZE) {
        throw new errors.InvalidArgumentError(`Cannot compose an object of size ${totalSize} (> 5TiB)`)
      }

      // record source size
      srcObjectSizes[index] = srcCopySize

      // calculate parts needed for current source
      totalParts += partsRequired(srcCopySize)
      // Do we need more parts than we are allowed?
      if (totalParts > PART_CONSTRAINTS.MAX_PARTS_COUNT) {
        throw new errors.InvalidArgumentError(
          `Your proposed compose object requires more than ${PART_CONSTRAINTS.MAX_PARTS_COUNT} parts`,
        )
      }

      return resItemStat
    })

    if ((totalParts === 1 && totalSize <= PART_CONSTRAINTS.MAX_PART_SIZE) || totalSize === 0) {
      return await this.copyObject(sourceObjList[0] as CopySourceOptions, destObjConfig) // use copyObjectV2
    }

    // preserve etag to avoid modification of object while copying.
    for (let i = 0; i < sourceFilesLength; i++) {
      ;(sourceObjList[i] as CopySourceOptions).MatchETag = (validatedStats[i] as BucketItemStat).etag
    }

    const splitPartSizeList = validatedStats.map((resItemStat, idx) => {
      return calculateEvenSplits(srcObjectSizes[idx] as number, sourceObjList[idx] as CopySourceOptions)
    })

    const getUploadPartConfigList = (uploadId: string) => {
      const uploadPartConfigList: UploadPartConfig[] = []

      splitPartSizeList.forEach((splitSize, splitIndex: number) => {
        if (splitSize) {
          const { startIndex: startIdx, endIndex: endIdx, objInfo: objConfig } = splitSize

          const partIndex = splitIndex + 1 // part index starts from 1.
          const totalUploads = Array.from(startIdx)

          const headers = (sourceObjList[splitIndex] as CopySourceOptions).getHeaders()

          totalUploads.forEach((splitStart, upldCtrIdx) => {
            const splitEnd = endIdx[upldCtrIdx]

            const sourceObj = `${objConfig.Bucket}/${objConfig.Object}`
            headers['x-amz-copy-source'] = `${sourceObj}`
            headers['x-amz-copy-source-range'] = `bytes=${splitStart}-${splitEnd}`

            const uploadPartConfig = {
              bucketName: destObjConfig.Bucket,
              objectName: destObjConfig.Object,
              uploadID: uploadId,
              partNumber: partIndex,
              headers: headers,
              sourceObj: sourceObj,
            }

            uploadPartConfigList.push(uploadPartConfig)
          })
        }
      })

      return uploadPartConfigList
    }

    const uploadAllParts = async (uploadList: UploadPartConfig[]) => {
      const partUploads = uploadList.map(async (item) => {
        return this.uploadPart(item)
      })
      // Process results here if needed
      return await Promise.all(partUploads)
    }

    const performUploadParts = async (uploadId: string) => {
      const uploadList = getUploadPartConfigList(uploadId)
      const partsRes = await uploadAllParts(uploadList)
      return partsRes.map((partCopy) => ({ etag: partCopy.etag, part: partCopy.part }))
    }

    const newUploadHeaders = destObjConfig.getHeaders()

    const uploadId = await this.initiateNewMultipartUpload(destObjConfig.Bucket, destObjConfig.Object, newUploadHeaders)
    try {
      const partsDone = await performUploadParts(uploadId)
      return await this.completeMultipartUpload(destObjConfig.Bucket, destObjConfig.Object, uploadId, partsDone)
    } catch (err) {
      return await this.abortMultipartUpload(destObjConfig.Bucket, destObjConfig.Object, uploadId)
    }
  }

  async presignedUrl(
    method: string,
    bucketName: string,
    objectName: string,
    expires?: number | PreSignRequestParams | undefined,
    reqParams?: PreSignRequestParams | Date,
    requestDate?: Date,
  ): Promise<string> {
    if (this.anonymous) {
      throw new errors.AnonymousRequestError(`Presigned ${method} url cannot be generated for anonymous requests`)
    }

    if (!expires) {
      expires = PRESIGN_EXPIRY_DAYS_MAX
    }
    if (!reqParams) {
      reqParams = {}
    }
    if (!requestDate) {
      requestDate = new Date()
    }

    // Type assertions
    if (expires && typeof expires !== 'number') {
      throw new TypeError('expires should be of type "number"')
    }
    if (reqParams && typeof reqParams !== 'object') {
      throw new TypeError('reqParams should be of type "object"')
    }
    if ((requestDate && !(requestDate instanceof Date)) || (requestDate && isNaN(requestDate?.getTime()))) {
      throw new TypeError('requestDate should be of type "Date" and valid')
    }

    const query = reqParams ? qs.stringify(reqParams) : undefined

    try {
      const region = await this.getBucketRegionAsync(bucketName)
      await this.checkAndRefreshCreds()
      const reqOptions = this.getRequestOptions({ method, region, bucketName, objectName, query })

      return presignSignatureV4(
        reqOptions,
        this.accessKey,
        this.secretKey,
        this.sessionToken,
        region,
        requestDate,
        expires,
      )
    } catch (err) {
      if (err instanceof errors.InvalidBucketNameError) {
        throw new errors.InvalidArgumentError(`Unable to get bucket region for ${bucketName}.`)
      }

      throw err
    }
  }

  async presignedGetObject(
    bucketName: string,
    objectName: string,
    expires?: number,
    respHeaders?: PreSignRequestParams | Date,
    requestDate?: Date,
  ): Promise<string> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }

    const validRespHeaders = [
      'response-content-type',
      'response-content-language',
      'response-expires',
      'response-cache-control',
      'response-content-disposition',
      'response-content-encoding',
    ]
    validRespHeaders.forEach((header) => {
      // @ts-ignore
      if (respHeaders !== undefined && respHeaders[header] !== undefined && !isString(respHeaders[header])) {
        throw new TypeError(`response header ${header} should be of type "string"`)
      }
    })
    return this.presignedUrl('GET', bucketName, objectName, expires, respHeaders, requestDate)
  }

  async presignedPutObject(bucketName: string, objectName: string, expires?: number): Promise<string> {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`)
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`)
    }

    return this.presignedUrl('PUT', bucketName, objectName, expires)
  }

  newPostPolicy(): PostPolicy {
    return new PostPolicy()
  }

  async presignedPostPolicy(postPolicy: PostPolicy): Promise<PostPolicyResult> {
    if (this.anonymous) {
      throw new errors.AnonymousRequestError('Presigned POST policy cannot be generated for anonymous requests')
    }
    if (!isObject(postPolicy)) {
      throw new TypeError('postPolicy should be of type "object"')
    }
    const bucketName = postPolicy.formData.bucket as string
    try {
      const region = await this.getBucketRegionAsync(bucketName)

      const date = new Date()
      const dateStr = makeDateLong(date)
      await this.checkAndRefreshCreds()

      if (!postPolicy.policy.expiration) {
        // 'expiration' is mandatory field for S3.
        // Set default expiration date of 7 days.
        const expires = new Date()
        expires.setSeconds(PRESIGN_EXPIRY_DAYS_MAX)
        postPolicy.setExpires(expires)
      }

      postPolicy.policy.conditions.push(['eq', '$x-amz-date', dateStr])
      postPolicy.formData['x-amz-date'] = dateStr

      postPolicy.policy.conditions.push(['eq', '$x-amz-algorithm', 'AWS4-HMAC-SHA256'])
      postPolicy.formData['x-amz-algorithm'] = 'AWS4-HMAC-SHA256'

      postPolicy.policy.conditions.push(['eq', '$x-amz-credential', this.accessKey + '/' + getScope(region, date)])
      postPolicy.formData['x-amz-credential'] = this.accessKey + '/' + getScope(region, date)

      if (this.sessionToken) {
        postPolicy.policy.conditions.push(['eq', '$x-amz-security-token', this.sessionToken])
        postPolicy.formData['x-amz-security-token'] = this.sessionToken
      }

      const policyBase64 = Buffer.from(JSON.stringify(postPolicy.policy)).toString('base64')

      postPolicy.formData.policy = policyBase64

      postPolicy.formData['x-amz-signature'] = postPresignSignatureV4(region, date, this.secretKey, policyBase64)
      const opts = {
        region: region,
        bucketName: bucketName,
        method: 'POST',
      }
      const reqOptions = this.getRequestOptions(opts)
      const portStr = this.port == 80 || this.port === 443 ? '' : `:${this.port.toString()}`
      const urlStr = `${reqOptions.protocol}//${reqOptions.host}${portStr}${reqOptions.path}`
      return { postURL: urlStr, formData: postPolicy.formData }
    } catch (err) {
      if (err instanceof errors.InvalidBucketNameError) {
        throw new errors.InvalidArgumentError(`Unable to get bucket region for ${bucketName}.`)
      }

      throw err
    }
  }
  // list a batch of objects
  async listObjectsQuery(bucketName: string, prefix?: string, marker?: string, listQueryOpts?: ListObjectQueryOpts) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isString(prefix)) {
      throw new TypeError('prefix should be of type "string"')
    }
    if (!isString(marker)) {
      throw new TypeError('marker should be of type "string"')
    }

    if (listQueryOpts && !isObject(listQueryOpts)) {
      throw new TypeError('listQueryOpts should be of type "object"')
    }
    let { Delimiter, MaxKeys, IncludeVersion } = listQueryOpts as ListObjectQueryOpts

    if (!isString(Delimiter)) {
      throw new TypeError('Delimiter should be of type "string"')
    }
    if (!isNumber(MaxKeys)) {
      throw new TypeError('MaxKeys should be of type "number"')
    }

    const queries = []
    // escape every value in query string, except maxKeys
    queries.push(`prefix=${uriEscape(prefix)}`)
    queries.push(`delimiter=${uriEscape(Delimiter)}`)
    queries.push(`encoding-type=url`)

    if (IncludeVersion) {
      queries.push(`versions`)
    }

    if (marker) {
      marker = uriEscape(marker)
      if (IncludeVersion) {
        queries.push(`key-marker=${marker}`)
      } else {
        queries.push(`marker=${marker}`)
      }
    }

    // no need to escape maxKeys
    if (MaxKeys) {
      if (MaxKeys >= 1000) {
        MaxKeys = 1000
      }
      queries.push(`max-keys=${MaxKeys}`)
    }
    queries.sort()
    let query = ''
    if (queries.length > 0) {
      query = `${queries.join('&')}`
    }

    const method = 'GET'
    const res = await this.makeRequestAsync({ method, bucketName, query })
    const body = await readAsString(res)
    const listQryList = parseListObjects(body)
    return listQryList
  }

  listObjects(
    bucketName: string,
    prefix?: string,
    recursive?: boolean,
    listOpts?: ListObjectQueryOpts | undefined,
  ): BucketStream<ObjectInfo> {
    if (prefix === undefined) {
      prefix = ''
    }
    if (recursive === undefined) {
      recursive = false
    }
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName)
    }
    if (!isValidPrefix(prefix)) {
      throw new errors.InvalidPrefixError(`Invalid prefix : ${prefix}`)
    }
    if (!isString(prefix)) {
      throw new TypeError('prefix should be of type "string"')
    }
    if (!isBoolean(recursive)) {
      throw new TypeError('recursive should be of type "boolean"')
    }
    if (listOpts && !isObject(listOpts)) {
      throw new TypeError('listOpts should be of type "object"')
    }
    let marker: string | undefined = ''
    const listQueryOpts = {
      Delimiter: recursive ? '' : '/', // if recursive is false set delimiter to '/'
      MaxKeys: 1000,
      IncludeVersion: listOpts?.IncludeVersion,
    }
    let objects: ObjectInfo[] = []
    let ended = false
    const readStream: stream.Readable = new stream.Readable({ objectMode: true })
    readStream._read = async () => {
      // push one object per _read()
      if (objects.length) {
        readStream.push(objects.shift())
        return
      }
      if (ended) {
        return readStream.push(null)
      }

      try {
        const result: ListObjectQueryRes = await this.listObjectsQuery(bucketName, prefix, marker, listQueryOpts)
        if (result.isTruncated) {
          marker = result.nextMarker || result.versionIdMarker
        } else {
          ended = true
        }
        if (result.objects) {
          objects = result.objects
        }
        // @ts-ignore
        readStream._read()
      } catch (err) {
        readStream.emit('error', err)
      }
    }
    return readStream
  }
}
