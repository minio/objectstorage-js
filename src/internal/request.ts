import type * as http from 'node:http'
import type * as https from 'node:https'
import type * as stream from 'node:stream'
import { pipeline } from 'node:stream'

export async function request(
  transport: typeof http | typeof https,
  opt: https.RequestOptions,
  body: Buffer | string | stream.Readable | null = null,
): Promise<http.IncomingMessage> {
  return new Promise<http.IncomingMessage>((resolve, reject) => {
    const requestObj = transport.request(opt, (resp) => {
      resolve(resp)
    })

    requestObj.on('error', (e: unknown) => {
      reject(e)
    })

    if (body) {
      if (!Buffer.isBuffer(body) && typeof body !== 'string') {
        pipeline(body, requestObj, (err) => {
          if (err) {
            reject(err)
          }
        })
      } else {
        requestObj.write(body)
      }
    }

    requestObj.end()
  })
}
