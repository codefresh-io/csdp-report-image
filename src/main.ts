import EventSource from 'eventsource'

import { validate } from './validate'
import { Utils }  from './utils'
import {  errors } from './errors'
import { logger, workflowLogger } from './logger'

const { EventSourceError } = errors

const INITIAL_HEARTBEAT_TIMEOUT_IN_SEC = Number(process.env.INITIAL_HEARTBEAT_TIMEOUT_IN_SECONDS) || 5

/**
 * Take (CF_ prefixed) Env variables and perform http/s request (SSE) to app-proxy for image-report with CF_ENRICHERS
 */
async function main(argv, env): Promise<void> {
    const payload = validate(env)
    const { url, headers } = await Utils.buildUrlHeaders(payload)

    logger.debug(`skip TLS verification on client: ${env['NODE_TLS_REJECT_UNAUTHORIZED'] === '0'}`)
    logger.debug(`skip TLS verification on workflow: ${env['CF_INSECURE'] === 'true'}`)
    logger.debug(`payload: ${JSON.stringify(payload, null, 2)}`)
    logger.debug(`sending request: ${url}, headers: ${JSON.stringify(headers)}`)

    if (payload['CF_CI_TYPE'] && payload['CF_WORKFLOW_URL']) {
        logger.info(`CI provider: ${payload['CF_CI_TYPE']}, job URL: ${payload['CF_WORKFLOW_URL']}`)
    }
    const eventSource = new EventSource(url, { headers })
    eventSource.reconnectInterval = 1000 * 100000 // prevent retry. client should not issue a reconnect

    let heartbeatTimer: Utils.Timer

    const waitFor = new Promise<void>((resolve, reject) => {

        eventSource.addEventListener('open', function () {
            logger.debug('event-source connected')

            heartbeatTimer = Utils.createHeartbeatTimer(() => {
                logger.debug(`missing heartbeat after ${heartbeatTimer.timeoutTime / 1000} seconds`)
                heartbeatTimer.restart()
            }, INITIAL_HEARTBEAT_TIMEOUT_IN_SEC * 1000)
        })

        eventSource.addEventListener('info', function (event) {
            logger.info(event.data)
        })
        eventSource.addEventListener('warn', function (event) {
            logger.warn(event.data)
        })
        eventSource.addEventListener('workflow-log', function (event) {
            const log = Utils.tryParseJson(event.data)
            if (typeof log === 'object' && log.content && log.podName) {
                workflowLogger.info({ pod: log.podName, message: log.content })
            } else {
                workflowLogger.info(event.data)
            }
        })
        eventSource.addEventListener('heartbeat', function (event) {
            logger.debug(`heartbeat ${JSON.stringify(event)}`)

            const heartBeatInterval = Number(event.data)
            if (heartBeatInterval) {
                const extraGap = 1 * 1000
                heartbeatTimer.restart(heartBeatInterval + extraGap)
            } else {
                heartbeatTimer.restart()
            }
        })
        eventSource.addEventListener('error', (errorEvent) => {
            // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
            if (heartbeatTimer) {
                heartbeatTimer.stop()
            }
            eventSource.close()

            const error = Utils.tryParseJson(errorEvent.data)
            let name
            let message
            if (typeof error === 'string') {
                message = error
            } else if (typeof error === 'object') {
                if (typeof error.message === 'string') {
                    message = error.message
                }
                if (typeof error.name === 'string') {
                    name = error.name
                }
            } else {
                message = errorEvent.message || `Runtime connection issue. Restart pipeline, and if problem persists, contact support.`
            }
            reject(new EventSourceError(message, name))
        })
        eventSource.addEventListener('end', (event) => {
            // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
            if (heartbeatTimer) {
                heartbeatTimer.stop()
            }
            eventSource.close()

            logger.info(event.data)
            resolve()
        })
    })
    await waitFor
}

/**
 * calling main with process argv and env. Exit code 1 on error
 */
export async function mainErrorHandling() {
    try {
        await main(process.argv, process.env)
    } catch (error) {
        logger.error(error.toString())
        process.exit(1)
    }
}
