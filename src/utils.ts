import { GraphQLClient, gql } from 'graphql-request'
import { get } from 'lodash'
import { gte } from 'semver'

import { errors } from './errors'

export namespace Utils {
    /**
     * Build image-report url and headers
     * @param payload
     */
     export async function buildUrlHeaders(payload: Record<string, string | undefined>): Promise<{ url: string, headers: { authorization: string } }> {
        const esc = encodeURIComponent
        const headers = { 'authorization': payload['CF_API_KEY']! }
        const runtimeName = payload['CF_RUNTIME_NAME']
        const platformHost = payload['CF_PLATFORM_URL']
        const runtimeVersion = await Utils.getRuntimeVersion(headers, runtimeName, platformHost)
        const shouldStorePayloadInHeaders = gte(runtimeVersion, '0.0.553')
        let host
        if (!runtimeName) {
            host = payload['CF_HOST']
            delete payload['CF_HOST']
        } else {
            host = await Utils.getRuntimeIngressHost(runtimeName, headers, platformHost)
            delete payload['CF_RUNTIME_NAME']
            delete payload['CF_PLATFORM_URL']
        }
        delete payload['CF_API_KEY']
        if (shouldStorePayloadInHeaders) {
            const dockerFileContent = payload['CF_DOCKERFILE_CONTENT']
            if (dockerFileContent) {
                headers['X-CF-DOCKERFILE-CONTENT'] = dockerFileContent
                delete payload['X-CF-DOCKERFILE-CONTENT']
            }
            const qs = Object.entries(payload).map(kv => `${kv[0]}=${kv[1] || ''}`).join('&')
            const data = Buffer.from(qs, 'binary').toString('base64')
            headers['X-CF-DATA'] = data
            return { url: `${host}/app-proxy/api/image-report`, headers }
        }
        const qs = Object.entries(payload).map(kv => `${esc(kv[0])}=${esc(kv[1] || '')}`).join('&')
        const url = `${host}/app-proxy/api/image-report?${qs}`
        if (payload['CF_LOCAL']) {
            return { url: `${host}/api/image-report?${qs}`, headers }
        }
        return { url, headers }
    }

    export async function getRuntimeIngressHost(runtimeName: string, headers: Record<string, string>, platformHost = 'https://g.codefresh.io'): Promise<string> {
        const graphQLClient = new GraphQLClient(`${platformHost}/2.0/api/graphql`, {
            headers
        })

        const getRuntimeIngressHostQuery = gql`
            query Runtime($name: String!) {
                runtime(name: $name) {
                    ingressHost
                }
            }`

        const res = await graphQLClient.request(getRuntimeIngressHostQuery, { name: runtimeName })
        const ingressHost = get(res, 'runtime.ingressHost')
        if (!ingressHost) {
            const message = res.runtime ? `ingress host is not defined on your '${runtimeName}' runtime` : `runtime '${runtimeName}' does not exist`
            throw new errors.ValidationError(message)
        }
        return ingressHost
    }

    export async function getRuntimeVersion(headers: Record<string, string>, runtimeName?: string , platformHost = 'https://g.codefresh.io'): Promise<string> {
        if (!runtimeName) {
            return ''
        }
        const graphQLClient = new GraphQLClient(`${platformHost}/2.0/api/graphql`, {
            headers
        })
        const getRuntimeIngressHostQuery = gql`
            query Runtime($name: String!) {
                runtime(name: $name) {
                    runtimeVersion
                }
            }`
        const res = await graphQLClient.request(getRuntimeIngressHostQuery, { name: runtimeName })
        return get(res, 'runtime.runtimeVersion', '')
    }

    export function tryParseJson (str: string) {
        try {
            return JSON.parse(str)
        } catch {
            return str
        }
    }

    export type Timer = {
        timeoutTime: number,
        restart: (timeoutMs?: number) => void,
        stop: () => void,
    }

    export function createHeartbeatTimer(cb: () => void, timeoutTime: number): Timer {
        let timeout: NodeJS.Timeout = setTimeout(cb, timeoutTime)

        return {
            timeoutTime,
            restart(_timeoutMs?: number) {
                this.stop()
                timeout = setTimeout(cb, _timeoutMs || this.timeoutTime)
            },
            stop() {
                clearTimeout(timeout)
            }
        }
    }
}

