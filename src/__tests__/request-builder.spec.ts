import { buildUrlHeaders } from '../request-builder'


describe('request builder test', () => {
    it('not clean CF_', async () => {
        const
            { url, headers } = buildUrlHeaders({
                'CF_API_KEY': 'the-token',
                'CF_HOST': 'g.codefresh.io',
                'CF_IMAGE': 'testImage'
            })
        expect(url).toEqual('g.codefresh.io/api/image-report?CF_IMAGE=testImage')
        expect(headers).toEqual({ 'authorization': 'the-token' })
    })

})
