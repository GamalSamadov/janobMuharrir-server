import { logger } from '@/lib/logger'

export function extractVideoId(url: string): string | null {
	try {
		const urlObj = new URL(url)
		if (urlObj.hostname === 'youtu.be') {
			return urlObj.pathname.slice(1)
		}
		if (
			urlObj.hostname.includes('youtube.com') &&
			urlObj.searchParams.has('v')
		) {
			return urlObj.searchParams.get('v')
		}
		return null
	} catch (e) {
		logger.error(`Failed to parse URL: ${url}`, e)
		return null
	}
}
