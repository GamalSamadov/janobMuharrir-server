import { Response } from 'express'

import { IS_PRODUCTION } from '@/constants'

const getBaseDomain = (url: string | undefined): string | undefined => {
	if (!url) return undefined
	try {
		const hostname = new URL(url).hostname // e.g., "muharrir.janob.io" or "api-muharrir.janob.io"
		const parts = hostname.split('.')
		if (parts.length >= 2) {
			// Join the last two parts, preceded by a dot
			return `.${parts.slice(-2).join('.')}` // e.g., ".janob.io"
		}
		return hostname // Fallback for domains like "localhost" or simple TLDs
	} catch (e) {
		return undefined // Handle invalid URL
	}
}

class RefreshTokenService {
	readonly EXPIRE_DAY_REFRESH_TOKEN = 30
	readonly REFRESH_TOKEN_NAME = 'refreshToken'

	addRefreshTokenResponse(res: Response, refreshToken: string) {
		const expiresIn = new Date()
		expiresIn.setDate(expiresIn.getDate() + this.EXPIRE_DAY_REFRESH_TOKEN)

		// Use the base domain for production
		const cookieDomain = IS_PRODUCTION
			? getBaseDomain(process.env.CLIENT_URL)
			: 'localhost'

		const cookieOptions: any = {
			httpOnly: true,
			expires: expiresIn,
			secure: IS_PRODUCTION,
			sameSite: IS_PRODUCTION ? 'none' : 'lax',
			...(cookieDomain &&
				cookieDomain !== 'localhost' && { domain: cookieDomain })
		}

		res.cookie(this.REFRESH_TOKEN_NAME, refreshToken, cookieOptions)
	}

	removeRefreshTokenResponse(res: Response) {
		const cookieDomain = IS_PRODUCTION
			? getBaseDomain(process.env.CLIENT_URL)
			: 'localhost'
		const cookieOptions: any = {
			httpOnly: true,
			expires: new Date(0),
			secure: IS_PRODUCTION,
			sameSite: IS_PRODUCTION ? 'none' : 'lax',
			...(cookieDomain &&
				cookieDomain !== 'localhost' && { domain: cookieDomain })
		}

		res.cookie(this.REFRESH_TOKEN_NAME, '', cookieOptions)
	}
}

const refreshTokenService = new RefreshTokenService()
export default refreshTokenService
