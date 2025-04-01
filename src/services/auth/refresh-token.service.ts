import { Response } from 'express'

import { CLIENT_URL, IS_PRODUCTION } from '@/constants'

class RefreshTokenService {
	readonly EXPIRE_DAY_REFRESH_TOKEN = 30
	readonly REFRESH_TOKEN_NAME = 'refresh_token'

	addRefreshTokenResponse(res: Response, refreshToken: string) {
		const expiresIn = new Date()
		expiresIn.setDate(expiresIn.getDate() + this.EXPIRE_DAY_REFRESH_TOKEN)

		res.cookie(this.REFRESH_TOKEN_NAME, refreshToken, {
			httpOnly: true,
			domain: IS_PRODUCTION
				? CLIENT_URL.replace('https://', '')
				: 'localhost',
			expires: expiresIn,
			secure: IS_PRODUCTION,
			sameSite: 'none'
		})
	}

	removeRefreshTokenResponse(res: Response) {
		res.cookie(this.REFRESH_TOKEN_NAME, '', {
			httpOnly: true,
			domain: IS_PRODUCTION
				? CLIENT_URL.replace('https://', '')
				: 'localhost',
			expires: new Date(0),
			secure: IS_PRODUCTION,
			sameSite: 'none'
		})
	}
}

const refreshTokenService = new RefreshTokenService()

export default refreshTokenService
