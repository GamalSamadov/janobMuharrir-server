import { Response } from 'express'

import { IS_PRODUCTION } from '@/constants'

export class RefreshTokenService {
	readonly EXPIRE_DAY_REFRESH_TOKEN = 1
	readonly REFRESH_TOKEN_NAME = 'refresh_token'

	addRefreshTokenResponse(res: Response, refreshToken: string) {
		const expiresIn = new Date()
		expiresIn.setDate(expiresIn.getDate() + this.EXPIRE_DAY_REFRESH_TOKEN)

		res.cookie(this.REFRESH_TOKEN_NAME, refreshToken, {
			httpOnly: true,
			domain: 'localhost',
			expires: expiresIn,
			secure: IS_PRODUCTION,
			sameSite: IS_PRODUCTION ? 'lax' : 'none'
		})
	}

	removeRefreshTokenResponse(res: Response) {
		res.cookie(this.REFRESH_TOKEN_NAME, '', {
			httpOnly: true,
			domain: 'localhost',
			expires: new Date(0),
			secure: IS_PRODUCTION,
			sameSite: IS_PRODUCTION ? 'lax' : 'none'
		})
	}
}
