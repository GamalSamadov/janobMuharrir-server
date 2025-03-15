import * as jwt from 'jsonwebtoken'

import { JWT_SECRET } from '@/constants'
import { logger } from '@/lib/logger'

export class JWTService {
	readonly ACCESS_TOKEN_EXPIRATION = '1h'
	readonly REFRESH_TOKEN_EXPIRATION = '7d'

	sign(payload: object, options?: jwt.SignOptions) {
		return jwt.sign(payload, JWT_SECRET, options)
	}

	verify(token: string, options?: jwt.VerifyOptions) {
		try {
			return jwt.verify(token, JWT_SECRET, options)
		} catch (error) {
			logger.error(error)
			return null
		}
	}

	signAccessToken(payload: object) {
		return this.sign(payload, { expiresIn: this.ACCESS_TOKEN_EXPIRATION })
	}

	signRefreshToken(payload: object) {
		return this.sign(payload, { expiresIn: this.REFRESH_TOKEN_EXPIRATION })
	}
}
