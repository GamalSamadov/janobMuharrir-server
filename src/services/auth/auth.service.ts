import { User } from '@prisma/client'
import { verify } from 'argon2'
import { JwtPayload } from 'jsonwebtoken'
import omit from 'lodash/omit'

import { AuthDto } from '@/dto/auth.dto'

import { userService } from '../user'

import jwtService from './jwt.service'
import refreshTokenService from './refresh-token.service'

const refreshTokenExpiresIn = refreshTokenService.EXPIRE_DAY_REFRESH_TOKEN

class AuthService {
	async login(dto: AuthDto) {
		const user = await this.validateUser(dto)
		return this.buildResponseObject(user)
	}

	async register(dto: AuthDto) {
		const userExists = await userService.getByEmail(dto.email)

		if (userExists) {
			throw new Error('User already exists')
		}

		const user = await userService.create(dto)
		return this.buildResponseObject(user)
	}

	async getNewTokens(refreshToken: string) {
		const result = jwtService.verify(refreshToken) as JwtPayload

		if (!result || typeof result === 'string') {
			throw new Error('Invalid refresh token')
		}

		const user = await userService.getById(result.id)

		return this.buildResponseObject(user)
	}

	async buildResponseObject(user: User) {
		const tokens = await this.issueTokens(user.id, user.email)
		return { user: this.omitPassword(user), ...tokens }
	}

	private async issueTokens(userId: string, email: string) {
		const payload = { id: userId }
		const accessToken = jwtService.sign(payload, {
			expiresIn: jwtService.ACCESS_TOKEN_EXPIRATION
		})
		const refreshToken = jwtService.sign(payload, {
			expiresIn: refreshTokenExpiresIn
		})

		return { accessToken, refreshToken }
	}

	private async validateUser(dto: AuthDto) {
		const user = await userService.getByEmail(dto.email)

		if (!user) {
			throw new Error('User not found')
		}

		const isValid = await verify(user.password, dto.password)

		if (!isValid) {
			throw new Error('Invalid password')
		}

		return user
	}

	private async omitPassword(user: User) {
		return omit(user, ['password'])
	}
}

const authService = new AuthService()

export default authService
