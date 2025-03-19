import { User } from '@prisma/client'
import { verify } from 'argon2'
import { JwtPayload } from 'jsonwebtoken'
import omit from 'lodash/omit'

import { AuthDto } from '@/dto/auth.dto'
import { UserService } from '@/services/user'

import { JWTService } from './jwt.service'
import { RefreshTokenService } from './refresh-token.service'

const refreshTokenService = new RefreshTokenService()
const refreshTokenExpiresIn = refreshTokenService.EXPIRE_DAY_REFRESH_TOKEN

class AuthService {
	private readonly userService = new UserService()
	private readonly jwt = new JWTService()

	async login(dto: AuthDto) {
		const user = await this.validateUser(dto)
		return this.buildResponseObject(user)
	}

	async register(dto: AuthDto) {
		const userExists = await this.userService.getByEmail(dto.email)

		if (userExists) {
			throw new Error('User already exists')
		}

		const user = await this.userService.create(dto)
		return this.buildResponseObject(user)
	}

	async getNewTokens(refreshToken: string) {
		const result = this.jwt.verify(refreshToken) as JwtPayload

		if (!result || typeof result === 'string') {
			throw new Error('Invalid refresh token')
		}

		const user = await this.userService.getById(result.id)

		return this.buildResponseObject(user)
	}

	async buildResponseObject(user: User) {
		const tokens = await this.issueTokens(user.id, user.email)
		return { user: this.omitPassword(user), ...tokens }
	}

	private async issueTokens(userId: string, email: string) {
		const payload = { id: userId }
		const accessToken = this.jwt.sign(payload, {
			expiresIn: this.jwt.ACCESS_TOKEN_EXPIRATION
		})
		const refreshToken = this.jwt.sign(payload, {
			expiresIn: refreshTokenExpiresIn
		})

		return { accessToken, refreshToken }
	}

	private async validateUser(dto: AuthDto) {
		const user = await this.userService.getByEmail(dto.email)

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

export const authService = new AuthService()
