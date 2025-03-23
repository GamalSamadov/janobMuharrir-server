import { Request, Response, Router } from 'express'
import { body, validationResult } from 'express-validator'

import { AuthDto } from '@/dto/auth.dto'
import { authService, refreshTokenService } from '@/services/auth'

const router = Router()

router.post(
	'/login',
	body('email').isEmail(),
	body('password').isLength({ min: 6 }),
	async (req: Request, res: Response): Promise<void> => {
		const errors = validationResult(req)

		if (!errors.isEmpty()) {
			res.status(400).json({ errors: errors.array() })
			return
		}

		try {
			const dto: AuthDto = req.body
			const { refreshToken, ...response } = await authService.login(dto)

			refreshTokenService.addRefreshTokenResponse(res, refreshToken)

			res.status(201).json(response)
		} catch (error) {
			res.status(400).json({ message: error.message })
		}
	}
)

router.post(
	'/register',
	body('email').isEmail(),
	body('password').isLength({ min: 6 }),
	async (req: Request, res: Response): Promise<void> => {
		const errors = validationResult(req)

		if (!errors.isEmpty()) {
			res.status(400).json({ errors: errors.array() })
			return
		}

		try {
			const dto: AuthDto = req.body
			const { refreshToken, ...response } =
				await authService.register(dto)

			refreshTokenService.addRefreshTokenResponse(res, refreshToken)

			res.status(201).json(response)
		} catch (error) {
			res.status(400).json({ message: error.message })
		}
	}
)

router.post(
	'/access-token',
	async (req: Request, res: Response): Promise<void> => {
		const refreshTokenFromCookies =
			req.cookies[refreshTokenService.REFRESH_TOKEN_NAME]

		if (!refreshTokenFromCookies) {
			refreshTokenService.removeRefreshTokenResponse(res)
			res.status(401).json({ message: 'Unauthorized' })
			return
		}

		try {
			const { refreshToken, ...response } =
				await authService.getNewTokens(refreshTokenFromCookies)

			refreshTokenService.addRefreshTokenResponse(res, refreshToken)
			res.status(201).json(response)
		} catch (error) {
			res.status(400).json({ message: error.message })
		}
	}
)

router.post('/logout', async (req: Request, res: Response): Promise<void> => {
	try {
		refreshTokenService.removeRefreshTokenResponse(res)
		res.status(200).json(true)
	} catch (error) {
		res.status(400).json({ message: error.message })
	}
})

export default router
