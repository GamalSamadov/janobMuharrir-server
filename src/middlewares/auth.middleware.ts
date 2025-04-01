import { User } from '@prisma/client'
import * as dotenv from 'dotenv'
import { NextFunction, Request, Response } from 'express'
import * as jwt from 'jsonwebtoken'

import { logger } from '@/lib/logger'
import { userService } from '@/services/user'

dotenv.config()

const JWT_SECRET = process.env.JWT_SECRET

declare global {
	namespace Express {
		interface Request {
			user?: User
		}
	}
}

export const authenticate = async (
	req: Request,
	res: Response,
	next: NextFunction
): Promise<void> => {
	const authHeader = req.headers.authorization
	logger.info(`[Auth Middleware] Path: ${req.path}`) // Log entry
	logger.info(
		`[Auth Middleware] Authorization Header: ${authHeader ? authHeader.substring(0, 15) : 'Not Present'}`
	) // Log header start or absence

	const token = authHeader?.split(' ')[1]
	if (!token) {
		logger.warn('[Auth Middleware] No token found in header.')
		res.status(401).json({ message: 'Unauthorized - No token provided' })
		return
	}

	logger.info(`[Auth Middleware] Token found: ${token.substring(0, 10)}...`)

	if (!JWT_SECRET) {
		logger.error(
			'[Auth Middleware] JWT_SECRET environment variable is not set!'
		)
		res.status(500).json({
			message: 'Internal server error - JWT secret missing'
		})
		return
	}
	// Avoid logging the full secret in production logs if possible
	logger.info(
		`[Auth Middleware] Verifying token using ${JWT_SECRET.length}-char secret.`
	)

	try {
		const decoded = jwt.verify(token, JWT_SECRET) as { id: string }
		logger.info(
			`[Auth Middleware] Token verified successfully for user ID: ${decoded.id}`
		)

		const user = await userService.getById(decoded.id)
		if (!user) {
			logger.warn(
				`[Auth Middleware] User not found for ID from token: ${decoded.id}`
			)
			res.status(401).json({
				message: 'User associated with token not found'
			})
			return
		}
		logger.info(
			`[Auth Middleware] User ${user.email} authenticated successfully.`
		)
		req.user = user
		next()
	} catch (err: any) {
		// Catch specific error
		logger.error('[Auth Middleware] Token verification failed!', {
			// Log the error object
			error_message: err.message,
			error_name: err.name
			// error_stack: err.stack // Optional: might be too verbose
		})
		// Customize message based on error type if needed
		if (err instanceof jwt.JsonWebTokenError) {
			res.status(401).json({ message: `Invalid token: ${err.message}` })
		} else if (err instanceof jwt.TokenExpiredError) {
			res.status(401).json({ message: 'Token expired' })
		} else {
			res.status(401).json({ message: 'Invalid token' })
		}
		return
	}
}
