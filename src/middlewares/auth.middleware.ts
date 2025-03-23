import { User } from '@prisma/client'
import * as dotenv from 'dotenv'
import { NextFunction, Request, Response } from 'express'
import * as jwt from 'jsonwebtoken'

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
	const token = req.headers.authorization?.split(' ')[1]
	if (!token) {
		res.status(401).json({ message: 'Unauthorized' })
		return
	}

	try {
		const decoded = jwt.verify(token, JWT_SECRET) as { id: string }
		const user = await userService.getById(decoded.id)
		if (!user) {
			res.status(401).json({ message: 'User not found' })
			return
		}
		req.user = user
		next()
	} catch (err) {
		res.status(401).json({ message: 'Invalid token' })
		return
	}
}
