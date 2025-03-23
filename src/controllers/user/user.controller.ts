import { Request, Response, Router } from 'express'

import { authenticate } from '@/middlewares/auth.middleware'
import { userService } from '@/services/user'

const router = Router()

router.get('/profile', authenticate, async (req: Request, res: Response) => {
	try {
		const userId = req.user.id
		const user = await userService.getById(userId)
		res.json(user)
	} catch (error) {
		res.status(400).json({ message: error.message })
	}
})

export default router
