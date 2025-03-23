import { Request, Response, Router } from 'express'

import { authenticate } from '@/middlewares/auth.middleware'
import { userSession } from '@/services/session/session.service'

const router = Router()

router.get('/start', authenticate, async (req: Request, res: Response) => {
	const userId = req.user.id
	const sessionId = await userSession.create(userId)

	res.json({ sessionId: sessionId })
})

export default router
