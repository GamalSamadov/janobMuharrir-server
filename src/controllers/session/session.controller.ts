import { Request, Response, Router } from 'express'

import { authenticate } from '@/middlewares/auth.middleware'
import { SessionService } from '@/services/session/session.service'

const router = Router()
const userSession = new SessionService()

router.get('/start', authenticate, async (req: Request, res: Response) => {
	const userId = req.user.id
	const sessionId = await userSession.create(userId)

	res.json({ sessionId: sessionId })
})

export { router as sessionController }
