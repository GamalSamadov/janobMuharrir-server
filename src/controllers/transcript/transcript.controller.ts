import { Request, Response, Router } from 'express'
import { body, validationResult } from 'express-validator'

import { TranscriptDto } from '@/dto/transcript.dto'
import { authenticate } from '@/middlewares/auth.middleware'

const router = Router()

router.post(
	'/',
	body('url').isURL(),
	authenticate,
	async (req: Request, res: Response): Promise<void> => {
		const errors = validationResult(req)

		if (!errors.isEmpty()) {
			res.status(400).json({ errors: errors.array() })
			return
		}

		const dto: TranscriptDto = req.body

		res.status(200).json(dto)
	}
)

export { router as transcriptController }
