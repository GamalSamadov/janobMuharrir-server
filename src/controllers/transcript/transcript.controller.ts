import { Request, Response, Router } from 'express'

import { authenticate } from '@/middlewares/auth.middleware'
import { transcriptService } from '@/services/transcript/transcript.service'

const router = Router()

router.get(
	'/',
	authenticate,
	async (req: Request, res: Response): Promise<void> => {
		try {
			const transcripts = await transcriptService.getAll()

			if (!transcripts) {
				res.status(404).json({ message: 'Transcripts not found' })
				return
			}

			res.status(200).json(transcripts)
		} catch (error) {
			res.status(400).json({ message: error.message })
			return
		}
	}
)

router.delete(
	'/:id',
	authenticate,
	async (req: Request, res: Response): Promise<void> => {
		try {
			const { id } = req.params

			const transcript = await transcriptService.delete(id)

			if (!transcript) {
				res.status(404).json({ message: 'Transcript not found' })
				return
			}

			res.status(200).json(transcript)
		} catch (error) {
			res.status(400).json({ message: error.message })
			return
		}
	}
)

export default router
