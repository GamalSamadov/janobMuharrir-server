import { PrismaClient, TranscriptionJobStatus } from '@prisma/client'
import { Request, Response, Router } from 'express'

import { runTranscriptionJob } from '@/jobs/transcribe.job'
import { logger } from '@/lib/logger'
import { authenticate } from '@/middlewares/auth.middleware'

const router = Router()

const prisma = new PrismaClient()

const jobConnections = new Map<string, Response[]>()

router.get(
	'/:sessionId/find-all',
	authenticate,
	async (req: Request, res: Response) => {
		try {
			const { sessionId } = req.params

			const existingSession = await prisma.userSession.findUnique({
				where: { id: sessionId }
			})
			if (!existingSession) {
				res.status(404).json({ message: 'Session not found!' })
				return
			}

			const events = await prisma.transcriptionEvent.findMany({
				where: {
					job: {
						session: {
							id: sessionId
						}
					}
				},
				orderBy: { createdAt: 'asc' }
			})

			res.json(events)
		} catch (error: any) {
			logger.error(error)
			res.status(500).json({ message: error.message })
		}
	}
)

router.get('/:sessionId', async (req: Request, res: Response) => {
	const { sessionId } = req.params
	const { url } = req.query as { url: string }

	if (!url) {
		res.status(400).json({ message: 'URL is required!' })
		return
	}

	try {
		// 1. Check that the session actually exists
		const existingSession = await prisma.userSession.findUnique({
			where: { id: sessionId }
		})
		if (!existingSession) {
			res.status(404).json({ message: 'Session not found!' })
			return
		}

		// 2. Find or create a TranscriptionJob in the DB
		let job = await prisma.transcriptionJob.findFirst({
			where: {
				session: {
					id: sessionId
				},
				url
			}
		})

		if (!job) {
			// Create a brand new job
			job = await prisma.transcriptionJob.create({
				data: {
					url,
					status: TranscriptionJobStatus.PENDING,
					session: {
						connect: { id: sessionId }
					}
				}
			})

			// Kick off background process
			void runTranscriptionJob(
				job.id,
				sessionId,
				url,
				// This broadcast function is how the worker can push SSE updates
				async (content, completed) => {
					const connections = jobConnections.get(job!.id) || []
					if (connections.length) {
						// Write SSE data to all connected clients
						const payload = {
							content,
							completed,
							createdAt: new Date().toISOString()
						}
						connections.forEach(resp => {
							resp.write(`data: ${JSON.stringify(payload)}\n\n`)
						})
					}
				}
			).catch(err => {
				console.error('Transcription job error:', err)
			})
		}

		// 3. Setup SSE response
		res.writeHead(200, {
			'Content-Type': 'text/event-stream',
			'Cache-Control': 'no-cache',
			Connection: 'keep-alive'
		})

		// Add this res to the jobConnections
		if (!jobConnections.has(job.id)) {
			jobConnections.set(job.id, [])
		}
		jobConnections.get(job.id)?.push(res)

		// 4. Send any previously stored events so the user can see the history
		const existingEvents = await prisma.transcriptionEvent.findMany({
			where: { jobId: job.id },
			orderBy: { createdAt: 'asc' }
		})

		for (const evt of existingEvents) {
			const payload = {
				content: evt.content,
				completed: evt.completed,
				createdAt: evt.createdAt
			}
			res.write(`data: ${JSON.stringify(payload)}\n\n`)
		}

		// 5. Keep connection open and handle disconnect
		req.on('close', () => {
			const list = jobConnections.get(job!.id)
			if (list) {
				jobConnections.set(
					job!.id,
					list.filter(r => r !== res)
				)
			}
		})
	} catch (error) {
		logger.error(error)
		res.status(500).json({ message: error.message })
	}
})

export default router
