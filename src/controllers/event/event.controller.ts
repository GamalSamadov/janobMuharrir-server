import { PrismaClient, TranscriptionJobStatus } from '@prisma/client'
import { type Request, type Response, Router } from 'express'

import { runTranscriptionJob } from '@/jobs/transcribe.job'
import { logger } from '@/lib/logger'
import { authenticate } from '@/middlewares/auth.middleware'

const router = Router()

const prisma = new PrismaClient()

// Store connections per JOB ID
const jobConnections = new Map<string, Response[]>()

router.get(
	'/:sessionId/find-all',
	authenticate,
	async (req: Request, res: Response): Promise<void> => {
		try {
			const { sessionId } = req.params

			const existingSession = await prisma.userSession.findUnique({
				where: { id: sessionId }
			})
			if (!existingSession) {
				res.status(404).json({ message: 'Session not found!' })
				return
			}

			const jobs = await prisma.transcriptionJob.findMany({
				where: { sessionId: sessionId },
				select: { id: true }
			})

			if (jobs.length === 0) {
				res.json([])
				return
			}

			const jobIds = jobs.map(job => job.id)

			const events = await prisma.transcriptionEvent.findMany({
				where: {
					jobId: { in: jobIds }
				},
				orderBy: { createdAt: 'asc' }
			})

			res.json(events)
		} catch (error: any) {
			logger.error(
				{
					error: error.message,
					stack: error.stack,
					sessionId: req.params.sessionId
				},
				'Error finding all events for session'
			)
			res.status(500).json({ message: 'Server error fetching events.' })
		}
	}
)

router.get(
	'/:sessionId',
	async (req: Request, res: Response): Promise<void> => {
		const { sessionId } = req.params
		const { url } = req.query as { url: string }

		if (!url) {
			res.status(400).json({
				message: 'URL query parameter is required!'
			})
			return
		}

		// Define job with a more precise Prisma-generated type if possible, or keep the explicit one.
		// Using Prisma's generated type is generally better if available.
		// type TranscriptionJob = Prisma.TranscriptionJobGetPayload<{}>; // Example using generated type
		let job: {
			id: string
			status: TranscriptionJobStatus
			url: string
			sessionId: string | null
			finalTranscript: string | null
			createdAt: Date
			updatedAt: Date
		} | null = null

		// Define job ID variable for cleanup in catch blocks
		let currentJobId: string | null = null

		try {
			// 1. Check session
			const existingSession = await prisma.userSession.findUnique({
				where: { id: sessionId }
			})
			if (!existingSession) {
				res.status(404).json({ message: 'Session not found!' })
				return
			}

			// 2. Find or create Job
			job = await prisma.transcriptionJob.findFirst({
				where: {
					sessionId: sessionId,
					url
				}
			})
			currentJobId = job?.id ?? null // Assign job ID early if found

			let isNewJob = false
			if (!job) {
				job = await prisma.transcriptionJob.create({
					data: {
						url,
						status: TranscriptionJobStatus.PENDING, // Use Enum
						session: {
							connect: { id: sessionId }
						}
					}
				})
				isNewJob = true
				currentJobId = job.id // Assign newly created job ID
				logger.info(
					`Created new transcription job ${job.id} for session ${sessionId}`
				)
			} else {
				logger.info(
					`Found existing transcription job ${job.id} for session ${sessionId} and URL ${url}`
				)
			}

			// --- Ensure job is not null after creation/finding ---
			// This check satisfies TypeScript that 'job' is non-null in the following code block.
			if (!job) {
				logger.error('Job could not be found or created.', {
					sessionId,
					url
				})
				// Should not happen if logic above is correct, but acts as a safeguard.
				res.status(500).json({
					message: 'Failed to find or create the transcription job.'
				})
				return
			}
			// Now job is guaranteed non-null below this point for the current scope

			// --- SSE Setup ---
			res.writeHead(200, {
				'Content-Type': 'text/event-stream',
				'Cache-Control': 'no-cache',
				Connection: 'keep-alive',
				'X-Accel-Buffering': 'no'
			})

			// Add response to connections map
			if (!jobConnections.has(job.id)) {
				jobConnections.set(job.id, [])
			}
			const currentConnections = jobConnections.get(job.id)! // Non-null assertion safe here
			currentConnections.push(res)
			logger.info(
				`SSE connection established for job ${job.id}. Total connections: ${currentConnections.length}`
			)

			// 4. Send existing events
			const existingEvents = await prisma.transcriptionEvent.findMany({
				where: { jobId: job.id },
				orderBy: { createdAt: 'asc' }
			})

			logger.info(
				`Sending ${existingEvents.length} existing events for job ${job.id}`
			)
			for (const evt of existingEvents) {
				const payload = JSON.stringify({
					content: evt.content,
					completed: evt.completed,
					createdAt: evt.createdAt.toISOString()
				})
				res.write(`data: ${payload}\n\n`)
			}

			// 5. Check if job is already in a terminal state
			// Use Enum members for comparisons
			const terminalStatuses: TranscriptionJobStatus[] = [
				TranscriptionJobStatus.COMPLETED,
				TranscriptionJobStatus.ERROR
			]
			if (terminalStatuses.includes(job.status)) {
				logger.info(
					`Job ${job.id} already in terminal state (${job.status}). Sending last event and closing stream.`
				)
				const lastEvent = existingEvents[existingEvents.length - 1]
				// ... (logic to send last event/final transcript) ... remains the same
				if (lastEvent) {
					const payload = JSON.stringify({
						content: lastEvent.content,
						completed: true, // Final state is always 'completed' for the stream
						createdAt: lastEvent.createdAt.toISOString()
					})
					res.write(`data: ${payload}\n\n`)
				} else if (
					job.status === TranscriptionJobStatus.COMPLETED &&
					job.finalTranscript
				) {
					const payload = JSON.stringify({
						content: job.finalTranscript,
						completed: true,
						createdAt: job.updatedAt.toISOString()
					})
					res.write(`data: ${payload}\n\n`)
				}
				// Send final empty message to signal end? Optional.
				res.end()
				// Remove this response from connections map
				jobConnections.set(
					job.id,
					currentConnections.filter(r => r !== res)
				)
				// Clean up map entry if last connection closed
				if (jobConnections.get(job.id)?.length === 0) {
					jobConnections.delete(job.id)
					logger.info(
						`Last SSE connection closed for terminal job ${job.id}. Removing map entry.`
					)
				}
				return // Stop processing
			}

			// 6. Start background job if needed
			// Define statuses that allow starting/restarting the job
			const startableStatuses: TranscriptionJobStatus[] = [
				TranscriptionJobStatus.PENDING,
				TranscriptionJobStatus.ERROR // Allow restarting errored jobs
			]

			// *** FIXED Condition using array includes ***
			if (isNewJob || startableStatuses.includes(job.status)) {
				logger.info(
					`Starting background transcription job ${job.id} (Status: ${job.status})`
				)

				// Capture the non-null job ID for use inside async callbacks/promises
				const jobIdToRun = job.id

				void runTranscriptionJob(
					jobIdToRun,
					sessionId,
					url,
					// Broadcast Function
					(content, completed) => {
						const connections = jobConnections.get(jobIdToRun) // Use captured ID
						if (connections && connections.length > 0) {
							const payload = JSON.stringify({
								content,
								completed,
								createdAt: new Date().toISOString()
							})
							logger.debug(
								`Broadcasting SSE for job ${jobIdToRun}: Completed=${completed}, Connections=${connections.length}`
							)

							// Iterate backwards for safe removal
							for (let i = connections.length - 1; i >= 0; i--) {
								const resp = connections[i]
								try {
									resp.write(`data: ${payload}\n\n`)
								} catch (writeErr: any) {
									logger.warn(
										`Failed to write SSE to connection ${i} for job ${jobIdToRun}: ${writeErr.message}. Removing connection.`
									)
									connections.splice(i, 1) // Remove broken connection
								}
							}

							// If job completed, close remaining connections and clean up map
							if (completed) {
								logger.info(
									`Job ${jobIdToRun} completed. Closing ${connections.length} SSE connections.`
								)
								connections.forEach(resp => {
									try {
										resp.end()
									} catch {} // Ignore errors on end
								})
								jobConnections.delete(jobIdToRun) // Clean up the map entry
							}
						}
					}
				).catch(err => {
					logger.error(
						{
							error: err.message,
							stack: err.stack,
							jobId: jobIdToRun
						}, // Use captured ID
						'Unhandled error in background transcription job execution.'
					)
					// Attempt to mark job as error in DB
					prisma.transcriptionJob
						.update({
							where: { id: jobIdToRun }, // Use captured ID
							data: { status: TranscriptionJobStatus.ERROR } // Use Enum
						})
						.catch(dbErr => {
							logger.error(
								{ error: dbErr.message, jobId: jobIdToRun }, // Use captured ID
								'Failed to mark job as error after background task exception.'
							)
						})
					// Also clean up any connections if the job errored unexpectedly
					const connections = jobConnections.get(jobIdToRun)
					if (connections) {
						logger.warn(
							`Closing ${connections.length} connections due to background job error for ${jobIdToRun}`
						)
						connections.forEach(resp => {
							try {
								resp.end()
							} catch {}
						})
						jobConnections.delete(jobIdToRun)
					}
				})
			} else {
				// Job exists but is already RUNNING
				logger.info(
					`Job ${job.id} is currently ${job.status}. Client connected to existing stream.`
				)
			}

			// 7. Handle client disconnect
			req.on('close', () => {
				// Use the job ID captured at the start of the request handler scope
				if (currentJobId) {
					const connections = jobConnections.get(currentJobId)
					if (connections) {
						const remainingConnections = connections.filter(
							r => r !== res
						)
						if (remainingConnections.length > 0) {
							jobConnections.set(
								currentJobId,
								remainingConnections
							)
						} else {
							jobConnections.delete(currentJobId) // Clean up map entry
							logger.info(
								`Last SSE connection closed for job ${currentJobId}. Removing map entry.`
							)
						}
						logger.info(
							`SSE connection closed for job ${currentJobId}. Remaining connections: ${remainingConnections.length}`
						)
					}
				} else {
					logger.warn(
						'SSE connection closed, but job ID was not available (unexpected).'
					)
				}
				res.end() // Ensure response stream is closed on client disconnect
			})
		} catch (error: any) {
			logger.error(
				{
					error: error.message,
					stack: error.stack,
					sessionId: sessionId,
					url: url,
					jobId: currentJobId
				},
				'Critical error in main SSE setup endpoint'
			)
			if (!res.headersSent) {
				res.status(500).json({
					message: 'Internal server error during event stream setup.'
				})
			} else {
				// If headers were sent, the stream might be open, try ending it.
				try {
					res.end()
				} catch {}
			}
			// Clean up connection if it was added to the map
			if (currentJobId) {
				const connections = jobConnections.get(currentJobId)
				if (connections) {
					const remaining = connections.filter(r => r !== res)
					if (remaining.length > 0) {
						jobConnections.set(currentJobId, remaining)
					} else {
						jobConnections.delete(currentJobId)
					}
				}
			}
		}
	}
)

export default router
