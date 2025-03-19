import { Request, Response, Router } from 'express'
import fs from 'fs'
import path from 'path'

import { logger } from '@/lib/logger'
import { authenticate } from '@/middlewares/auth.middleware'
import { EventService } from '@/services/event/event.service'
import { SessionService } from '@/services/session/session.service'
import { convertToUzbekLatin } from '@/utils/cryllic-to-latin.util'
import { downloadYoutubeAudio } from '@/utils/download-audio.util'
import { editTranscribed } from '@/utils/edit-transcribed .util'
import { formatDuration } from '@/utils/format-duration.util'
import { transcribeWithGoogle, uploadAudioToGCS } from '@/utils/google-stt.util'
import { splitMp3IntoSegments } from '@/utils/split-audio.util'
import { transcribeAudioElevenLabs } from '@/utils/transcribe-elevenlabs.util'

const router = Router()

const eventService = new EventService()
const userSession = new SessionService()

const audioFilesPath = path.resolve(__dirname, 'audios')
const transcriptsPath = path.resolve(__dirname, 'transcripts')

router.get(
	'/:sessionId/find-all',
	authenticate,
	async (req: Request, res: Response) => {
		const events = await eventService.findMany(req.params.sessionId)
		res.json(events)
	}
)

router.get(
	'/:sessionId',

	async (req: Request, res: Response) => {
		const { sessionId } = req.params
		const { url: orgUrl } = req.query
		const url = orgUrl as string

		if (!url) {
			res.status(400).json({ message: 'URL is required!' })
			return
		}

		try {
			let event
			const startTime = performance.now()

			res.writeHead(200, {
				'Content-Type': 'text/event-stream',
				Connection: 'keep-alive',
				'Cache-Control': 'no-cache'
			})

			// DOWNLOAD AUDIO

			event = await eventService.create(
				`(Youtube)dan video ovoz shaklida yuklanmoqda...`,
				sessionId
			)
			res.write(
				`data: ${JSON.stringify({
					event: event.content,
					createdAt: event.createdAt
				})}\n\n`
			)

			const { outputPath, title: downloadedVideoTitle } =
				await downloadYoutubeAudio(url, audioFilesPath)

			event = await eventService.create(`Ovoz yuklandi!`, sessionId)
			res.write(
				`data: ${JSON.stringify({
					event: event.content,
					createdAt: event.createdAt
				})}\n\n`
			)

			await new Promise(resolve => setTimeout(resolve, 500))

			// SPLIT AUDIO INTO SEGMENTS

			event = await eventService.create(
				`Ovoz qismlarga taqsimlanmoqda...`,
				sessionId
			)
			res.write(
				`data: ${JSON.stringify({
					event: event.content,
					createdAt: event.createdAt
				})}\n\n`
			)

			const { segments, outputDir } = await splitMp3IntoSegments(
				outputPath,
				audioFilesPath
			)

			event = await eventService.create(
				`Ovoz ${segments.length} qismga taqsimlandi!`,
				sessionId
			)
			res.write(
				`data: ${JSON.stringify({
					event: event.content,
					createdAt: event.createdAt
				})}\n\n`
			)

			await new Promise(resolve => setTimeout(resolve, 500))

			// DELETE ORIGINAL AUDIO

			await fs.unlink(outputPath, async () => {
				event = await eventService.create(
					`Asl ovoz o'chirildi!`,
					sessionId
				)
				res.write(
					`data: ${JSON.stringify({
						event: event.content,
						createdAt: event.createdAt
					})}\n\n`
				)
			})

			await new Promise(resolve => setTimeout(resolve, 500))

			// TRANSCRIPTION

			event = await eventService.create(
				`Ovoz textga o'g'rilmoqda...`,
				sessionId
			)
			res.write(
				`data: ${JSON.stringify({
					event: event.content,
					createdAt: event.createdAt
				})}\n\n`
			)

			let i = 0

			const editedTexts = []

			while (i < segments.length) {
				const segmentName = segments[i]

				const segmentPath = path.resolve(
					outputDir,
					`${segmentName}.mp3`
				)

				await new Promise(resolve => setTimeout(resolve, 1000))

				try {
					event = await eventService.create(
						`Ovoz textga o'g'rilmoqda ${i}/${segments.length - 1}...`,
						sessionId
					)
					res.write(
						`data: ${JSON.stringify({
							event: event.content,
							createdAt: event.createdAt
						})}\n\n`
					)

					// google STT
					const gcsUri = await uploadAudioToGCS(segmentPath)
					const transcriptGoogle = await transcribeWithGoogle(gcsUri)

					if (!transcriptGoogle) {
						event = await eventService.create(
							`Xatolik ro'yberdi ${i}/${segments.length - 1}!!!`,
							sessionId
						)

						res.write(
							`data: ${JSON.stringify({
								event: event.content,
								createdAt: event.createdAt
							})}\n\n`
						)

						await new Promise(resolve => setTimeout(resolve, 500))

						event = await eventService.create(
							`Qayta qilinmoqda ${i}/${segments.length - 1}!!!`,
							sessionId
						)

						res.write(
							`data: ${JSON.stringify({
								event: event.content,
								createdAt: event.createdAt
							})}\n\n`
						)
						continue
					}

					// create Google text file
					// fs.writeFileSync(
					// 	path.join(transcriptsPath, `${segmentName}-google.txt`),
					// 	transcriptGoogle
					// )

					event = await eventService.create(
						`Ovoz textga o'g'rilmoqda ${i}/${segments.length - 1}...`,
						sessionId
					)
					res.write(
						`data: ${JSON.stringify({
							event: event.content,
							createdAt: event.createdAt
						})}\n\n`
					)

					// elevenlabs STT
					const transcriptElevenLabs =
						await transcribeAudioElevenLabs(segmentPath)

					if (!transcriptElevenLabs) {
						event = await eventService.create(
							`Xatolik ro'yberdi ${i}/${segments.length - 1}!!!`,
							sessionId
						)

						res.write(
							`data: ${JSON.stringify({
								event: event.content,
								createdAt: event.createdAt
							})}\n\n`
						)

						await new Promise(resolve => setTimeout(resolve, 500))

						event = await eventService.create(
							`Qayta qilinmoqda ${i}/${segments.length - 1}!!!`,
							sessionId
						)

						res.write(
							`data: ${JSON.stringify({
								event: event.content,
								createdAt: event.createdAt
							})}\n\n`
						)
						continue
					}

					// fs.writeFileSync(
					// 	path.join(transcriptsPath, `${segmentName}-elevenlabs.txt`),
					// 	transcriptElevenLabs
					// )

					event = await eventService.create(
						`Text tahrirlanmoqda ${i}/${segments.length - 1}...`,
						sessionId
					)
					res.write(
						`data: ${JSON.stringify({
							event: event.content,
							createdAt: event.createdAt
						})}\n\n`
					)

					const finalText = await editTranscribed(
						transcriptGoogle,
						transcriptElevenLabs
					)

					if (finalText) {
						editedTexts.push(finalText)

						// create final edited text
						// fs.writeFileSync(
						// 	path.join(transcriptsPath, `${segmentName}-edited.txt`),
						// 	finalText
						// )

						await fs.unlink(segmentPath, async () => {
							event = await eventService.create(
								`Ovoz o'chirilmoqda ${i}/${segments.length - 1}...`,
								sessionId
							)
							res.write(
								`data: ${JSON.stringify({
									event: event.content,
									createdAt: event.createdAt
								})}\n\n`
							)
						})

						await new Promise(resolve => setTimeout(resolve, 500))

						event = await eventService.create(
							`Text tahrirlandi ${i}/${segments.length - 1}...`,
							sessionId
						)
						res.write(
							`data: ${JSON.stringify({
								event: event.content,
								createdAt: event.createdAt
							})}\n\n`
						)
					} else {
						event = await eventService.create(
							`Tahrirda xatolik ro'y berdi, qayta tahrir qilinmoqda ${i}/${segments.length - 1}...`,
							sessionId
						)
						res.write(
							`data: ${JSON.stringify({
								event: event.content,
								createdAt: event.createdAt
							})}\n\n`
						)

						await new Promise(resolve => setTimeout(resolve, 500))

						continue
					}
				} catch (error) {
					console.error(error)
					continue
				}

				event = await eventService.create(
					`Text tayyor ${i}/${segments.length - 1}!`,
					sessionId
				)
				res.write(
					`data: ${JSON.stringify({
						event: event.content,
						createdAt: event.createdAt
					})}\n\n`
				)

				await new Promise(resolve => setTimeout(resolve, 500))
				i++
			}

			await userSession.completed(sessionId)

			event = await eventService.create(`Text tayyor bo'ldi!`, sessionId)
			res.write(
				`data: ${JSON.stringify({
					complated: true,
					data: {
						event: event.content,
						createdAt: event.createdAt
					}
				})}\n\n`
			)

			await new Promise(resolve => setTimeout(resolve, 500))

			const result = editedTexts
				.join('\n\n')
				.replace(/\(\(\((.*?)\)\)\)/g, '$1')
			const duration = performance.now() - startTime
			const { name: videoName } = path.parse(outputPath)

			fs.writeFileSync(
				path.join(transcriptsPath, `${videoName}-final-transcript.txt`),
				`🕒 Arginalni yozib chiqish uchun: ${formatDuration(
					duration
				)} vaqt ketdi!\n\n${downloadedVideoTitle}\n\n${convertToUzbekLatin(
					result
				)}`
			)

			event = await eventService.create(`Text jamlandi!`, sessionId)
			res.write(
				`data: ${JSON.stringify({
					event: event.content,
					createdAt: event.createdAt
				})}\n\n`
			)

			await new Promise(resolve => setTimeout(resolve, 500))

			const finalTranscript = `🕒 Arginalni yozib chiqish uchun: ${formatDuration(
				duration
			)} vaqt ketdi!\n\n${downloadedVideoTitle}\n\n${convertToUzbekLatin(
				result
			)}`

			event = await eventService.create(finalTranscript, sessionId)
			res.write(
				`data: ${JSON.stringify({
					event: event.content,
					createdAt: event.createdAt
				})}\n\n`
			)

			req.on('close', () => {
				console.log(`Connection closed for session ${sessionId}`)
			})
		} catch (error) {
			logger.error(error)
			res.status(500).json({ message: error.message })
		}
	}
)

export { router as eventController }
