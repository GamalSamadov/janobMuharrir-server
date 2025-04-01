import ffmpeg from 'fluent-ffmpeg'
import { performance } from 'perf_hooks'
import youtubeDl from 'youtube-dl-exec'

import {
	convertToUzbekLatin,
	deleteGCSFile,
	editTranscribed,
	formatDuration,
	getGCSFileStream,
	transcribeAudioElevenLabs,
	transcribeWithGoogle,
	uploadStreamToGCS
} from '@/jobs/helpers'
import { logger } from '@/lib/logger'
import { userSession } from '@/services/session/session.service'
import {
	transcriptEventService,
	transcriptService
} from '@/services/transcript/transcript.service'

const delay = (ms: number) => new Promise(res => setTimeout(res, ms))

export async function pushTranscriptionEvent(
	jobId: string,
	content: string,
	completed = false,
	broadcast?: (content: string, completed: boolean) => void
) {
	await transcriptEventService.create(jobId, content, completed)

	if (broadcast) {
		broadcast(content, completed)
	}
}

export async function runTranscriptionJob(
	jobId: string,
	sessionId: string,
	url: string,
	broadcast?: (content: string, completed: boolean) => void
) {
	const startTime = performance.now()

	try {
		await transcriptService.running(jobId)

		logger.info(`Workspaceing video info for: ${url}`)
		const info = await youtubeDl(url, {
			dumpSingleJson: true,
			noWarnings: true,
			getUrl: true,
			quiet: true,
			cookies: './youtube-cookies.txt'
		})

		const videoInfo = typeof info === 'string' ? JSON.parse(info) : info

		const title = videoInfo.title || 'Untitled Video'
		const totalDuration = videoInfo.duration // duration is usually in seconds

		if (typeof totalDuration !== 'number') {
			logger.error(
				`Could not determine video duration for ${url}. Info:`,
				videoInfo
			)
			await transcriptService.error(jobId)
			await pushTranscriptionEvent(
				jobId,
				"Video davomiyligini aniqlab bo'lmadi",
				true,
				broadcast
			)
			return // Stop processing
		}

		logger.info(`Video Title: ${title}, Duration: ${totalDuration}s`)

		await transcriptService.updateTitle(jobId, title)

		await pushTranscriptionEvent(
			jobId,
			'Ovoz yuklanmoqda',
			false,
			broadcast
		)
		await delay(500)

		logger.info(`Getting direct audio stream URL for: ${url}`)
		const audioUrl = await youtubeDl(url, {
			dumpSingleJson: true,
			noWarnings: true,
			getUrl: true,
			quiet: true,
			cookies: './youtube-cookies.txt'
		})

		// youtube-dl-exec with getUrl returns a string (the URL)
		if (typeof audioUrl !== 'string' || !audioUrl.startsWith('http')) {
			logger.error(
				`Failed to get a valid audio stream URL for ${url}. Output: ${audioUrl}`
			)
			await transcriptService.error(jobId)
			await pushTranscriptionEvent(
				jobId,
				"Audio manzilini olib bo'lmadi",
				true,
				broadcast
			)
			return
		}
		logger.info(`Obtained audio stream URL.`)

		const segmentDuration = 150 // 2.5 minutes
		const numSegments = Math.ceil(totalDuration / segmentDuration)
		await pushTranscriptionEvent(
			jobId,
			`Ovoz ${numSegments}ga taqsimlanmoqda`,
			false,
			broadcast
		)
		await delay(500)

		// TRANSCRIPTION

		await pushTranscriptionEvent(
			jobId,
			`Matnga o'g'rilmoqda`,
			false,
			broadcast
		)

		const editedTexts: string[] = []
		let i = 0

		while (i < numSegments) {
			const segmentNumber = i + 1
			const segmentStartTime = i * segmentDuration
			const actualDuration = Math.min(
				segmentDuration,
				totalDuration - segmentStartTime
			)

			if (actualDuration <= 0) {
				logger.warn(
					`Skipping segment ${segmentNumber} due to zero or negative duration.`
				)
				i++
				continue
			}

			logger.info(
				`Processing segment ${segmentNumber}/${numSegments}, Start: ${segmentStartTime}s, Duration: ${actualDuration}s`
			)

			const destFileName = `segment_${jobId}_${i}.mp3`
			let gcsUri: string | null = null

			try {
				const ffmpegStream = ffmpeg(audioUrl)
					.inputOption(`-ss ${segmentStartTime}`)
					.inputOption('-nostdin')
					.duration(actualDuration)
					.format('mp3')
					.audioCodec('libmp3lame')
					.audioQuality(2)
					.on('start', cmd =>
						logger.info(
							`FFmpeg command segment ${segmentNumber}: ${cmd}`
						)
					)
					.on('error', (err, stdout, stderr) => {
						logger.error(
							`FFmpeg error processing segment ${segmentNumber}:`
						)
						logger.error(
							{
								message: err.message,
								stack: err.stack,
								stdout: stdout,
								stderr: stderr,
								segmentStartTime,
								actualDuration
							},
							'FFmpeg Error Details:'
						)
					})
					.pipe()

				gcsUri = await uploadStreamToGCS(ffmpegStream, destFileName)
				logger.info(
					`Segment ${segmentNumber} uploaded to GCS: ${gcsUri}`
				)

				await pushTranscriptionEvent(
					jobId,
					`Google matnni o'girmoqda ${segmentNumber}/${numSegments}`,
					false,
					broadcast
				)

				const transcriptGoogle = await transcribeWithGoogle(gcsUri)

				if (!transcriptGoogle) {
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi google matnida xatolik yuz berdi! Qayta urinilmoqda...`,
						false,
						broadcast
					)
					await delay(1000)
					continue
				}

				// elevenlabs STT
				await pushTranscriptionEvent(
					jobId,
					`Elevenlabs matnni o'girmoqda ${segmentNumber}/${numSegments}`,
					false,
					broadcast
				)

				const segmentStreamForElevenLabs =
					await getGCSFileStream(gcsUri)
				const transcriptElevenLabs = await transcribeAudioElevenLabs(
					segmentStreamForElevenLabs
				)

				if (!transcriptElevenLabs) {
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi elevenlabs matnida xatolik yuz berdi! Qayta urinilmoqda...`,
						false,
						broadcast
					)
					await delay(1000)
					continue
				}

				await pushTranscriptionEvent(
					jobId,
					`Matnni Gemini tahrirlamoqda ${segmentNumber}/${numSegments}!`,
					false,
					broadcast
				)

				const finalText = await editTranscribed(
					transcriptGoogle,
					transcriptElevenLabs
				)

				if (finalText) {
					editedTexts.push(finalText)
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi matn tayyor! Ovoz o'chirilmoqda...`,
						false,
						broadcast
					)
					await delay(500)
				} else {
					await pushTranscriptionEvent(
						jobId,
						`Gemini tahririda xatolik (${segmentNumber}/${numSegments})! Qayta urinilmoqda...`,
						false,
						broadcast
					)
					await delay(1000)
					continue
				}
			} catch (segmentErr) {
				logger.error(
					`Error processing segment ${segmentNumber}:`,
					segmentErr
				)
				// Push an error event for this segment
				await pushTranscriptionEvent(
					jobId,
					`Segment ${segmentNumber}/${numSegments} da xatolik yuz berdi. Keyingisiga o'tilmoqda.`,
					false,
					broadcast
				)
				await delay(1000)
			} finally {
				if (gcsUri) {
					try {
						logger.info(
							`Deleting GCS file for segment ${segmentNumber}: ${gcsUri}`
						)
						await deleteGCSFile(gcsUri)
					} catch (deleteErr) {
						logger.error(
							`Failed to delete segment ${segmentNumber} from GCS (${gcsUri}):`,
							deleteErr
						)
					}
				}
			}

			// If we successfully processed the segment (no 'continue' was hit in try/catch)
			i++ // Move to the next segment
			await delay(200) // Small delay between segments
		}

		// --- Combine final results ---
		if (editedTexts.length === 0 && numSegments > 0) {
			logger.error(
				`Job ${jobId} finished, but no segments were successfully transcribed.`
			)
			await transcriptService.error(jobId)
			await pushTranscriptionEvent(
				jobId,
				"Matn qismlarini o'girib bo'lmadi.",
				true,
				broadcast
			)
			return // Exit early
		}

		try {
			await userSession.completed(sessionId)
		} catch (err) {
			logger.warn(
				`Could not mark session as completed for sessionId=${sessionId}`,
				err
			)
		}

		await pushTranscriptionEvent(
			jobId,
			'Matn tayyorlanmoqda...',
			false,
			broadcast
		)
		await delay(500)

		const combinedResult = editedTexts
			.join('\n\n')
			.replace(/\(\(\((.*?)\)\)\)/g, '$1') // Consider if this regex is still needed
		const duration = performance.now() - startTime

		await pushTranscriptionEvent(jobId, `Text jamlandi!`, false, broadcast)
		await delay(500)

		const finalTranscript = `<i style="display: block; font-style: italic; text-align: center;">🕒Arginalni yozib chiqish uchun: ${formatDuration(duration)} vaqt ketdi!</i><h1 style="font-weight: 700; font-size: 1.8rem; margin: 1rem 0; text-align: center; line-height: 1;">${title}</h1>\n\n<p style="text-indent: 30px;">${convertToUzbekLatin(combinedResult)}</p>`

		await transcriptService.saveFinalTranscript(jobId, finalTranscript)
		logger.info(
			`Transcription job ${jobId} completed successfully in ${formatDuration(duration)}.`
		)

		// Send final SSE event
		await pushTranscriptionEvent(jobId, finalTranscript, true, broadcast)
	} catch (err) {
		// Catch errors from initial setup or unexpected loop errors
		logger.error(`runTranscriptionJob error for job ${jobId}:`, err)
		await transcriptService.error(jobId)
		await pushTranscriptionEvent(
			jobId,
			`Umumiy xatolik yuz berdi: ${err instanceof Error ? err.message : String(err)}`,
			true,
			broadcast
		) // Mark as completed with error
	}
}
