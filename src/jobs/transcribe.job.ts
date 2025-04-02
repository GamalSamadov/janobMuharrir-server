import ytdl from '@distube/ytdl-core'
import ffmpeg from 'fluent-ffmpeg'
import { performance } from 'perf_hooks'
import { PassThrough } from 'stream'

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

const ytdlRequestOptions = {
	headers: {
		cookie: process.env.YOUTUBE_COOKIE
	}
}

export async function pushTranscriptionEvent(
	jobId: string,
	content: string,
	completed = false,
	broadcast?: (content: string, completed: boolean) => void
) {
	const message =
		typeof content === 'string' ? content : JSON.stringify(content)
	await transcriptEventService.create(jobId, message, completed)

	if (broadcast) {
		broadcast(message, completed)
	}
}

export async function runTranscriptionJob(
	jobId: string,
	sessionId: string,
	url: string,
	broadcast?: (content: string, completed: boolean) => void
) {
	const startTime = performance.now()
	let jobStatusUpdated = false

	try {
		await transcriptService.running(jobId)
		jobStatusUpdated = true

		await delay(1000)

		let info: ytdl.videoInfo

		try {
			logger.info(`Fetching video info for URL: ${url}`)
			info = await ytdl.getInfo(url, {
				requestOptions: ytdlRequestOptions
			})
			logger.info(
				`Successfully fetched video info for title: ${info.videoDetails.title}`
			)
		} catch (err: any) {
			logger.error(
				{ error: err.message, stack: err.stack, url: url },
				'Failed to get video info from ytdl. Check URL and YOUTUBE_COOKIE validity.'
			)
			const errorMessage =
				err.message?.includes('sign in') ||
				err.message?.includes('login required')
					? `Video ma'lumotlarini olib bo'lmadi. YouTube cookie eskirgan yoki noto'g'ri bo'lishi mumkin. (${err.message})`
					: `Xatolik: Video ma'lumotlarini olib bo'lmadi. URL manzilini tekshiring. (${err.message || 'Unknown ytdl error'})`

			await pushTranscriptionEvent(jobId, errorMessage, false, broadcast)
			await transcriptService.error(jobId)
			jobStatusUpdated = true
			return // Stop processing
		}
		// --- End Get Video Info ---

		const title = info.videoDetails.title
		const totalDuration = parseFloat(info.videoDetails.lengthSeconds)

		// Validate duration
		if (isNaN(totalDuration) || totalDuration <= 0) {
			logger.error(
				`Invalid video duration received: ${info.videoDetails.lengthSeconds}`
			)
			await pushTranscriptionEvent(
				jobId,
				`Xatolik: Video davomiyligi noto'g'ri (${info.videoDetails.lengthSeconds}).`,
				false,
				broadcast
			)
			await transcriptService.error(jobId)
			jobStatusUpdated = true
			return
		}

		await transcriptService.updateTitle(jobId, title)
		await pushTranscriptionEvent(
			jobId,
			'Ovoz yuklanmoqda...',
			false,
			broadcast
		)
		await delay(500)

		const segmentDuration = 150 // 2.5 minutes
		const numSegments = Math.ceil(totalDuration / segmentDuration)
		await pushTranscriptionEvent(
			jobId,
			`Ovoz ${numSegments} bo'lakka taqsimlanmoqda...`,
			false,
			broadcast
		)
		await delay(500)

		await pushTranscriptionEvent(
			jobId,
			`Matnga o'girish boshlanmoqda...`,
			false,
			broadcast
		)

		const editedTexts: string[] = []
		let i = 0
		const bucketName = process.env.GOOGLE_CLOUD_BUCKET_NAME
		if (!bucketName) {
			throw new Error(
				'GOOGLE_CLOUD_BUCKET_NAME environment variable is not set.'
			)
		}

		while (i < numSegments) {
			const segmentNumber = i + 1
			const segmentStartTime = i * segmentDuration
			const actualDuration = Math.min(
				segmentDuration,
				totalDuration - segmentStartTime
			)
			const destFileName = `segment_${jobId}_${i}.mp3`
			const gcsUri = `gs://${bucketName}/${destFileName}`

			let segmentProcessedSuccessfully = false
			let attempt = 0
			const maxAttempts = 2

			while (!segmentProcessedSuccessfully && attempt < maxAttempts) {
				attempt++
				if (attempt > 1) {
					logger.warn(
						`Retrying segment ${segmentNumber}/${numSegments} (Attempt ${attempt})`
					)
					await pushTranscriptionEvent(
						jobId,
						`Bo'lak ${segmentNumber}/${numSegments} qayta ishlanmoqda (Urinish ${attempt})...`,
						false,
						broadcast
					)
					await delay(2000)
				}

				let gcsUploadSucceeded = false
				let segmentStreamError: Error | null = null

				try {
					await pushTranscriptionEvent(
						jobId,
						`Bo'lak ${segmentNumber}/${numSegments} yuklanmoqda...`,
						false,
						broadcast
					)

					// --- Stream Segment with Error Handling ---
					const segmentStream = ytdl(url, {
						requestOptions: ytdlRequestOptions,
						quality: 'lowestaudio',
						filter: 'audioonly',
						begin: `${segmentStartTime}s`
					})
						.on('error', (err: Error) => {
							logger.error(
								{
									error: err.message,
									stack: err.stack,
									segment: segmentNumber
								},
								`ytdl stream error for segment ${segmentNumber}`
							)
							segmentStreamError = err
						})
						.on('progress', (_, downloaded, total) => {
							logger.debug(
								`Segment ${segmentNumber} download progress: ${downloaded}/${total}`
							)
						})

					const passThrough = new PassThrough()
					segmentStream.pipe(passThrough)

					await new Promise((resolve, reject) => {
						const timeout = setTimeout(() => {
							if (segmentStreamError) {
								reject(
									new Error(
										`ytdl stream failed immediately for segment ${segmentNumber}: ${segmentStreamError.message}`
									)
								)
							} else {
								resolve(true)
							}
						}, 200)

						segmentStream.once('end', () => {
							clearTimeout(timeout)
							resolve(true)
						})
						segmentStream.once('error', err => {
							clearTimeout(timeout)
							reject(err)
						})
					})

					logger.info(
						`Starting FFmpeg for segment ${segmentNumber}...`
					)
					const ffmpegStream = ffmpeg(passThrough)
						.format('mp3')
						.audioCodec('libmp3lame')
						.audioQuality(5)
						.duration(actualDuration)
						.on('start', cmd =>
							logger.info(
								`FFmpeg started for segment ${segmentNumber}: ${cmd}`
							)
						)
						.on('error', (err, stdout, stderr) => {
							logger.error(
								{
									message: err.message,
									stack: err.stack,
									stdout: stdout,
									stderr: stderr,
									segment: segmentNumber
								},
								`FFmpeg error processing segment ${segmentNumber}`
							)
						})
						.pipe()

					// Upload to GCS
					await uploadStreamToGCS(ffmpegStream, destFileName)
					gcsUploadSucceeded = true // Mark upload as successful
					logger.info(
						`Segment ${segmentNumber}/${numSegments} successfully uploaded to ${gcsUri}`
					)

					// --- Google Transcription ---
					await pushTranscriptionEvent(
						jobId,
						`Google matnni o'girmoqda ${segmentNumber}/${numSegments}...`,
						false,
						broadcast
					)
					const transcriptGoogle = await transcribeWithGoogle(gcsUri)
					if (!transcriptGoogle) {
						logger.error(
							`Google transcription returned empty/null for segment ${segmentNumber}`
						)
						await pushTranscriptionEvent(
							jobId,
							`${segmentNumber}/${numSegments}-chi Google matnida xatolik (bo'sh natija).`,
							false,
							broadcast
						)
						continue // Retry segment processing loop
					}
					logger.info(
						`Google transcription done for segment ${segmentNumber}`
					)

					// --- ElevenLabs Transcription ---
					await pushTranscriptionEvent(
						jobId,
						`ElevenLabs matnni o'girmoqda ${segmentNumber}/${numSegments}...`,
						false,
						broadcast
					)
					// Get a *new* stream for ElevenLabs each time
					const segmentStreamForElevenLabs =
						await getGCSFileStream(gcsUri)
					const transcriptElevenLabs =
						await transcribeAudioElevenLabs(
							segmentStreamForElevenLabs
						)
					if (!transcriptElevenLabs) {
						logger.error(
							`ElevenLabs transcription returned empty/null for segment ${segmentNumber}`
						)
						await pushTranscriptionEvent(
							jobId,
							`${segmentNumber}/${numSegments}-chi ElevenLabs matnida xatolik (bo'sh natija).`,
							false,
							broadcast
						)
						continue // Retry segment processing loop
					}
					logger.info(
						`ElevenLabs transcription done for segment ${segmentNumber}`
					)

					// --- Gemini Editing ---
					await pushTranscriptionEvent(
						jobId,
						`Matnni Gemini tahrirlamoqda ${segmentNumber}/${numSegments}...`,
						false,
						broadcast
					)
					const finalText = await editTranscribed(
						transcriptGoogle,
						transcriptElevenLabs
					)
					if (!finalText) {
						logger.error(
							`Gemini editing returned empty/null for segment ${segmentNumber}`
						)
						await pushTranscriptionEvent(
							jobId,
							`${segmentNumber}/${numSegments}-chi Gemini tahririda xatolik (bo'sh natija).`,
							false,
							broadcast
						)
						continue // Retry segment processing loop
					}
					logger.info(
						`Gemini editing done for segment ${segmentNumber}`
					)

					// If all steps succeeded
					editedTexts.push(finalText)
					segmentProcessedSuccessfully = true // Mark as successful
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi bo'lak tayyor!`,
						false,
						broadcast
					)
				} catch (segmentErr: any) {
					logger.error(
						{
							error: segmentErr.message,
							stack: segmentErr.stack,
							segment: segmentNumber,
							attempt: attempt
						},
						`Error processing segment ${segmentNumber} on attempt ${attempt}`
					)
					await pushTranscriptionEvent(
						jobId,
						`Xatolik (${segmentNumber}/${numSegments}, urinish ${attempt}): ${segmentErr.message}`,
						false,
						broadcast
					)
					// Specific error check for ytdl authentication/access failure
					const errorMsgLower =
						segmentErr.message?.toLowerCase() || ''
					if (
						errorMsgLower.includes('status code: 403') ||
						errorMsgLower.includes('sign in') ||
						errorMsgLower.includes('login required') ||
						errorMsgLower.includes('private video') ||
						errorMsgLower.includes('age restricted')
					) {
						logger.error(
							'Potential YouTube authentication/access error detected. Check YOUTUBE_COOKIE validity or video permissions.'
						)
						await pushTranscriptionEvent(
							jobId,
							`YouTube kirish xatosi yoki video maxfiy/yosh cheklangan bo'lishi mumkin. Cookie'ni yangilang.`,
							false,
							broadcast
						)
						// Abort the job immediately if it's an auth error - retrying won't help
						throw new Error(
							`Aborting job due to YouTube access error on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}
					await delay(1000) // Wait a bit before potential retry for other errors
				} finally {
					// --- Cleanup GCS File ---
					if (gcsUploadSucceeded) {
						try {
							logger.info(
								`Attempting to delete GCS file: ${destFileName}`
							)
							await deleteGCSFile(destFileName) // Use filename helper expects
							logger.info(
								`Successfully deleted GCS file: ${destFileName}`
							)
							await pushTranscriptionEvent(
								jobId,
								`Vaqtinchalik fayl (${segmentNumber}/${numSegments}) o'chirildi.`,
								false,
								broadcast
							)
						} catch (deleteErr: any) {
							logger.error(
								{
									error: deleteErr.message,
									file: destFileName
								},
								`Failed to delete GCS segment file: ${destFileName}`
							)
							// Don't prevent job completion for failed cleanup
						}
					}
					await delay(500) // Small delay after segment processing/cleanup
				}
			} // End retry loop (while !segmentProcessedSuccessfully)

			// If segment failed after all attempts
			if (!segmentProcessedSuccessfully) {
				logger.error(
					`Segment ${segmentNumber}/${numSegments} failed after ${maxAttempts} attempts. Aborting job.`
				)
				await pushTranscriptionEvent(
					jobId,
					`Xatolik: ${segmentNumber}/${numSegments}-chi bo'lakni ${maxAttempts} urinishda ham ishlab bo'lmadi. Jarayon to'xtatildi.`,
					false,
					broadcast
				)
				throw new Error(
					`Failed to process segment ${segmentNumber} after ${maxAttempts} attempts.`
				) // Throw to trigger main catch block
			}

			i++ // Move to the next segment
		} // End segment loop (while i < numSegments)

		// --- Combine and Finalize ---
		logger.info(
			`All ${numSegments} segments processed successfully for job ${jobId}. Combining...`
		)

		try {
			await userSession.completed(sessionId)
			logger.info(`Marked session ${sessionId} as completed.`)
		} catch (err: any) {
			logger.warn(
				{ error: err.message, sessionId: sessionId },
				`Could not mark session as completed for sessionId=${sessionId}`
			)
		}

		await pushTranscriptionEvent(
			jobId,
			'Yakuniy matn tayyorlanmoqda...',
			false,
			broadcast
		)
		await delay(500)

		const combinedResult = editedTexts
			.join('\n\n')
			.replace(/\(\(\((.*?)\)\)\)/g, '$1')
			.replace(/(\n\s*){3,}/g, '\n\n')

		const duration = performance.now() - startTime
		logger.info(`Job ${jobId} completed in ${formatDuration(duration)}`)

		await pushTranscriptionEvent(
			jobId,
			`Yakuniy matn jamlandi!`,
			false,
			broadcast
		)
		await delay(500)

		const finalTranscript = `<i style="display: block; font-style: italic; text-align: center;">🕒 Transkripsiya uchun ${formatDuration(duration)} vaqt ketdi.</i><h1 style="font-weight: 700; font-size: 1.8rem; margin: 1rem 0; text-align: center; line-height: 1;">${title}</h1>\n\n<p style="text-indent: 30px;">${convertToUzbekLatin(combinedResult)}</p>`

		await transcriptService.saveFinalTranscript(jobId, finalTranscript)
		logger.info(`Final transcript saved for job ${jobId}.`)

		await pushTranscriptionEvent(jobId, finalTranscript, true, broadcast)
		jobStatusUpdated = true // Mark as updated
	} catch (err: any) {
		logger.error(
			{ error: err.message, stack: err.stack, jobId: jobId },
			'Critical error in runTranscriptionJob'
		)
		if (!jobStatusUpdated) {
			try {
				await transcriptService.error(jobId)
			} catch (dbErr: any) {
				logger.error(
					{ error: dbErr.message, jobId: jobId },
					'Failed to mark job as error in DB during catch block'
				)
			}
		}
		if (broadcast) {
			try {
				const clientErrorMessage = `Serverda kutilmagan xatolik yuz berdi: ${err.message || 'No details'}`
				await pushTranscriptionEvent(
					jobId,
					clientErrorMessage,
					true,
					broadcast
				) // Mark SSE as 'completed' (stream ends)
			} catch (sseErr: any) {
				logger.error(
					{ error: sseErr.message, jobId: jobId },
					'Failed to send final error SSE event'
				)
			}
		}
	}
}
