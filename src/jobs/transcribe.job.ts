import ytdl from '@distube/ytdl-core'
import ffmpeg from 'fluent-ffmpeg'
import { performance } from 'perf_hooks'

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

// --- Configuration ---
// !! Store this securely in environment variables, DO NOT hardcode !!
const YOUTUBE_COOKIE = process.env.YOUTUBE_COOKIE || '' // Get from environment

if (!YOUTUBE_COOKIE) {
	// Warn or throw an error if the cookie is essential and missing
	logger.warn(
		'YOUTUBE_COOKIE environment variable is not set. Downloads may fail due to bot detection.'
	)
}

const LATEST_USER_AGENT =
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36' // Keep this updated

const ytdlRequestOptions = {
	headers: {
		'User-Agent': LATEST_USER_AGENT,
		Accept: '*/*',
		'Accept-Language': 'en-US,en;q=0.9',
		Referer: 'https://www.youtube.com/', // More generic Referer
		Origin: 'https://www.youtube.com', // More generic Origin
		DNT: '1',
		// Add the cookie header if it exists
		...(YOUTUBE_COOKIE && { cookie: YOUTUBE_COOKIE })
		// REMOVED: 'X-Forwarded-For': generateRandomIP() // DO NOT USE RANDOM IPs HERE
	}
}

const ytdlOptionsBase = {
	filter: 'audioonly' as const,
	quality: 'highestaudio',
	highWaterMark: 1 << 25, // 32MB buffer
	dlChunkSize: 0, // Use default chunk size based on highWaterMark
	retries: 3,
	requestOptions: ytdlRequestOptions
}

// REMOVED: generateRandomIP function is no longer needed

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
		await delay(1000) // Small delay before first YouTube interaction

		logger.info(`[${jobId}] Fetching video info for: ${url}`)
		// Use the options WITH cookies for getInfo as well
		const info = await ytdl.getInfo(url, {
			requestOptions: ytdlRequestOptions
		})
		const title = info.videoDetails.title
		const totalDuration = parseFloat(info.videoDetails.lengthSeconds)

		logger.info(
			`[${jobId}] Video Title: ${title}, Duration: ${totalDuration}s`
		)
		await transcriptService.updateTitle(jobId, title)

		await pushTranscriptionEvent(
			jobId,
			'Ovoz yuklanmoqda',
			false,
			broadcast
		)
		await delay(500)

		const segmentDuration = 150 // 2.5 minutes
		const numSegments = Math.ceil(totalDuration / segmentDuration)
		logger.info(`[${jobId}] Splitting into ${numSegments} segments.`)
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
			const segmentId = `segment_${jobId}_${i}` // For logging

			logger.info(
				`[${jobId}] Processing ${segmentId} (Start: ${segmentStartTime}s, Duration: ${actualDuration}s)`
			)

			// Stream segment from YouTube
			// Ensure ytdl uses the options WITH cookies for the download stream
			const segmentStream = ytdl(url, {
				...ytdlOptionsBase, // Includes requestOptions with cookies
				// 'begin' is deprecated, use 'range' - check ytdl-core docs if range needs start/end bytes instead of time
				// For time-based, 'begin' might still work or require specific format
				// Using begin for simplicity, verify if 'range' is needed for byte ranges
				begin: `${segmentStartTime}s`
			})

			const destFileName = `${segmentId}.mp3` // Use segmentId for filename
			let gcsUri = '' // Define gcsUri here to be accessible in finally

			try {
				// Use a Promise to handle the FFmpeg stream processing and GCS upload
				gcsUri = await new Promise<string>((resolve, reject) => {
					const ffmpegStream = ffmpeg(segmentStream)
						.format('mp3')
						.audioCodec('libmp3lame')
						// Setting audio quality might be less reliable than bitrate
						// Consider .audioBitrate('128k') for more predictable quality/size
						.audioQuality(2) // Corresponds to VBR preset V2 (~190kbps)
						.duration(actualDuration)
						.on('start', cmd =>
							logger.info(
								`[${jobId}] FFmpeg command for ${segmentId}: ${cmd}`
							)
						)
						.on('error', (err, stdout, stderr) => {
							logger.error(
								`[${jobId}] FFmpeg error processing ${segmentId}: ${err.message}`
							)
							logger.error(
								{
									// Error might not have stack directly, log what's available
									message: err.message,
									// stack: err.stack,
									stdout: stdout,
									stderr: stderr
								},
								`[${jobId}] FFmpeg Error Details for ${segmentId}`
							)
							// Reject the promise on FFmpeg error BEFORE upload starts
							reject(
								new Error(
									`FFmpeg processing failed for ${segmentId}: ${err.message}`
								)
							)
						})
						.pipe() // Get the output stream

					// Upload the processed stream to GCS
					uploadStreamToGCS(ffmpegStream, destFileName)
						.then(uri => {
							logger.info(
								`[${jobId}] Successfully uploaded ${segmentId} to ${uri}`
							)
							resolve(uri) // Resolve the promise with the GCS URI
						})
						.catch(uploadErr => {
							logger.error(
								`[${jobId}] GCS upload failed for ${segmentId}:`,
								uploadErr
							)
							reject(uploadErr) // Reject the promise on upload error
						})

					// Handle potential errors on the source YouTube stream itself
					segmentStream.on('error', ytdlErr => {
						logger.error(
							`[${jobId}] ytdl stream error for ${segmentId}:`,
							ytdlErr
						)
						// Ensure ffmpeg is potentially cleaned up if it started
						try {
							ffmpegStream.destroy()
						} catch (e) {
							/* ignore cleanup error */
						}
						reject(ytdlErr)
					})
				})

				// --- Transcription Steps ---
				logger.info(
					`[${jobId}] Transcribing ${segmentId} with Google...`
				)
				await pushTranscriptionEvent(
					jobId,
					`Google matnni o'girmoqda ${segmentNumber}/${numSegments}`,
					false,
					broadcast
				)

				const transcriptGoogle = await transcribeWithGoogle(gcsUri)
				if (!transcriptGoogle) {
					logger.warn(
						`[${jobId}] Google transcription failed for ${segmentId}. Retrying segment.`
					)
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi google matnida xatolik yuz berdi! Qayta urinilmoqda...`,
						false,
						broadcast
					)
					await delay(1000) // Delay before retry
					continue // Retry the current segment loop iteration
				}
				logger.info(
					`[${jobId}] Google transcription done for ${segmentId}.`
				)

				// ElevenLabs STT
				logger.info(
					`[${jobId}] Transcribing ${segmentId} with ElevenLabs...`
				)
				await pushTranscriptionEvent(
					jobId,
					`Elevenlabs matnni o'girmoqda ${segmentNumber}/${numSegments}`,
					false,
					broadcast
				)
				// Get a NEW stream for ElevenLabs
				const segmentStreamForElevenLabs =
					await getGCSFileStream(gcsUri)
				const transcriptElevenLabs = await transcribeAudioElevenLabs(
					segmentStreamForElevenLabs
				)
				if (!transcriptElevenLabs) {
					logger.warn(
						`[${jobId}] ElevenLabs transcription failed for ${segmentId}. Retrying segment.`
					)
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi elevenlabs matnida xatolik yuz berdi! Qayta urinilmoqda...`,
						false,
						broadcast
					)
					await delay(1000)
					continue // Retry the current segment
				}
				logger.info(
					`[${jobId}] ElevenLabs transcription done for ${segmentId}.`
				)

				// Gemini Edit
				logger.info(
					`[${jobId}] Editing ${segmentId} transcript with Gemini...`
				)
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
					logger.info(
						`[${jobId}] Gemini editing done for ${segmentId}.`
					)

					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi matn tahrirlandi! Ovoz o'chirilmoqda...`,
						false,
						broadcast
					)
					await delay(200) // Shorter delay maybe ok here
				} else {
					logger.warn(
						`[${jobId}] Gemini editing failed for ${segmentId}. Retrying segment.`
					)
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi Gemini tahririda xatolik! Qayta urinilmoqda...`,
						false,
						broadcast
					)
					await delay(1000)
					continue // Retry the current segment
				}
			} catch (err: any) {
				// Catch errors from Promise (ffmpeg, upload, ytdl stream) or transcription steps
				logger.error(
					`[${jobId}] Error processing ${segmentId}: ${err.message}`,
					err
				)
				// Decide if the error is fatal for the job or just this segment
				// For now, we log and attempt to continue to the finally block for cleanup
				// Consider adding retry logic here or marking the job as failed
				await pushTranscriptionEvent(
					jobId,
					`${segmentNumber}/${numSegments}-chi segmentda kutilmagan xatolik! Keyingisiga o'tilmoqda yoki to'xtatilmoqda...`, // Update message based on strategy
					false,
					broadcast
				)
				// Depending on the error, you might want to:
				// 1. continue; // Skip this segment and try the next one
				// 2. throw err; // Stop the whole job immediately (will be caught by outer try/catch)
				// 3. Implement specific retries for certain errors (e.g., network flakes)
				// For now, let's just continue to the next segment after cleanup
				// We need to ensure cleanup happens even if we 'continue' inside the try block
			} finally {
				// Cleanup GCS file regardless of success or failure of transcription steps
				if (gcsUri) {
					// Only attempt delete if upload was successful
					logger.info(`[${jobId}] Deleting GCS file: ${gcsUri}`)
					try {
						await deleteGCSFile(gcsUri)
						logger.info(`[${jobId}] Successfully deleted ${gcsUri}`)
					} catch (deleteErr) {
						logger.error(
							`[${jobId}] Failed to delete GCS file ${gcsUri}:`,
							deleteErr
						)
						// Don't stop the job for a cleanup failure, just log it.
					}
				} else {
					logger.warn(
						`[${jobId}] No GCS URI for ${segmentId}, skipping delete (upload likely failed).`
					)
				}
			}

			// If we reached here without continuing/throwing in catch, the segment processing (including cleanup attempt) is done.
			logger.info(`[${jobId}] Successfully processed ${segmentId}.`)
			await pushTranscriptionEvent(
				jobId,
				`${segmentNumber}/${numSegments}-chi matn tayyor!`,
				false,
				broadcast
			)
			await delay(500) // Delay before starting next segment
			i++ // Move to the next segment
		} // End while loop

		// --- Final Steps ---

		logger.info(`[${jobId}] All segments processed. Combining results.`)
		await pushTranscriptionEvent(
			jobId,
			'Matn tayyorlanmoqda...',
			false,
			broadcast
		)
		await delay(500)

		const combinedResult = editedTexts
			.join('\n\n')
			.replace(/\(\(\((.*?)\)\)\)/g, '$1') // Assuming this regex is for cleanup

		const duration = performance.now() - startTime
		logger.info(`[${jobId}] Job finished in ${formatDuration(duration)}.`)

		await pushTranscriptionEvent(jobId, `Matn jamlandi!`, false, broadcast)
		await delay(500)

		const finalTranscript = `<i style="display: block; font-style: italic; text-align: center;">🕒Arginalni yozib chiqish uchun: ${formatDuration(duration)} vaqt ketdi!</i><h1 style="font-weight: 700; font-size: 1.8rem; margin: 1rem 0; text-align: center; line-height: 1;">${title}</h1>\n\n<p style="text-indent: 30px;">${convertToUzbekLatin(combinedResult)}</p>`

		await transcriptService.saveFinalTranscript(jobId, finalTranscript)
		logger.info(`[${jobId}] Final transcript saved.`)

		// Mark session complete *before* sending final event
		try {
			await userSession.completed(sessionId)
			logger.info(`[${jobId}] Marked session ${sessionId} as completed.`)
		} catch (err) {
			logger.warn(
				`[${jobId}] Could not mark session ${sessionId} as completed:`,
				err
			)
		}

		// Send final SSE event
		await pushTranscriptionEvent(jobId, finalTranscript, true, broadcast)
		logger.info(`[${jobId}] Final completion event sent.`)
	} catch (err: any) {
		// Catch errors from getInfo or fatal errors during loop
		logger.error(
			`[${jobId}] Unrecoverable error in runTranscriptionJob: ${err.message}`,
			err
		)
		await transcriptService.error(jobId)
		// Optionally send a final error event via broadcast
		if (broadcast) {
			broadcast(`Xatolik yuz berdi: ${err.message}`, true) // Mark as complete (with error)
		}
	}
}
