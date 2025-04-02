import { spawn } from 'child_process'
// Used for yt-dlp
import ffmpeg from 'fluent-ffmpeg'
import { performance } from 'perf_hooks'
import { PassThrough, Readable } from 'stream'

// Added Readable type

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

// --- Helper Functions for yt-dlp ---

interface VideoInfo {
	title: string
	duration: number // in seconds
}

/**
 * Fetches video metadata (title, duration) using yt-dlp.
 */
async function getVideoInfoWithYtDlp(
	youtubeUrl: string,
	cookie?: string
): Promise<VideoInfo> {
	return new Promise((resolve, reject) => {
		const args = [
			'--no-warnings',
			'--no-call-home', // Prevent potential external calls
			'--ignore-config', // Ensure clean run
			'--dump-json', // Get metadata as JSON
			'--skip-download', // Don't download the video itself
			youtubeUrl
		]

		if (cookie) {
			// Add cookie header if provided
			args.unshift('--add-header', `Cookie:${cookie}`)
			logger.info('Using provided cookie with yt-dlp info command.')
		} else {
			logger.warn('No YouTube cookie provided for yt-dlp info command.')
		}

		logger.info(
			`Spawning yt-dlp to get video info: yt-dlp ${args.join(' ')}`
		)

		const ytDlpProcess = spawn('yt-dlp', args)
		let jsonData = ''
		let errorData = ''

		ytDlpProcess.stdout.on('data', data => {
			jsonData += data.toString()
		})

		ytDlpProcess.stderr.on('data', data => {
			const errLine = data.toString()
			// Log stderr, but filter common non-fatal warnings if needed
			if (!errLine.includes('WARNING:')) {
				// Example filter
				errorData += errLine
				logger.warn(`yt-dlp info stderr: ${errLine.trim()}`)
			}
		})

		ytDlpProcess.on('error', err => {
			logger.error(
				{ error: err },
				'Failed to spawn yt-dlp process for info.'
			)
			reject(new Error(`Failed to start yt-dlp for info: ${err.message}`))
		})

		ytDlpProcess.on('close', code => {
			if (code !== 0) {
				logger.error(
					`yt-dlp info process exited with code ${code}. Stderr: ${errorData}`
				)
				if (
					errorData.includes('Private video') ||
					errorData.includes('login required') ||
					errorData.includes('confirm your age') ||
					errorData.includes('unavailable') ||
					errorData.includes('Sign in') ||
					errorData.includes('403')
				) {
					reject(
						new Error(
							`YouTube access error (yt-dlp info): Video might be private/unavailable, require login, or cookie invalid. Code ${code}.`
						)
					)
				} else {
					reject(
						new Error(
							`yt-dlp info process exited with code ${code}. Review stderr logs.`
						)
					)
				}
			} else {
				try {
					if (!jsonData) {
						logger.error(
							'yt-dlp info command closed successfully but produced no JSON output.'
						)
						reject(
							new Error(
								'yt-dlp returned empty JSON output for video info.'
							)
						)
						return
					}
					const info = JSON.parse(jsonData)
					// Check for essential fields
					if (
						info &&
						info.title &&
						typeof info.duration === 'number'
					) {
						logger.info(
							`yt-dlp info successful for title: ${info.title}, duration: ${info.duration}s`
						)
						resolve({
							title: info.title,
							duration: info.duration
						})
					} else {
						logger.error(
							{ parsedJson: info },
							'yt-dlp JSON missing title or duration, or invalid structure.'
						)
						reject(
							new Error(
								'Failed to parse required title or duration from yt-dlp JSON.'
							)
						)
					}
				} catch (parseErr: any) {
					logger.error(
						{ error: parseErr, rawJson: jsonData },
						'Failed to parse yt-dlp JSON output.'
					)
					reject(
						new Error(
							`Failed to parse yt-dlp JSON info: ${parseErr.message}`
						)
					)
				}
			}
		})
	})
}

/**
 * Streams a specific audio segment using yt-dlp and its ffmpeg postprocessor.
 */
async function streamAudioWithYtDlp(
	youtubeUrl: string,
	startTime: number,
	duration: number,
	cookie?: string
): Promise<Readable> {
	// Return type is Readable stream

	const args = [
		'--no-warnings',
		'--no-call-home',
		'--ignore-config',
		// Select best audio format, prioritizing m4a for potential direct copy by ffmpeg
		'-f',
		'bestaudio[ext=m4a]/bestaudio/best',
		'--output',
		'-', // Output raw audio data to stdout

		// Use FFmpeg postprocessor for accurate seeking/cutting.
		// -ss seeks, -t sets duration. -c:a copy tries to avoid re-encoding.
		'--postprocessor-args',
		`ffmpeg:-ss ${startTime} -t ${duration} -c:a copy`,

		youtubeUrl
	]

	if (cookie) {
		args.unshift('--add-header', `Cookie:${cookie}`)
		logger.info('Using provided cookie with yt-dlp stream command.')
	} else {
		logger.warn('No YouTube cookie provided for yt-dlp stream command.')
	}

	logger.info(`Spawning yt-dlp for audio segment: yt-dlp ${args.join(' ')}`)

	const ytDlpProcess = spawn('yt-dlp', args, {
		stdio: ['ignore', 'pipe', 'pipe'] // ignore stdin, pipe stdout/stderr
	})

	// Return the stdout stream directly. Error handling is attached below.
	const outputAudioStream = ytDlpProcess.stdout

	ytDlpProcess.stderr.on('data', data => {
		const errLine = data.toString()
		// Log stderr, can be noisy with ffmpeg messages. Filter if needed.
		if (!errLine.includes('WARNING:')) {
			// Example filter
			logger.warn(`yt-dlp stream stderr: ${errLine.trim()}`)
		}
	})

	ytDlpProcess.on('error', err => {
		logger.error(
			{ error: err },
			'Failed to spawn yt-dlp process for streaming.'
		)
		// Emit error on the stream that ffmpeg will consume
		outputAudioStream.emit(
			'error',
			new Error(`Failed to start yt-dlp stream process: ${err.message}`)
		)
	})

	ytDlpProcess.on('close', code => {
		if (code !== 0) {
			logger.error(
				`yt-dlp stream process exited with error code ${code}. Check stderr logs.`
			)
			// Emit error on the stream consumed by ffmpeg
			outputAudioStream.emit(
				'error',
				new Error(
					`yt-dlp stream process exited with error code ${code}`
				)
			)
		} else {
			logger.info('yt-dlp stream process finished successfully.')
			// Stream should end naturally when process closes stdout. If ffmpeg hangs,
			// might need explicit stream.end() here, but usually pipe handles it.
		}
	})

	return outputAudioStream
}

// --- Main Transcription Job Logic ---

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

		// --- Get Video Info using yt-dlp ---
		let videoInfo: VideoInfo
		const youtubeCookie = process.env.YOUTUBE_COOKIE

		logger.info(
			{
				hasCookie: !!youtubeCookie,
				cookieLength: youtubeCookie?.length ?? 0
			},
			'Checking YouTube cookie presence before yt-dlp info call'
		)

		try {
			logger.info(`Fetching video info via yt-dlp for URL: ${url}`)
			videoInfo = await getVideoInfoWithYtDlp(url, youtubeCookie)
			logger.info(
				`Successfully fetched video info via yt-dlp for title: ${videoInfo.title}`
			)
		} catch (err: any) {
			logger.error(
				{ error: err.message, stack: err.stack, url: url },
				'Failed to get video info from yt-dlp.'
			)
			const errorMessage = err.message?.includes('YouTube access error')
				? `Video ma'lumotlarini olib bo'lmadi (yt-dlp). YouTube kirish xatosi (maxfiy/yosh cheklangan/cookie?) bo'lishi mumkin. (${err.message})`
				: `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). URL yoki serverni tekshiring. (${err.message || 'Unknown yt-dlp info error'})`

			await pushTranscriptionEvent(jobId, errorMessage, false, broadcast)
			await transcriptService.error(jobId)
			jobStatusUpdated = true
			return
		}
		// --- End Get Video Info ---

		const title = videoInfo.title
		const totalDuration = videoInfo.duration

		if (isNaN(totalDuration) || totalDuration <= 0) {
			logger.error(`Invalid video duration from yt-dlp: ${totalDuration}`)
			await pushTranscriptionEvent(
				jobId,
				`Xatolik: Video davomiyligi noto'g'ri (${totalDuration}).`,
				false,
				broadcast
			)
			await transcriptService.error(jobId)
			jobStatusUpdated = true
			return
		}

		// Update job title in DB (ensure schema field name matches)
		try {
			await transcriptService.updateTitle(jobId, title)
		} catch (updateErr: any) {
			logger.warn(
				{ jobId, title, error: updateErr.message },
				'Failed to update job title in database.'
			)
			// Continue processing even if title update fails
		}

		await pushTranscriptionEvent(
			jobId,
			'Ovoz yuklanmoqda (yt-dlp)...',
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
			logger.error(
				'CRITICAL: GOOGLE_CLOUD_BUCKET_NAME environment variable is not set.'
			)
			await pushTranscriptionEvent(
				jobId,
				'Server konfiguratsiya xatosi: Bucket nomi topilmadi.',
				false,
				broadcast
			)
			await transcriptService.error(jobId)
			jobStatusUpdated = true
			return
		}

		// --- Segment Processing Loop ---
		while (i < numSegments) {
			const segmentNumber = i + 1
			const segmentStartTime = i * segmentDuration
			const actualDuration = Math.min(
				segmentDuration,
				totalDuration - segmentStartTime
			)
			// Ensure duration is slightly positive if calculation results in zero/negative
			const safeActualDuration = Math.max(0.1, actualDuration)

			const destFileName = `segment_${jobId}_${i}.mp3`
			const gcsUri = `gs://${bucketName}/${destFileName}`

			let segmentProcessedSuccessfully = false
			let attempt = 0
			const maxAttempts = 2 // Retries per segment

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

				try {
					await pushTranscriptionEvent(
						jobId,
						`Bo'lak ${segmentNumber}/${numSegments} yuklanmoqda (yt-dlp)...`,
						false,
						broadcast
					)

					// --- Stream Audio Segment using yt-dlp ---
					logger.info(
						`Attempting segment ${segmentNumber} download via yt-dlp (start: ${segmentStartTime}s, duration: ${safeActualDuration}s)...`
					)
					const audioStream = await streamAudioWithYtDlp(
						url,
						segmentStartTime,
						safeActualDuration, // Use safe duration
						youtubeCookie
					)
					// --- End yt-dlp Streaming ---

					// --- Create ffmpeg command ---
					logger.info(
						`Starting FFmpeg encoding for segment ${segmentNumber}...`
					)
					const ffmpegCommand = ffmpeg(audioStream)
						.format('mp3')
						.audioCodec('libmp3lame')
						.audioQuality(5)
						// .inputOption('-probesize 32M') // Potentially help ffmpeg identify input faster
						// .inputOption('-analyzeduration 10M')
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
									stdout,
									stderr,
									segment: segmentNumber
								},
								`FFmpeg error processing segment ${segmentNumber}`
							)
							// Error will be caught by the promise wrapper below
						})
						.on('end', () => {
							logger.info(
								`FFmpeg processing seemingly finished for segment ${segmentNumber}.`
							)
						})

					// --- Wrap ffmpeg processing and upload in a Promise ---
					// This allows catching errors from either ffmpeg or the upload process
					await new Promise<void>((resolve, reject) => {
						const ffmpegOutputStream = ffmpegCommand.pipe() // Get the output stream

						// Handle errors on the output stream (important!)
						ffmpegOutputStream.on('error', err => {
							logger.error(
								{ error: err.message, segment: segmentNumber },
								'Error emitted on ffmpeg output stream.'
							)
							reject(
								new Error(
									`FFmpeg output stream error: ${err.message}`
								)
							)
						})

						uploadStreamToGCS(ffmpegOutputStream, destFileName)
							.then(() => {
								gcsUploadSucceeded = true
								logger.info(
									`Segment ${segmentNumber}/${numSegments} successfully encoded and uploaded to ${gcsUri}`
								)
								resolve() // Success
							})
							.catch(uploadErr => {
								logger.error(
									{
										error: uploadErr.message,
										segment: segmentNumber
									},
									'GCS upload failed.'
								)
								// Check if error originated from ffmpeg piping
								if (uploadErr.message?.includes('ffmpeg')) {
									reject(
										new Error(
											`FFmpeg processing/piping failed: ${uploadErr.message}`
										)
									)
								} else {
									reject(
										new Error(
											`GCS upload failed: ${uploadErr.message}`
										)
									)
								}
							})
					})
					// --- End ffmpeg/upload Promise ---

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
						continue // Retry segment processing
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
						continue // Retry segment processing
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
						continue // Retry segment processing
					}
					logger.info(
						`Gemini editing done for segment ${segmentNumber}`
					)

					// --- Segment Success ---
					editedTexts.push(finalText)
					segmentProcessedSuccessfully = true
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi bo'lak tayyor!`,
						false,
						broadcast
					)
				} catch (segmentErr: any) {
					// Catches errors from yt-dlp helpers, ffmpeg promise wrapper, or transcription services
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

					// Check if error indicates yt-dlp failure (more specific)
					if (
						segmentErr.message?.includes('yt-dlp') ||
						segmentErr.message?.includes('YouTube access error')
					) {
						logger.error(
							'yt-dlp related error occurred. Check detailed logs. Possible cookie/auth/network issue or invalid video.'
						)
						await pushTranscriptionEvent(
							jobId,
							`YouTube yuklashda/kirishda xatolik (yt-dlp ${segmentNumber}/${numSegments}). Cookie/URL'ni tekshiring.`,
							false,
							broadcast
						)
						// Abort job if yt-dlp fails - retrying likely won't help same error
						throw new Error(
							`Aborting job due to yt-dlp failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}
					// Check for ffmpeg specific errors if identifiable
					if (segmentErr.message?.includes('FFmpeg')) {
						logger.error(
							'FFmpeg failed during processing/piping. Check logs.'
						)
						// Decide if retryable or fatal - often retry won't help if input stream was bad
						throw new Error(
							`Aborting job due to FFmpeg failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}

					await delay(1500) // Wait before retry for potentially transient errors
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
						}
					}
					await delay(500)
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
				)
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

		const finalTitle = videoInfo.title || "Noma'lum Sarlavha" // Use fetched title with fallback
		const finalTranscript = `<i style="display: block; font-style: italic; text-align: center;">🕒 Transkripsiya uchun ${formatDuration(duration)} vaqt ketdi.</i><h1 style="font-weight: 700; font-size: 1.8rem; margin: 1rem 0; text-align: center; line-height: 1;">${finalTitle}</h1>\n\n<p style="text-indent: 30px;">${convertToUzbekLatin(combinedResult)}</p>`

		await transcriptService.saveFinalTranscript(jobId, finalTranscript)
		logger.info(`Final transcript saved for job ${jobId}.`)

		await pushTranscriptionEvent(jobId, finalTranscript, true, broadcast)
		jobStatusUpdated = true
	} catch (err: any) {
		// Catch errors from info fetching, segment processing loop (incl. aborts), or finalization
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
				const clientErrorMessage = err.message?.startsWith(
					'Aborting job due to'
				)
					? `Xatolik: YouTube'dan yuklab bo'lmadi. Cookie eskirgan yoki video mavjud emas bo'lishi mumkin.`
					: `Serverda kutilmagan xatolik yuz berdi. (${err.message || 'No details'})`
				await pushTranscriptionEvent(
					jobId,
					clientErrorMessage,
					true,
					broadcast
				)
			} catch (sseErr: any) {
				logger.error(
					{ error: sseErr.message, jobId: jobId },
					'Failed to send final error SSE event'
				)
			}
		}
	}
}
