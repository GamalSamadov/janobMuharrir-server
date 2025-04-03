import ffmpeg from 'fluent-ffmpeg'
import os from 'os'
import path from 'path'
import { performance } from 'perf_hooks'
import { Readable } from 'stream'
import { v4 as uuidv4 } from 'uuid'
import { exec } from 'youtube-dl-exec'

// Import youtube-dl-exec

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

import fs from 'fs/promises'

const delay = (ms: number) => new Promise(res => setTimeout(res, ms))

// --- Helper: Manage Temporary Cookie File ---
async function useCookieFile(
	cookieValue: string | undefined,
	logPrefix: string = 'ytdl'
): Promise<{ cookieFilePath?: string; cleanup: () => Promise<void> }> {
	if (!cookieValue || cookieValue.trim().length === 0) {
		logger.info(`${logPrefix}: No valid cookie value provided.`)
		return { cleanup: async () => {} }
	}

	const tempDir = os.tmpdir()
	const uniqueId = uuidv4()
	const cookieFilePath = path.join(tempDir, `youtube_cookies_${uniqueId}.txt`)

	const cleanup = async () => {
		try {
			await fs.unlink(cookieFilePath)
			logger.info(
				`${logPrefix}: Deleted temp cookie file: ${cookieFilePath}`
			)
		} catch (err: any) {
			if (err.code !== 'ENOENT') {
				logger.warn(
					{ error: err.message, file: cookieFilePath },
					`${logPrefix}: Failed to delete temp cookie file.`
				)
			}
		}
	}

	try {
		const sanitizedCookieValue = cookieValue.trimEnd() + '\n'
		await fs.writeFile(cookieFilePath, sanitizedCookieValue, {
			encoding: 'utf-8',
			mode: 0o600
		}) // Set secure permissions
		logger.info(`${logPrefix}: Created temp cookie file: ${cookieFilePath}`)
		return { cookieFilePath, cleanup }
	} catch (err: any) {
		logger.error(
			{ error: err.message, file: cookieFilePath },
			`${logPrefix}: Failed to create temp cookie file.`
		)
		await cleanup() // Attempt cleanup
		throw new Error(`Failed to write cookie file: ${err.message}`)
	}
}

// --- Helper Functions using youtube-dl-exec ---
interface VideoInfo {
	title: string
	duration: number // in seconds
}

async function getVideoInfoWithYtdl(
	youtubeUrl: string,
	cookie?: string
): Promise<VideoInfo> {
	const logPrefix = 'ytdl-info'
	let cookieHandler: {
		cookieFilePath?: string
		cleanup: () => Promise<void>
	} | null = null
	logger.info(
		`${logPrefix}: Fetching info for ${youtubeUrl}. Cookie provided: ${!!cookie}`
	)

	try {
		cookieHandler = await useCookieFile(cookie, logPrefix)
		const options: any = {
			noWarnings: true,
			noCallHome: true,
			ignoreConfig: true,
			dumpJson: true,
			skipDownload: true,
			forceIpv4: true
		}

		if (cookieHandler.cookieFilePath) {
			try {
				const stats = await fs.stat(cookieHandler.cookieFilePath)
				if (stats.size > 0) {
					options.cookies = cookieHandler.cookieFilePath
					logger.info(
						`${logPrefix}: Using cookie file: ${cookieHandler.cookieFilePath}`
					)
				} else {
					logger.warn(
						`${logPrefix}: Cookie file exists but is EMPTY: ${cookieHandler.cookieFilePath}. Proceeding without --cookies.`
					)
				}
			} catch (statErr: any) {
				logger.error(
					{ error: statErr, file: cookieHandler.cookieFilePath },
					`${logPrefix}: Failed to stat cookie file. Proceeding without --cookies.`
				)
			}
		}

		logger.info(`${logPrefix}: Executing youtube-dl-exec with options...`)
		const result = await exec(youtubeUrl, options)

		// The result should be the JSON string if dumpJson is true
		const info = JSON.parse(result.stdout)

		if (!info.title || typeof info.duration !== 'number') {
			throw new Error('Invalid video info structure received.')
		}

		logger.info(
			`${logPrefix}: Successfully fetched info for title: ${info.title}`
		)
		return { title: info.title, duration: info.duration }
	} catch (error: any) {
		const stderr = error?.stderr || 'No stderr available'
		const exitCode = error?.exitCode || 'N/A'
		logger.error(
			{
				error: error.message,
				stderr: stderr.substring(0, 500), // Log truncated stderr
				exitCode
			},
			`${logPrefix}: Failed to get video info.`
		)

		let specificError = `yt-dlp info process failed (Code: ${exitCode}).`
		if (
			stderr.includes('Private video') ||
			stderr.includes('login required') ||
			stderr.includes('Sign in to confirm you’re not a bot') ||
			stderr.includes('confirm your age') ||
			stderr.includes('unavailable') ||
			stderr.includes('Sign in') ||
			stderr.includes('consent') ||
			stderr.includes('403') ||
			stderr.includes('401') ||
			stderr.includes('Premiere') ||
			stderr.includes('confirm you')
		) {
			specificError = `YouTube access error (yt-dlp info): Video might be private/unavailable/premiere, require login/age/bot confirmation, or cookie invalid/expired/rejected. Code ${exitCode}.`
		} else if (stderr.includes('unable to download video data')) {
			specificError = `yt-dlp info failed: Unable to download video data. Check URL/Network/Cookie. Code ${exitCode}.`
		}

		throw new Error(`${specificError} Stderr: ${stderr.substring(0, 500)}`)
	} finally {
		await cookieHandler
			?.cleanup()
			.catch(e =>
				logger.warn(
					{ error: e.message },
					`${logPrefix}: Error during cookie cleanup.`
				)
			)
	}
}

async function streamAudioWithYtdl(
	youtubeUrl: string,
	startTime: number,
	duration: number,
	cookie?: string
): Promise<Readable> {
	const logPrefix = 'ytdl-stream'
	let cookieHandler: {
		cookieFilePath?: string
		cleanup: () => Promise<void>
	} | null = null
	let ytdlProcess: ReturnType<typeof exec> | null = null

	logger.info(
		`${logPrefix}: Streaming audio for ${youtubeUrl}. Cookie provided: ${!!cookie}`
	)

	try {
		cookieHandler = await useCookieFile(cookie, logPrefix)
		const options: any = {
			noWarnings: true,
			noCallHome: true,
			ignoreConfig: true,
			format: 'bestaudio/best',
			output: '-',
			forceIpv4: true,
			// Pass postprocessor args correctly for youtube-dl-exec
			'postprocessor-args': `"ffmpeg_i:-ss ${startTime} -to ${startTime + duration}"`
		}

		if (cookieHandler.cookieFilePath) {
			try {
				const stats = await fs.stat(cookieHandler.cookieFilePath)
				if (stats.size > 0) {
					options.cookies = cookieHandler.cookieFilePath
					logger.info(
						`${logPrefix}: Using cookie file: ${cookieHandler.cookieFilePath}`
					)
				} else {
					logger.warn(
						`${logPrefix}: Cookie file exists but is EMPTY: ${cookieHandler.cookieFilePath}. Proceeding without --cookies.`
					)
				}
			} catch (statErr: any) {
				logger.error(
					{ error: statErr, file: cookieHandler.cookieFilePath },
					`${logPrefix}: Failed to stat cookie file. Proceeding without --cookies.`
				)
			}
		}

		logger.info(`${logPrefix}: Executing youtube-dl-exec for streaming...`)
		// Execute and get the child process to access stdout stream
		ytdlProcess = exec(youtubeUrl, options, {
			stdio: ['ignore', 'pipe', 'pipe']
		})

		if (!ytdlProcess.stdout) {
			throw new Error('Failed to get stdout stream from youtube-dl-exec.')
		}
		const outputAudioStream = ytdlProcess.stdout
		let stderrData = ''
		const MAX_STDERR_LOG = 2000

		ytdlProcess.stderr?.on('data', data => {
			const chunk = data.toString()
			stderrData += chunk
			// Avoid overly verbose logging of every stderr chunk unless debugging
			// logger.debug(`${logPrefix} stderr chunk: ${chunk.trim()}`);
		})

		ytdlProcess.on('error', err => {
			logger.error({ error: err }, `${logPrefix}: Process spawn error.`)
			if (!outputAudioStream.destroyed) {
				outputAudioStream.emit(
					'error',
					new Error(`yt-dlp process spawn error: ${err.message}`)
				)
				outputAudioStream.destroy()
			}
		})

		ytdlProcess.on('close', async code => {
			const finalStderr = stderrData
			await cookieHandler
				?.cleanup()
				.catch(e =>
					logger.warn(
						{ error: e.message },
						`${logPrefix}: Error during cookie cleanup.`
					)
				)
			cookieHandler = null // Prevent further cleanup attempts

			if (code !== 0) {
				logger.error(
					`${logPrefix}: process exited with code ${code}. Full Stderr: ${finalStderr}`
				)
				let specificError = `yt-dlp stream process exited with error code ${code}.`

				if (
					finalStderr.includes('403 Forbidden') ||
					finalStderr.includes('401 Unauthorized') ||
					finalStderr.includes(
						'Sign in to confirm you’re not a bot'
					) ||
					finalStderr.includes('Sign in') ||
					finalStderr.includes('confirm you') ||
					finalStderr.includes('consent') ||
					finalStderr.includes('login required')
				) {
					specificError = `yt-dlp download failed (Authentication/Authorization Error - 403/401/Login/Bot/Consent?). Check cookie validity/freshness. Code ${code}.`
				} else if (
					finalStderr.includes('Socket error') ||
					finalStderr.includes('timed out')
				) {
					specificError = `yt-dlp download failed (Network/Socket/Timeout error). Check connection. Code ${code}.`
				}

				if (!outputAudioStream.destroyed) {
					outputAudioStream.emit(
						'error',
						new Error(
							`${specificError} Stderr: ${finalStderr.substring(0, MAX_STDERR_LOG)}`
						)
					)
					outputAudioStream.destroy()
				}
			} else {
				logger.info(`${logPrefix}: process finished successfully.`)
			}
		})

		outputAudioStream.on('error', async err => {
			logger.error(
				{ error: err.message },
				`${logPrefix}: Error emitted on output stream.`
			)
			// Ensure process is killed if the stream errors
			if (ytdlProcess && ytdlProcess.kill) {
				logger.warn(
					`${logPrefix}: Killing ytdl process due to stream error.`
				)
				ytdlProcess.kill('SIGKILL')
			}
			await cookieHandler?.cleanup().catch(/* ignore */) // Try cleanup on stream error too
		})

		outputAudioStream.on('end', () => {
			logger.info(`${logPrefix}: Output stream ended.`)
		})

		return outputAudioStream
	} catch (error: any) {
		logger.error(
			{ error: error.message },
			`${logPrefix}: Error setting up stream (e.g., cookie file, process start).`
		)
		await cookieHandler
			?.cleanup()
			.catch(e =>
				logger.warn(
					{ error: e.message },
					`${logPrefix}: Error during cookie cleanup in catch block.`
				)
			)

		// Return a stream that immediately errors
		const errorStream = new Readable({
			read() {
				this.emit(
					'error',
					new Error(
						`Failed to initiate audio stream: ${error.message}`
					)
				)
				this.push(null)
			}
		})
		return errorStream
	}
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
	// Basic logging, avoid logging full final transcript if large
	if (!completed || message.length < 500) {
		logger.info(
			{ jobId, completed, length: message.length },
			'Pushing transcription event'
		)
	} else {
		logger.info(
			{
				jobId,
				completed,
				length: message.length
			},
			'Pushing final transcription event (content truncated in logs)'
		)
	}
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
	const operationId = `job-${jobId}-${Date.now()}`
	const jobLogger = logger.child({ jobId, operationId, url })

	jobLogger.info('Starting transcription job...')

	const youtubeCookie = process.env.YOUTUBE_COOKIE
	if (youtubeCookie && youtubeCookie.trim().length > 0) {
		jobLogger.info(
			`Found YOUTUBE_COOKIE env var (length: ${youtubeCookie.length}).`
		)
	} else {
		jobLogger.warn(
			'YOUTUBE_COOKIE env var not set or empty. Transcription may fail for private/restricted videos.'
		)
	}

	try {
		await transcriptService.running(jobId)
		jobStatusUpdated = true
		await delay(1000) // Small delay for UI updates

		// --- Get Video Info ---
		let videoInfo: VideoInfo
		jobLogger.info(`Fetching video info via youtube-dl-exec...`)

		try {
			videoInfo = await getVideoInfoWithYtdl(url, youtubeCookie)
			jobLogger.info(
				`Successfully fetched video info for title: ${videoInfo.title}`
			)
		} catch (err: any) {
			jobLogger.error(
				{ error: err.message, stack: err.stack },
				'Failed to get video info from youtube-dl-exec.'
			)
			let errorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). URL, server yoki cookie'ni tekshiring. (${err.message || 'Unknown yt-dlp info error'})`
			if (err.message?.includes('YouTube access error')) {
				if (err.message?.includes('bot confirmation')) {
					errorMessage = `Video ma'lumotlarini olib bo'lmadi (yt-dlp). YouTube bot tekshiruvini talab qilmoqda. Cookie faylini yangilang/tekshiring. (${err.message})`
				} else {
					errorMessage = `Video ma'lumotlarini olib bo'lmadi (yt-dlp). YouTube kirish xatosi (maxfiy/mavjud emas/yosh tekshiruvi/cookie yaroqsiz?). (${err.message})`
				}
			} else if (err.message?.includes('write cookie file')) {
				errorMessage = `Server xatosi: Cookie faylini yozib bo'lmadi. (${err.message})`
			} else if (err.message?.includes('Unable to download video data')) {
				errorMessage = `Video ma'lumotlarini olib bo'lmadi (yt-dlp): Video data yuklanmadi. URL/Tarmoq/Cookie'ni tekshiring. (${err.message})`
			}

			await pushTranscriptionEvent(jobId, errorMessage, true, broadcast)
			await transcriptService.error(jobId)
			jobStatusUpdated = true
			return
		}
		// --- End Get Video Info ---

		const title = videoInfo.title
		const totalDuration = videoInfo.duration

		if (isNaN(totalDuration) || totalDuration <= 0) {
			jobLogger.error(`Invalid video duration received: ${totalDuration}`)
			await pushTranscriptionEvent(
				jobId,
				`Xatolik: Video davomiyligi noto'g'ri (${totalDuration}s).`,
				true,
				broadcast
			)
			await transcriptService.error(jobId)
			jobStatusUpdated = true
			return
		}
		jobLogger.info(
			`Video Title: "${title}", Duration: ${formatDuration(totalDuration * 1000)}`
		)

		try {
			await transcriptService.updateTitle(jobId, title)
		} catch (updateErr: any) {
			jobLogger.warn(
				{ title, error: updateErr.message },
				'Failed to update job title in database.'
			)
		}

		await pushTranscriptionEvent(
			jobId,
			'Ovoz yuklanmoqda (youtube-dl-exec)...',
			false,
			broadcast
		)
		await delay(500)
		const segmentDuration = 150 // seconds
		const numSegments = Math.ceil(totalDuration / segmentDuration)
		jobLogger.info(`Total segments calculated: ${numSegments}`)

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
			jobLogger.error(
				'Missing GOOGLE_CLOUD_BUCKET_NAME environment variable.'
			)
			await pushTranscriptionEvent(
				jobId,
				'Server konfiguratsiya xatosi: Bucket topilmadi.',
				true,
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
			// Ensure duration is positive, even if very small
			const safeActualDuration = Math.max(0.1, actualDuration)

			const destFileName = `segment_${jobId}_${segmentNumber}.mp3`
			const gcsUri = `gs://${bucketName}/${destFileName}`

			let segmentProcessedSuccessfully = false
			let attempt = 0
			const maxAttempts = 2 // Retry logic remains

			while (!segmentProcessedSuccessfully && attempt < maxAttempts) {
				attempt++
				const segmentLogger = jobLogger.child({
					segment: segmentNumber,
					attempt,
					startTime: segmentStartTime,
					duration: safeActualDuration
				})
				let gcsUploadSucceeded = false
				let ffmpegCommand: ffmpeg.FfmpegCommand | null = null
				let audioStream: Readable | null = null // Keep track for potential cleanup

				if (attempt > 1) {
					segmentLogger.warn(
						`Retrying segment ${segmentNumber} (attempt ${attempt})...`
					)
					await pushTranscriptionEvent(
						jobId,
						`Bo'lak ${segmentNumber}/${numSegments} qayta urinish (${attempt}/${maxAttempts})...`,
						false,
						broadcast
					)
					await delay(1000 * attempt) // Backoff delay
				}

				try {
					await pushTranscriptionEvent(
						jobId,
						`Bo'lak ${segmentNumber}/${numSegments} yuklanmoqda (youtube-dl-exec)...`,
						false,
						broadcast
					)

					segmentLogger.info(
						`Attempting segment download via youtube-dl-exec...`
					)
					audioStream = await streamAudioWithYtdl(
						url,
						segmentStartTime,
						safeActualDuration,
						youtubeCookie
					)

					segmentLogger.info(`Starting FFmpeg encoding...`)
					ffmpegCommand = ffmpeg(audioStream)
						.format('mp3')
						.audioCodec('libmp3lame')
						.audioBitrate('96k')
						.on('start', cmd =>
							segmentLogger.info(`FFmpeg started: ${cmd}`)
						)
						.on('error', (err, stdout, stderr) => {
							// Log ffmpeg specific errors, but main error handled in promise reject
							segmentLogger.error(
								{ message: err.message, stdout, stderr },
								`FFmpeg error event processing segment`
							)
						})
						.on('end', () => {
							segmentLogger.info(
								`FFmpeg processing finished (stream ended).`
							)
						})

					// --- Wrap ffmpeg processing and upload in a Promise ---
					await new Promise<void>((resolve, reject) => {
						if (!ffmpegCommand || !audioStream) {
							return reject(
								new Error(
									'FFmpeg command or audio stream was not initialized.'
								)
							)
						}
						const ffmpegOutputStream = ffmpegCommand.pipe()
						let promiseRejected = false // Avoid multiple rejections

						const killFFmpegAndReject = (err: Error) => {
							if (promiseRejected) return
							promiseRejected = true
							try {
								if (ffmpegCommand) {
									segmentLogger.warn(
										`Killing ffmpeg due to error: ${err.message}`
									)
									ffmpegCommand.kill('SIGKILL')
								}
							} catch (killErr: any) {
								segmentLogger.warn(
									{ error: killErr.message },
									'Error trying to kill ffmpeg after error'
								)
							}
							reject(err)
						}

						// Handle errors from the *input* stream (ytdl)
						audioStream.on('error', inputError => {
							segmentLogger.error(
								{ error: inputError.message },
								'Error on youtube-dl-exec input stream for ffmpeg'
							)
							// The specific yt-dlp errors (auth, network) are now surfaced here
							killFFmpegAndReject(
								new Error(
									`Input stream error: ${inputError.message}`
								)
							)
						})

						// Handle errors from the ffmpeg process itself
						ffmpegCommand.on('error', err => {
							killFFmpegAndReject(
								new Error(
									`FFmpeg command failed directly: ${err.message}`
								)
							)
						})

						// Handle errors from the *output* stream (piping to GCS)
						ffmpegOutputStream.on('error', outputError => {
							segmentLogger.error(
								{ error: outputError.message },
								'Error on ffmpeg output stream during upload pipe.'
							)
							killFFmpegAndReject(
								new Error(
									`FFmpeg output stream error: ${outputError.message}`
								)
							)
						})

						// Handle GCS upload success/failure
						uploadStreamToGCS(ffmpegOutputStream, destFileName)
							.then(() => {
								if (!promiseRejected) {
									gcsUploadSucceeded = true
									segmentLogger.info(
										`Segment uploaded to ${gcsUri}`
									)
									resolve()
								} else {
									segmentLogger.warn(
										'GCS upload finished, but an error occurred earlier.'
									)
									gcsUploadSucceeded = false
									// Don't resolve or reject here, let the original error handler do it
								}
							})
							.catch(uploadErr => {
								segmentLogger.error(
									{ error: uploadErr.message },
									'GCS upload failed.'
								)
								killFFmpegAndReject(
									new Error(
										`GCS upload failed: ${uploadErr.message}`
									)
								)
							})
					})
					// --- End ffmpeg/upload Promise ---

					segmentLogger.info(
						'FFmpeg/Upload promise resolved successfully.'
					)

					// --- Transcriptions & Editing (No changes needed here) ---
					segmentLogger.info('Starting Google transcription...')
					await pushTranscriptionEvent(
						jobId,
						`Google matnni o'girmoqda ${segmentNumber}/${numSegments}...`,
						false,
						broadcast
					)
					const transcriptGoogle = await transcribeWithGoogle(gcsUri)
					if (transcriptGoogle) {
						segmentLogger.info(
							`Google transcription done (length: ${transcriptGoogle.length}).`
						)
					} else {
						segmentLogger.warn(
							`Google transcription returned empty/null for ${gcsUri}.`
						)
						await pushTranscriptionEvent(
							jobId,
							`${segmentNumber}/${numSegments}-chi Google matnida xatolik (bo'sh natija). Davom etilmoqda...`,
							false,
							broadcast
						)
					}

					segmentLogger.info('Starting ElevenLabs transcription...')
					await pushTranscriptionEvent(
						jobId,
						`ElevenLabs matnni o'girmoqda ${segmentNumber}/${numSegments}...`,
						false,
						broadcast
					)
					let transcriptElevenLabs: string | null = null
					try {
						const stream11 = await getGCSFileStream(gcsUri)
						transcriptElevenLabs =
							await transcribeAudioElevenLabs(stream11)
						if (transcriptElevenLabs) {
							segmentLogger.info(
								`ElevenLabs transcription done (length: ${transcriptElevenLabs.length}).`
							)
						} else {
							segmentLogger.warn(
								`ElevenLabs transcription returned empty/null for ${gcsUri}`
							)
							await pushTranscriptionEvent(
								jobId,
								`${segmentNumber}/${numSegments}-chi ElevenLabs matnida xatolik (bo'sh natija). Google natijasi bilan davom etilmoqda...`,
								false,
								broadcast
							)
						}
					} catch (elevenLabsError: any) {
						segmentLogger.error(
							{ error: elevenLabsError.message },
							`ElevenLabs transcription failed for ${gcsUri}`
						)
						if (!transcriptGoogle) {
							// If Google also failed, this is critical
							throw new Error(
								`ElevenLabs failed and Google text is also empty: ${elevenLabsError.message}`
							)
						} else {
							await pushTranscriptionEvent(
								jobId,
								`${segmentNumber}/${numSegments}-chi ElevenLabs matnida xatolik (${elevenLabsError.message}). Google natijasi bilan davom etilmoqda...`,
								false,
								broadcast
							)
						}
					}

					const googleInput = transcriptGoogle || ''
					const elevenLabsInput =
						transcriptElevenLabs || (transcriptGoogle ? '' : null) // Provide empty string if Google has text, otherwise null

					if (googleInput === '' && elevenLabsInput === null) {
						segmentLogger.error(
							`Both Google and ElevenLabs transcription failed or returned empty for ${gcsUri}`
						)
						throw new Error(
							'Both Google and ElevenLabs transcription failed or returned empty.'
						)
					}

					segmentLogger.info('Starting Gemini editing...')
					await pushTranscriptionEvent(
						jobId,
						`Matnni Gemini tahrirlamoqda ${segmentNumber}/${numSegments}...`,
						false,
						broadcast
					)
					const finalText = await editTranscribed(
						googleInput,
						elevenLabsInput ?? ''
					) // Pass empty string if null
					if (!finalText) {
						segmentLogger.error(
							`Gemini editing returned empty/null for ${gcsUri}`
						)
						throw new Error('Gemini editing returned empty.')
					} else {
						segmentLogger.info(
							`Gemini editing done (length: ${finalText.length}).`
						)
					}

					editedTexts.push(finalText)
					segmentProcessedSuccessfully = true
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi bo'lak tayyor!`,
						false,
						broadcast
					)
					segmentLogger.info(
						`Segment ${segmentNumber} processed successfully.`
					)
				} catch (segmentErr: any) {
					segmentLogger.error(
						{ error: segmentErr.message, stack: segmentErr.stack },
						`Error processing segment ${segmentNumber} on attempt ${attempt}`
					)
					await pushTranscriptionEvent(
						jobId,
						`Xatolik (${segmentNumber}/${numSegments}, urinish ${attempt}): ${segmentErr.message.substring(0, 150)}...`,
						false,
						broadcast
					)

					// --- Check for Fatal Errors to abort the job ---
					// Capture yt-dlp errors surfaced via the input stream error
					if (
						segmentErr.message?.includes('Input stream error:') &&
						(segmentErr.message?.includes(
							'Authentication/Authorization Error'
						) ||
							segmentErr.message?.includes(
								'Network/Socket/Timeout error'
							) ||
							segmentErr.message?.includes(
								'yt-dlp stream process exited'
							)) // Catch generic exit code errors too
					) {
						segmentLogger.error(
							'Fatal youtube-dl-exec stream related error occurred. Aborting job.'
						)
						let userMsg = `YouTube yuklashda/kirishda xatolik (yt-dlp ${segmentNumber}/${numSegments}). Cookie/URL/Video holatini tekshiring. Jarayon to'xtatildi. (${segmentErr.message})`
						if (
							segmentErr.message?.includes(
								'Authentication/Authorization Error'
							)
						) {
							userMsg = `YouTube kirish xatosi (${segmentNumber}/${numSegments}): Cookie yaroqsiz/eskirgan yoki video maxfiy/yosh/bot tekshiruvi? (${segmentErr.message})`
						} else if (
							segmentErr.message?.includes(
								'Network/Socket/Timeout error'
							)
						) {
							userMsg = `Tarmoq xatosi (${segmentNumber}/${numSegments}): YouTube'ga ulanib bo'lmadi (yt-dlp timeout/socket error). (${segmentErr.message})`
						}
						await pushTranscriptionEvent(
							jobId,
							userMsg,
							true,
							broadcast
						)
						throw new Error( // Re-throw to exit the main try block
							`Aborting job due to fatal stream/auth/network failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					} else if (
						segmentErr.message?.includes('FFmpeg command failed') ||
						segmentErr.message?.includes(
							'FFmpeg output stream error'
						)
					) {
						segmentLogger.error(
							'Fatal FFmpeg error occurred. Aborting job.'
						)
						await pushTranscriptionEvent(
							jobId,
							`Audio kodlashda xatolik (FFmpeg ${segmentNumber}/${numSegments}). Jarayon to'xtatildi. (${segmentErr.message})`,
							true,
							broadcast
						)
						throw new Error(
							`Aborting job due to fatal FFmpeg failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					} else if (
						segmentErr.message?.includes('GCS upload failed')
					) {
						segmentLogger.error(
							'Fatal GCS upload error occurred. Aborting job.'
						)
						await pushTranscriptionEvent(
							jobId,
							`Audio bo'lakni saqlashda xatolik (GCS ${segmentNumber}/${numSegments}). Jarayon to'xtatildi. (${segmentErr.message})`,
							true,
							broadcast
						)
						throw new Error(
							`Aborting job due to fatal GCS upload failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					} else if (
						segmentErr.message?.includes(
							'Both Google and ElevenLabs transcription failed'
						) ||
						segmentErr.message?.includes(
							'Gemini editing returned empty'
						)
					) {
						segmentLogger.error(
							`Fatal Transcription/Editing error on segment ${segmentNumber}. Aborting job.`
						)
						await pushTranscriptionEvent(
							jobId,
							`Matnni o'girishda/tahrirlashda tuzatib bo'lmas xatolik (${segmentNumber}/${numSegments}). Jarayon to'xtatildi. (${segmentErr.message})`,
							true,
							broadcast
						)
						throw new Error(
							`Aborting job due to fatal transcription/editing failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					} else if (segmentErr.message?.includes('cookie file')) {
						// This is less likely here, more likely in the initial call, but handle defensively
						segmentLogger.error(
							'Fatal cookie file error during segment processing. Aborting job.'
						)
						await pushTranscriptionEvent(
							jobId,
							`Server xatosi: Cookie fayl bilan ishlashda muammo (${segmentNumber}/${numSegments}). Jarayon to'xtatildi. (${segmentErr.message})`,
							true,
							broadcast
						)
						throw new Error(
							`Aborting job due to cookie file failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}
					// Non-fatal errors will allow retry loop to continue

					await delay(2000 + attempt * 1000) // Backoff before retry
				} finally {
					// Ensure resources are cleaned up after each attempt if needed
					// Kill ffmpeg if it's still running and the segment failed
					if (ffmpegCommand && !segmentProcessedSuccessfully) {
						try {
							segmentLogger.warn(
								'Ensuring FFmpeg process is killed in finally block (segment attempt failed).'
							)
							ffmpegCommand.kill('SIGKILL')
						} catch (killErr: any) {
							segmentLogger.warn(
								{ error: killErr.message },
								'Error killing ffmpeg in finally block'
							)
						}
					}
					// Destroy input stream if it exists and segment failed, to prevent leaks
					if (
						audioStream &&
						!segmentProcessedSuccessfully &&
						!audioStream.destroyed
					) {
						segmentLogger.warn(
							'Destroying ytdl audio stream in finally block.'
						)
						audioStream.destroy()
					}

					// Delete GCS file only if upload succeeded
					if (gcsUploadSucceeded) {
						try {
							segmentLogger.info(
								`Attempting to delete GCS file: ${destFileName}`
							)
							await deleteGCSFile(destFileName)
							segmentLogger.info(
								`Successfully deleted GCS file: ${destFileName}`
							)
						} catch (deleteErr: any) {
							segmentLogger.warn(
								// Warn instead of error, as job can continue
								{
									error: deleteErr.message,
									file: destFileName
								},
								`Failed to delete GCS segment file: ${destFileName}. Continuing job.`
							)
						}
					} else if (destFileName) {
						segmentLogger.info(
							`Skipping GCS delete for ${destFileName} as upload didn't succeed or error occurred.`
						)
					}
					await delay(300) // Small pause
				}
			} // End retry loop

			if (!segmentProcessedSuccessfully) {
				jobLogger.error(
					`Segment ${segmentNumber}/${numSegments} failed after ${maxAttempts} attempts. Aborting job.`
				)
				await pushTranscriptionEvent(
					jobId,
					`Xatolik: ${segmentNumber}/${numSegments}-chi bo'lakni ${maxAttempts} urinishda ham ishlab bo'lmadi. Jarayon to'xtatildi.`,
					true,
					broadcast
				)
				throw new Error( // Throw to exit main try block
					`Failed to process segment ${segmentNumber} after ${maxAttempts} attempts.`
				)
			}

			i++
		} // End segment loop

		// --- Combine and Finalize ---
		jobLogger.info(
			`All ${numSegments} segments processed successfully. Combining...`
		)
		try {
			await userSession.completed(sessionId)
			jobLogger.info(`Marked session ${sessionId} as completed.`)
		} catch (err: any) {
			jobLogger.warn(
				{ error: err.message, sessionId: sessionId },
				`Could not mark session as completed`
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
			.replace(/(\n\s*){3,}/g, '\n\n') // Clean up extra newlines
			.trim()

		const duration = performance.now() - startTime
		jobLogger.info(`Job completed in ${formatDuration(duration)}`)
		await pushTranscriptionEvent(
			jobId,
			`Yakuniy matn jamlandi!`,
			false,
			broadcast
		)
		await delay(500)

		const finalTitle = videoInfo.title || "Noma'lum Sarlavha"
		const finalTranscript = `<i style="display: block; font-style: italic; text-align: center;">🕒 Transkripsiya uchun ${formatDuration(duration)} vaqt ketdi.</i><h1 style="font-weight: 700; font-size: 1.8rem; margin: 1rem 0; text-align: center; line-height: 1;">${finalTitle}</h1>\n\n<p style="text-indent: 30px;">${convertToUzbekLatin(combinedResult)}</p>`

		await transcriptService.saveFinalTranscript(jobId, finalTranscript)
		jobLogger.info(`Final transcript saved.`)
		await pushTranscriptionEvent(jobId, finalTranscript, true, broadcast)
		jobStatusUpdated = true // Mark success
	} catch (err: any) {
		// --- Final Error Handling ---
		jobLogger.error(
			{ error: err.message, stack: err.stack },
			'Critical error caught in runTranscriptionJob main try-catch block'
		)

		// Ensure job status is marked as error if it wasn't already completed/errored
		if (!jobStatusUpdated) {
			try {
				await transcriptService.error(jobId)
				jobStatusUpdated = true
			} catch (dbErr: any) {
				jobLogger.error(
					{ error: dbErr.message },
					'Failed to mark job as error in DB during final catch block'
				)
			}
		}

		// Attempt to send a final error message to the client
		if (broadcast) {
			try {
				let clientErrorMessage = `Serverda kutilmagan xatolik yuz berdi. (${err.message?.substring(0, 100) || 'No details'}...)`

				// Reuse specific error messages generated during fatal error checks
				if (
					err.message?.includes(
						'Aborting job due to fatal stream/auth/network failure'
					)
				) {
					if (err.message?.includes('bot confirmation')) {
						clientErrorMessage = `Xatolik: YouTube bot tekshiruvini talab qilmoqda. Cookie faylini yangilang/tekshiring. Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
					} else if (
						err.message?.includes(
							'Authentication/Authorization Error'
						)
					) {
						clientErrorMessage = `Xatolik: YouTube kirish xatosi (cookie yaroqsiz/video maxfiy?). Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
					} else if (
						err.message?.includes('Network/Socket/Timeout error')
					) {
						clientErrorMessage = `Xatolik: Tarmoq xatosi (YouTube'ga ulanib bo'lmadi?). Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
					} else {
						clientErrorMessage = `Xatolik: YouTube'dan yuklab bo'lmadi yoki kirishda/tarmoqda muammo. Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
					}
				} else if (
					err.message?.includes(
						'Aborting job due to fatal FFmpeg failure'
					)
				) {
					clientErrorMessage = `Xatolik: Audio faylni kodlashda muammo (FFmpeg). Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
				} else if (
					err.message?.includes(
						'Aborting job due to fatal GCS upload failure'
					)
				) {
					clientErrorMessage = `Xatolik: Audio bo'lakni bulutga saqlashda muammo (GCS). Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
				} else if (
					err.message?.includes(
						'Aborting job due to fatal transcription/editing failure'
					)
				) {
					clientErrorMessage = `Xatolik: Matnni o'girishda/tahrirda tuzatib bo'lmas xatolik. Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
				} else if (err.message?.includes('Failed to process segment')) {
					clientErrorMessage = `Xatolik: ${err.message}` // Pass segment failure message directly
				} else if (
					err.message?.includes('yt-dlp info process failed') ||
					err.message?.includes("Video ma'lumotlarini olib bo'lmadi") // Catch initial info fetch errors
				) {
					if (err.message?.includes('bot confirmation')) {
						clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). YouTube bot tekshiruvini talab qilmoqda. Cookie faylini yangilang/tekshiring. (${err.message?.substring(0, 100)}...)`
					} else {
						clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). URL/Cookie/Video holatini/Tarmoqni tekshiring. (${err.message?.substring(0, 100)}...)`
					}
				} else if (
					err.message?.includes('GOOGLE_CLOUD_BUCKET_NAME') ||
					err.message?.includes('Bucket topilmadi')
				) {
					clientErrorMessage = `Server konfiguratsiya xatosi: Bucket topilmadi.`
				} else if (err.message?.includes('cookie file')) {
					clientErrorMessage = `Server xatosi: Cookie fayl bilan ishlashda muammo. (${err.message?.substring(0, 100)}...)`
				}

				await pushTranscriptionEvent(
					jobId,
					clientErrorMessage,
					true,
					broadcast
				)
			} catch (sseErr: any) {
				jobLogger.error(
					{ error: sseErr.message },
					'Failed to send final error event'
				)
			}
		}
	} finally {
		const finalDuration = performance.now() - startTime
		jobLogger.info(
			`Transcription job function finished execution after ${formatDuration(finalDuration)}.`
		)
		// Note: Cookie file cleanup is handled within the helper functions' finally blocks
	}
}
