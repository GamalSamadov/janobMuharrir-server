import ffmpeg from 'fluent-ffmpeg'
import { HttpsProxyAgent } from 'hpagent'
// Use promises for async operations
import path from 'path'
import { performance } from 'perf_hooks'
import { Readable } from 'stream'
import { CookieJar } from 'tough-cookie'
// --- NEW IMPORTS ---
import ytdl from 'ytdl-core'

// --- END NEW IMPORTS ---

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

// Keep the delay helper
const delay = (ms: number) => new Promise(res => setTimeout(res, ms))

const persistentCookieFilePath = path.join(__dirname, 'cookies.txt')

interface VideoInfo {
	title: string
	duration: number // Duration in seconds
}

// --- Helper to create agent with cookies ---
async function createAgentWithCookies(
	logPrefix: string
): Promise<HttpsProxyAgent | undefined> {
	let agent: HttpsProxyAgent | undefined = undefined
	try {
		// Check if the persistent cookie file exists and is readable and not empty
		const cookieFileContent = await fs.readFile(
			persistentCookieFilePath,
			'utf-8'
		)
		if (cookieFileContent.trim().length > 0) {
			const jar = new CookieJar()
			// Parse Netscape cookie file format asynchronously
			// tough-cookie doesn't have a built-in async file parser, so we read it first
			const lines = cookieFileContent.split('\n')
			for (const line of lines) {
				// Basic parsing - assumes standard Netscape format lines
				// More robust parsing might be needed for complex cookies
				try {
					// Let tough-cookie handle parsing each line
					// Need to provide a dummy URL, the domain from the cookie file takes precedence
					await jar.setCookie(line.trim(), 'https://www.youtube.com')
				} catch (cookieParseError: any) {
					// Log individual cookie parsing errors but continue
					logger.warn(
						{ error: cookieParseError.message, line: line.trim() },
						`${logPrefix}: Failed to parse cookie line, skipping.`
					)
				}
			}

			agent = new HttpsProxyAgent({
				keepAlive: true,
				keepAliveMsecs: 1000,
				maxSockets: 256,
				maxFreeSockets: 256,
				scheduling: 'lifo',
				proxy: undefined, // No proxy needed here
				// @ts-ignore // tough-cookie's CookieJar type might not perfectly match agent's expectation initially
				cookieJar: jar // Pass the populated cookie jar
			})
			logger.info(
				`${logPrefix}: Using persistent cookie file: ${persistentCookieFilePath}`
			)
		} else {
			logger.warn(
				`${logPrefix}: Persistent cookie file exists but is EMPTY: ${persistentCookieFilePath}. Proceeding without cookies.`
			)
		}
	} catch (statErr: any) {
		if (statErr.code === 'ENOENT') {
			logger.info(
				`${logPrefix}: Persistent cookie file not found at ${persistentCookieFilePath}. Proceeding without cookies.`
			)
		} else {
			logger.error(
				{ error: statErr, file: persistentCookieFilePath },
				`${logPrefix}: Failed to read/process persistent cookie file. Proceeding without cookies.`
			)
		}
	}
	return agent
}
// --- End Helper ---

async function getVideoInfoWithYtdlCore(
	youtubeUrl: string
): Promise<VideoInfo> {
	const logPrefix = 'ytdl-core-info'
	logger.info(`${logPrefix}: Fetching info for ${youtubeUrl}.`)

	const agent = await createAgentWithCookies(logPrefix)
	const options: ytdl.getInfoOptions = {}
	if (agent) {
		options.requestOptions = { agent }
	}

	try {
		logger.info(`${logPrefix}: Calling ytdl.getInfo()...`)
		const info = await ytdl.getInfo(youtubeUrl, options)

		const title = info.videoDetails?.title
		const durationStr = info.videoDetails?.lengthSeconds
		const duration = durationStr ? parseInt(durationStr, 10) : 0 // duration is string in seconds

		if (!title || isNaN(duration) || duration <= 0) {
			logger.error(
				{ videoDetails: info.videoDetails },
				`${logPrefix}: Invalid video info structure received.`
			)
			throw new Error(
				'Invalid video info structure received from ytdl-core.'
			)
		}

		logger.info(
			`${logPrefix}: Successfully fetched info for title: ${title}`
		)
		return { title, duration }
	} catch (error: any) {
		const errorMessage = error?.message || 'Unknown ytdl-core info error'
		logger.error(
			{ error: errorMessage, stack: error?.stack?.substring(0, 500) }, // Log truncated stack
			`${logPrefix}: Failed to get video info.`
		)

		// Adapt error messages based on common ytdl-core errors
		let specificError = `yt-dlp info process failed.` // Keep similar structure
		if (
			errorMessage.includes('private video') ||
			errorMessage.includes('Login required') ||
			errorMessage.includes('confirm your age') ||
			errorMessage.includes('unavailable') ||
			errorMessage.includes('Status code: 403') ||
			errorMessage.includes('Status code: 401') ||
			errorMessage.includes('Status code: 410') ||
			errorMessage.includes('age-restricted')
		) {
			specificError = `YouTube access error (ytdl-core info): Video might be private/unavailable/premiere, require login/age confirmation, or cookie file (${persistentCookieFilePath}) is invalid/expired/rejected. (${errorMessage})`
		} else if (
			errorMessage.includes('No video id found') ||
			errorMessage.includes('Not a YouTube domain')
		) {
			specificError = `yt-dlp info failed: Invalid YouTube URL? (${errorMessage})`
		} else if (
			error?.code === 'ENOTFOUND' ||
			error?.code === 'ECONNRESET' ||
			error?.code === 'ETIMEDOUT'
		) {
			specificError = `yt-dlp info failed: Network error (${error.code}). Check connection. (${errorMessage})`
		} else {
			specificError = `yt-dlp info failed: (${errorMessage})` // Generic fallback
		}

		throw new Error(specificError) // Re-throw the processed error
	}
}

async function streamAudioWithYtdlCore(
	youtubeUrl: string
	// startTime and duration removed - slicing happens in ffmpeg
): Promise<Readable> {
	const logPrefix = 'ytdl-core-stream'
	logger.info(`${logPrefix}: Initiating audio stream for ${youtubeUrl}.`)

	const agent = await createAgentWithCookies(logPrefix)
	const options: ytdl.downloadOptions = {
		filter: 'audioonly',
		quality: 'highestaudio' // Or 'lowestaudio' if bandwidth is a concern
		// highWaterMark: 1024 * 1024 * 10, // Optional: Adjust buffer size (e.g., 10MB)
	}
	if (agent) {
		options.requestOptions = { agent }
	}

	try {
		logger.info(`${logPrefix}: Calling ytdl()...`)
		const stream = ytdl(youtubeUrl, options)

		stream.on('error', (err: any) => {
			// IMPORTANT: Errors *during* streaming are caught here
			// These need to be handled by the consumer (runTranscriptionJob)
			logger.error(
				{ error: err.message, code: err.code },
				`${logPrefix}: Error event emitted on ytdl stream during download.`
			)
			// The stream itself will emit 'error' which the ffmpeg pipe should catch
		})

		stream.on('progress', (chunkLength, downloaded, total) => {
			// Optional: Add progress logging if needed, can be verbose
			// logger.debug(`${logPrefix}: Progress - ${downloaded}/${total}`);
		})

		stream.on('end', () => {
			logger.info(`${logPrefix}: ytdl stream ended.`)
		})

		logger.info(`${logPrefix}: ytdl stream initiated successfully.`)
		return stream
	} catch (error: any) {
		// Errors during *initialization* of the stream
		const errorMessage =
			error?.message || 'Unknown ytdl-core stream init error'
		logger.error(
			{ error: errorMessage, stack: error?.stack?.substring(0, 500) },
			`${logPrefix}: Failed to initiate ytdl stream.`
		)

		// Adapt error messages similarly to getInfo
		let specificError = `yt-dlp stream init failed.`
		if (
			errorMessage.includes('private video') ||
			errorMessage.includes('Login required') ||
			errorMessage.includes('confirm your age') ||
			errorMessage.includes('unavailable') ||
			errorMessage.includes('Status code: 403') ||
			errorMessage.includes('Status code: 401') ||
			errorMessage.includes('Status code: 410') ||
			errorMessage.includes('age-restricted')
		) {
			specificError = `YouTube access error (ytdl-core stream init): Video might be private/unavailable/premiere, require login/age confirmation, or cookie file (${persistentCookieFilePath}) is invalid/expired/rejected. (${errorMessage})`
		} else if (
			errorMessage.includes('No video id found') ||
			errorMessage.includes('Not a YouTube domain')
		) {
			specificError = `yt-dlp stream init failed: Invalid YouTube URL? (${errorMessage})`
		} else if (
			error?.code === 'ENOTFOUND' ||
			error?.code === 'ECONNRESET' ||
			error?.code === 'ETIMEDOUT'
		) {
			specificError = `yt-dlp stream init failed: Network error (${error.code}). Check connection. (${errorMessage})`
		} else {
			specificError = `yt-dlp stream init failed: (${errorMessage})`
		}

		// Return a stream that immediately errors
		const errorStream = new Readable({
			read() {
				this.emit('error', new Error(specificError))
				this.push(null) // End the stream
			}
		})
		return errorStream
	}
}

// --- Main Transcription Job Logic (with ytdl-core changes) ---

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

	jobLogger.info('Starting transcription job (using ytdl-core)...')

	// Check cookie file existence (logging only)
	try {
		await fs.access(persistentCookieFilePath, fs.constants.R_OK)
		const stats = await fs.stat(persistentCookieFilePath)
		if (stats.size > 0) {
			jobLogger.info(
				`Persistent cookie file found and readable at ${persistentCookieFilePath}. It will be used if needed.`
			)
		} else {
			jobLogger.warn(
				`Persistent cookie file found at ${persistentCookieFilePath} but is empty. It will not be used.`
			)
		}
	} catch (err: any) {
		if (err.code === 'ENOENT') {
			jobLogger.warn(
				`Persistent cookie file not found at ${persistentCookieFilePath}. Transcription may fail for private/restricted videos.`
			)
		} else {
			jobLogger.error(
				{ error: err.message, file: persistentCookieFilePath },
				`Error accessing persistent cookie file. Transcription may fail for private/restricted videos.`
			)
		}
	}

	try {
		await transcriptService.running(jobId)
		jobStatusUpdated = true
		await delay(1000) // Small delay for UI updates

		// --- Get Video Info ---
		let videoInfo: VideoInfo
		jobLogger.info(`Fetching video info via ytdl-core...`)

		try {
			// Use the new ytdl-core function
			videoInfo = await getVideoInfoWithYtdlCore(url)
			jobLogger.info(
				`Successfully fetched video info for title: ${videoInfo.title}`
			)
		} catch (err: any) {
			jobLogger.error(
				{ error: err.message, stack: err.stack },
				'Failed to get video info from ytdl-core.'
			)
			// Keep user-friendly error messages, reference cookie file path
			// Use the specific error message thrown by getVideoInfoWithYtdlCore
			let errorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (ytdl-core). URL, server yoki cookie faylini (${persistentCookieFilePath}) tekshiring. (${err.message || 'Unknown ytdl-core info error'})`
			if (err.message?.includes('YouTube access error')) {
				if (err.message?.includes('age confirmation')) {
					errorMessage = `Video ma'lumotlarini olib bo'lmadi (ytdl-core). YouTube yosh tekshiruvini talab qilmoqda. Cookie faylini (${persistentCookieFilePath}) yangilang/tekshiring. (${err.message})`
				} else {
					errorMessage = `Video ma'lumotlarini olib bo'lmadi (ytdl-core). YouTube kirish xatosi (maxfiy/mavjud emas/cookie yaroqsiz?). Cookie faylini (${persistentCookieFilePath}) tekshiring. (${err.message})`
				}
			} else if (err.message?.includes('Invalid YouTube URL?')) {
				errorMessage = `Video ma'lumotlarini olib bo'lmadi (ytdl-core): Noto'g'ri YouTube URL? (${err.message})`
			} else if (err.message?.includes('Network error')) {
				errorMessage = `Video ma'lumotlarini olib bo'lmadi (ytdl-core): Tarmoq xatosi. Internet ulanishini tekshiring. (${err.message})`
			}

			await pushTranscriptionEvent(jobId, errorMessage, true, broadcast)
			await transcriptService.error(jobId)
			jobStatusUpdated = true
			return
		}
		// --- End Get Video Info ---

		const title = videoInfo.title
		const totalDuration = videoInfo.duration // Already in seconds

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
			'Ovoz yuklanmoqda (ytdl-core)...', // Updated message
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
			const safeActualDuration = Math.max(0.1, actualDuration) // Ensure positive duration
			const segmentEndTime = segmentStartTime + safeActualDuration // Needed for ffmpeg -to

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
						`Bo'lak ${segmentNumber}/${numSegments} yuklanmoqda (ytdl-core)...`, // Updated message
						false,
						broadcast
					)

					segmentLogger.info(
						`Attempting segment stream via ytdl-core...`
					)
					// Get the *full* audio stream - slicing happens in ffmpeg now
					audioStream = await streamAudioWithYtdlCore(url)

					segmentLogger.info(
						`Starting FFmpeg encoding and slicing...`
					)
					ffmpegCommand = ffmpeg(audioStream)
						// --- ADD FFMPEG TIME SLICING ---
						.inputOption(`-ss ${segmentStartTime}`) // Start time
						.inputOption(`-to ${segmentEndTime}`) // End time
						// --- END FFMPEG TIME SLICING ---
						.format('mp3')
						.audioCodec('libmp3lame')
						.audioBitrate('96k')
						.on('start', cmd =>
							segmentLogger.info(`FFmpeg started: ${cmd}`)
						)
						.on('error', (err, stdout, stderr) => {
							// NOTE: FFmpeg might error if the input stream (ytdl) errors *during* processing
							segmentLogger.error(
								{ message: err.message, stdout, stderr },
								`FFmpeg error event processing segment`
							)
							// This error will likely be caught by the promise reject below
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

						const cleanupAndReject = (err: Error) => {
							if (promiseRejected) return
							promiseRejected = true
							segmentLogger.error(
								{ error: err.message },
								'Error during FFmpeg/Upload, attempting cleanup.'
							)
							try {
								if (audioStream && !audioStream.destroyed) {
									segmentLogger.warn(
										'Destroying ytdl stream due to error...'
									)
									audioStream.destroy()
								}
								if (ffmpegCommand) {
									segmentLogger.warn(
										`Killing ffmpeg due to error: ${err.message}`
									)
									ffmpegCommand.kill('SIGKILL')
								}
							} catch (killErr: any) {
								segmentLogger.warn(
									{ error: killErr.message },
									'Error trying cleanup after error'
								)
							}
							reject(err)
						}

						// Handle errors from the *input* stream (ytdl-core)
						// This is CRITICAL for catching ytdl-core download errors
						audioStream.on('error', inputError => {
							segmentLogger.error(
								{
									error: inputError.message,
									code: (inputError as any).code
								},
								'Error on ytdl-core input stream for ffmpeg'
							)
							// Add specific error checks here based on ytdl-core errors
							let specificMsg = `Input stream error: ${inputError.message}`
							if (
								inputError.message.includes(
									'Status code: 403'
								) ||
								inputError.message.includes(
									'Status code: 401'
								) ||
								inputError.message.includes('Login required') ||
								inputError.message.includes('private video') ||
								inputError.message.includes('age-restricted')
							) {
								specificMsg = `Input stream error: YouTube access error (40x/Private/Age/Login?). Check cookie file (${persistentCookieFilePath}). Msg: ${inputError.message}`
							} else if (
								(inputError as any).code === 'ECONNRESET' ||
								(inputError as any).code === 'ETIMEDOUT' ||
								inputError.message.includes('socket hang up')
							) {
								specificMsg = `Input stream error: Network error (${(inputError as any).code}). Connection lost during download? Msg: ${inputError.message}`
							}
							cleanupAndReject(new Error(specificMsg))
						})

						// Handle errors from the ffmpeg process itself
						ffmpegCommand.on('error', err => {
							// This might be triggered by the input stream error above, or ffmpeg internal issues
							cleanupAndReject(
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
							cleanupAndReject(
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
									gcsUploadSucceeded = false // Ensure flag is correct
									// Don't resolve, let the original error handler manage rejection
								}
							})
							.catch(uploadErr => {
								segmentLogger.error(
									{ error: uploadErr.message },
									'GCS upload failed.'
								)
								cleanupAndReject(
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
							// Only throw if Google also failed
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
						transcriptElevenLabs || (transcriptGoogle ? '' : null) // Default to empty if Google exists, else null

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
						elevenLabsInput ?? '' // Pass empty string if null
					)
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
					// Updated error messages to reflect ytdl-core/ffmpeg/GCS causes
					if (segmentErr.message?.includes('Input stream error:')) {
						// Errors from ytdl-core stream
						let userMsg = `YouTube yuklashda/kirishda xatolik (ytdl-core ${segmentNumber}/${numSegments}). Cookie faylini (${persistentCookieFilePath})/URL/Video holatini tekshiring. Jarayon to'xtatildi. (${segmentErr.message})`
						if (
							segmentErr.message?.includes('YouTube access error')
						) {
							userMsg = `YouTube kirish xatosi (${segmentNumber}/${numSegments}): Cookie fayli (${persistentCookieFilePath}) yaroqsiz/eskirgan yoki video maxfiy/yosh tekshiruvi? (${segmentErr.message})`
						} else if (
							segmentErr.message?.includes('Network error')
						) {
							userMsg = `Tarmoq xatosi (${segmentNumber}/${numSegments}): YouTube'ga ulanish uzildi (ytdl-core network error). (${segmentErr.message})`
						}
						segmentLogger.error(
							'Fatal ytdl-core stream related error occurred. Aborting job.'
						)
						await pushTranscriptionEvent(
							jobId,
							userMsg,
							true,
							broadcast
						)
						throw new Error( // Re-throw to exit the main try block
							`Aborting job due to fatal ytdl-core stream failure on segment ${segmentNumber}: ${segmentErr.message}`
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
					} else if (
						segmentErr.message?.includes(
							'streamAudioWithYtdlCore failed'
						)
					) {
						// Catch errors from the initial ytdl call within the segment loop
						segmentLogger.error(
							'Fatal error initiating ytdl-core stream. Aborting job.'
						)
						await pushTranscriptionEvent(
							jobId,
							`YouTube audio streamni boshlashda xatolik (${segmentNumber}/${numSegments}). Cookie faylini (${persistentCookieFilePath})/URL/Video holatini tekshiring. Jarayon to'xtatildi. (${segmentErr.message})`,
							true,
							broadcast
						)
						throw new Error(
							`Aborting job due to fatal ytdl-core stream initiation failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}

					// Non-fatal errors will allow retry loop to continue
					await delay(2000 + attempt * 1000) // Backoff before retry
				} finally {
					// Ensure resources are cleaned up after each attempt
					// Use the cleanup logic embedded in the promise's reject handler now
					// If the promise resolved successfully, streams should have ended naturally
					if (
						audioStream &&
						!audioStream.destroyed &&
						!segmentProcessedSuccessfully
					) {
						// Extra check in case promise logic didn't catch an edge case
						segmentLogger.warn(
							'Manually destroying ytdl audio stream in finally block (attempt failed, promise might not have rejected cleanly).'
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
		jobLogger.error(
			{ error: err.message, stack: err.stack },
			'Critical error caught in runTranscriptionJob main try-catch block'
		)

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

		// --- Update Final Error Reporting ---
		if (broadcast) {
			try {
				let clientErrorMessage = `Serverda kutilmagan xatolik yuz berdi. (${err.message?.substring(0, 100) || 'No details'}...)`

				// Check for specific fatal errors thrown from the segment loop or info fetch
				if (
					err.message?.includes(
						'Aborting job due to fatal ytdl-core stream failure'
					)
				) {
					if (
						err.message?.includes('age confirmation') ||
						err.message?.includes('age-restricted')
					) {
						clientErrorMessage = `Xatolik: YouTube yosh tekshiruvini talab qilmoqda. Cookie faylini (${persistentCookieFilePath}) yangilang/tekshiring. Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
					} else if (err.message?.includes('YouTube access error')) {
						clientErrorMessage = `Xatolik: YouTube kirish xatosi (cookie fayli (${persistentCookieFilePath}) yaroqsiz/video maxfiy?). Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
					} else if (err.message?.includes('Network error')) {
						clientErrorMessage = `Xatolik: Tarmoq xatosi (YouTube'ga ulanib bo'lmadi?). Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
					} else {
						clientErrorMessage = `Xatolik: YouTube'dan yuklab bo'lmadi yoki kirishda/tarmoqda muammo (cookie fayli: ${persistentCookieFilePath}). Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
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
					err.message?.includes(
						"Video ma'lumotlarini olib bo'lmadi (ytdl-core)"
					)
				) {
					// Catch initial info fetch errors specifically
					if (
						err.message?.includes('age confirmation') ||
						err.message?.includes('age-restricted')
					) {
						clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (ytdl-core). YouTube yosh tekshiruvini talab qilmoqda. Cookie faylini (${persistentCookieFilePath}) yangilang/tekshiring. (${err.message?.substring(0, 100)}...)`
					} else if (err.message?.includes('Invalid YouTube URL?')) {
						clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (ytdl-core). Noto'g'ri YouTube URL? (${err.message?.substring(0, 100)}...)`
					} else {
						clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (ytdl-core). URL/Cookie faylini (${persistentCookieFilePath})/Video holatini/Tarmoqni tekshiring. (${err.message?.substring(0, 100)}...)`
					}
				} else if (
					err.message?.includes('GOOGLE_CLOUD_BUCKET_NAME') ||
					err.message?.includes('Bucket topilmadi')
				) {
					clientErrorMessage = `Server konfiguratsiya xatosi: Bucket topilmadi.`
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
	}
}
