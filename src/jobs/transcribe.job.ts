// --- NEW IMPORTS for @ybd-project/ytdl-core ---
import {
	toPipeableStream,
	YTDL_DownloadOptions,
	YtdlCore
} from '@ybd-project/ytdl-core'
// --- END NEW IMPORTS ---

import ffmpeg from 'fluent-ffmpeg'
import { HttpsProxyAgent } from 'hpagent'
import path from 'path'
import { performance } from 'perf_hooks'
import { Readable } from 'stream'
import { CookieJar } from 'tough-cookie'

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

// --- Helper to create agent with cookies (Remains largely the same) ---
async function createAgentWithCookies(
	logPrefix: string
): Promise<HttpsProxyAgent | undefined> {
	let agent: HttpsProxyAgent | undefined = undefined
	try {
		// --- Using Secret Manager is recommended for Cloud Run ---
		// TODO: Implement fetching cookie string from Secret Manager here
		// const cookieFileContent = await getCookieContentFromSecretManager(logPrefix);
		// --- Fallback to file for now ---
		const cookieFileContent = await fs.readFile(
			persistentCookieFilePath,
			'utf-8'
		)

		if (cookieFileContent && cookieFileContent.trim().length > 0) {
			const jar = new CookieJar()
			const lines = cookieFileContent.split('\n')
			let parsedCount = 0
			for (const line of lines) {
				const trimmedLine = line.trim()
				if (!trimmedLine || trimmedLine.startsWith('#')) continue
				try {
					await jar.setCookie(
						trimmedLine,
						'https://www.youtube.com',
						{ ignoreError: false }
					)
					parsedCount++
				} catch (cookieParseError: any) {
					logger.warn(
						{ error: cookieParseError.message, line: trimmedLine },
						`${logPrefix}: Failed to parse cookie line, skipping.`
					)
				}
			}

			if (parsedCount === 0) {
				logger.warn(
					`${logPrefix}: Cookie content available, but no valid cookies were parsed.`
				)
				return undefined
			}

			agent = new HttpsProxyAgent({
				keepAlive: true,
				keepAliveMsecs: 1000,
				maxSockets: 256,
				maxFreeSockets: 256,
				scheduling: 'lifo',
				proxy: undefined,
				// @ts-ignore // tough-cookie types might mismatch slightly with hpagent expectations
				cookieJar: jar
			})
			logger.info(
				`${logPrefix}: Using cookies from ${persistentCookieFilePath} (${parsedCount} cookies parsed).`
			)
		} else {
			logger.warn(
				`${logPrefix}: Cookie source was empty. Proceeding without cookies.`
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

// --- Get Video Info using @ybd-project/ytdl-core ---
async function getVideoInfoWithYtdlProject(
	youtubeUrl: string
): Promise<VideoInfo> {
	const logPrefix = 'ybd-ytdl-info'
	logger.info(`${logPrefix}: Fetching info for ${youtubeUrl}.`)

	const agent = await createAgentWithCookies(logPrefix)
	const ytdlInstance = new YtdlCore()

	const infoOptions: YTDL_DownloadOptions & {
		// Use YTDL_DownloadOptions for potential future use
		requestOptions?: { agent?: HttpsProxyAgent }
	} = {}
	if (agent) {
		infoOptions.requestOptions = { agent }
	}

	try {
		logger.info(`${logPrefix}: Calling ytdlInstance.getBasicInfo()...`)
		// Use getBasicInfo for efficiency when only title/duration needed
		const info = await ytdlInstance.getBasicInfo(youtubeUrl, infoOptions)

		const title = info.videoDetails?.title
		// Ensure durationStr is treated as string before parseInt
		const durationStr = info.videoDetails?.lengthSeconds
		const duration = durationStr ? parseInt(String(durationStr), 10) : 0

		if (!title || isNaN(duration) || duration <= 0) {
			logger.error(
				{ videoDetails: info.videoDetails },
				`${logPrefix}: Invalid video info structure received.`
			)
			throw new Error(
				'Invalid video info structure received from @ybd-project/ytdl-core.'
			)
		}

		logger.info(
			`${logPrefix}: Successfully fetched info for title: ${title}, duration: ${duration}s`
		)
		return { title, duration }
	} catch (error: any) {
		const errorMessage =
			error?.message || 'Unknown @ybd-project/ytdl-core info error'
		logger.error(
			{
				error: errorMessage,
				statusCode: error?.statusCode,
				reason: error?.reason,
				stack: error?.stack?.substring(0, 500)
			},
			`${logPrefix}: Failed to get video info.`
		)

		let specificError = `ytdl-project info failed: ${errorMessage}`
		if (
			errorMessage.includes('private video') ||
			errorMessage.includes('Login required') ||
			errorMessage.includes('confirm your age') ||
			errorMessage.includes('unavailable') ||
			error?.statusCode === 403 ||
			error?.statusCode === 401 ||
			error?.statusCode === 410 || // Often indicates expired/restricted/deleted
			errorMessage.includes('age-restricted') ||
			errorMessage.includes('Sign in')
		) {
			specificError = `YouTube access error (ybd-ytdl info): Video might be private/unavailable/premiere, require login/age confirmation, or cookie file (${persistentCookieFilePath}) is invalid/expired/rejected. (Status: ${error?.statusCode || 'N/A'}, Msg: ${errorMessage})`
		} else if (
			errorMessage.includes('No video id found') ||
			errorMessage.includes('Invalid URL') ||
			errorMessage.includes('Not a YouTube domain')
		) {
			specificError = `ytdl-project info failed: Invalid YouTube URL? (${errorMessage})`
		} else if (
			error?.code === 'ENOTFOUND' ||
			error?.code === 'ECONNRESET' ||
			error?.code === 'ETIMEDOUT' ||
			errorMessage.includes('fetch failed') // Generic fetch error from underlying library
		) {
			specificError = `ytdl-project info failed: Network error (${error.code || 'N/A'}). Check connection. (${errorMessage})`
		} else {
			// Keep the original message for other errors
			specificError = `ytdl-project info failed: (${errorMessage})`
		}

		throw new Error(specificError)
	}
}

// --- Stream Audio using @ybd-project/ytdl-core ---
async function streamAudioWithYtdlProject(
	youtubeUrl: string
): Promise<Readable> {
	const logPrefix = 'ybd-ytdl-stream'
	logger.info(`${logPrefix}: Initiating audio stream for ${youtubeUrl}.`)

	const agent = await createAgentWithCookies(logPrefix)
	const ytdlInstance = new YtdlCore()

	// *** FIXED ***: Removed quality: 'highestaudio' as it caused the error.
	// Relying on filter: 'audioonly' is generally sufficient for the library
	// to pick the best available audio format.
	const downloadOptions: YTDL_DownloadOptions & {
		requestOptions?: { agent?: HttpsProxyAgent }
	} = {
		filter: 'audioonly'
		// quality: 'highestaudio', // <-- REMOVED THIS LINE
	}
	if (agent) {
		downloadOptions.requestOptions = { agent }
	}

	try {
		logger.info(
			`${logPrefix}: Calling ytdlInstance.download() with filter: 'audioonly'...`
		)
		// ytdlInstance.download returns a custom stream-like object
		const ytdlStream = await ytdlInstance.download(
			youtubeUrl,
			downloadOptions // <-- Pass corrected options here
		)

		// Convert the custom stream to a standard Node.js Readable stream
		const readableStream = toPipeableStream(ytdlStream)

		// Basic error logging on the final readable stream
		readableStream.on('error', (err: any) => {
			logger.error(
				{
					error: err.message,
					code: err.code,
					statusCode: err.statusCode
				},
				`${logPrefix}: Error event emitted on ybd-ytdl stream during download/piping.`
			)
			// Note: Errors here might also be caught by FFmpeg or the upload process later
		})

		logger.info(`${logPrefix}: ybd-ytdl stream initiated successfully.`)
		return readableStream
	} catch (error: any) {
		const errorMessage =
			error?.message || 'Unknown @ybd-project/ytdl-core stream init error'
		logger.error(
			{
				error: errorMessage,
				statusCode: error?.statusCode,
				reason: error?.reason,
				stack: error?.stack?.substring(0, 500)
			},
			`${logPrefix}: Failed to initiate ybd-ytdl stream.`
		)

		let specificError = `ytdl-project stream init failed: ${errorMessage}`
		// *** Added check for the specific error message from the user ***
		if (errorMessage.includes('No such format found')) {
			specificError = `ytdl-project stream init failed: Couldn't find a suitable audio format. (${errorMessage})`
		} else if (
			errorMessage.includes('private video') ||
			errorMessage.includes('Login required') ||
			errorMessage.includes('confirm your age') ||
			errorMessage.includes('unavailable') ||
			error?.statusCode === 403 ||
			error?.statusCode === 401 ||
			error?.statusCode === 410 || // Often indicates expired/restricted/deleted
			errorMessage.includes('age-restricted') ||
			errorMessage.includes('Sign in')
		) {
			specificError = `YouTube access error (ybd-ytdl stream init): Video might be private/unavailable/premiere, require login/age confirmation, or cookie file (${persistentCookieFilePath}) is invalid/expired/rejected. (Status: ${error?.statusCode || 'N/A'}, Msg: ${errorMessage})`
		} else if (
			errorMessage.includes('No video id found') ||
			errorMessage.includes('Invalid URL') ||
			errorMessage.includes('Not a YouTube domain')
		) {
			specificError = `ytdl-project stream init failed: Invalid YouTube URL? (${errorMessage})`
		} else if (
			error?.code === 'ENOTFOUND' ||
			error?.code === 'ECONNRESET' ||
			error?.code === 'ETIMEDOUT' ||
			errorMessage.includes('fetch failed') // Generic fetch error from underlying library
		) {
			specificError = `ytdl-project stream init failed: Network error (${error.code || 'N/A'}). Check connection. (${errorMessage})`
		} else {
			// Keep the original message for other errors
			specificError = `ytdl-project stream init failed: (${errorMessage})`
		}

		// Instead of returning an error stream, re-throw the specific error
		// This makes the main job logic catch it directly in its try/catch block
		throw new Error(specificError)

		/* // --- OLD APPROACH: Returning an error stream ---
		// Return a stream that immediately emits the specific error
		const errorStream = new Readable({
			read() {
				// Defer the error emission slightly to allow event listeners to attach
				process.nextTick(() => {
					this.emit('error', new Error(specificError))
					this.push(null) // End the stream after emitting the error
				})
			}
		})
		return errorStream
        */
	}
}

// --- Main Transcription Job Logic (Unchanged below this line, but error handling in the main loop will now catch the thrown error from streamAudioWithYtdlProject) ---

export async function pushTranscriptionEvent(
	jobId: string,
	content: string,
	completed = false,
	broadcast?: (content: string, completed: boolean) => void
) {
	const message =
		typeof content === 'string' ? content : JSON.stringify(content)
	if (!completed || message.length < 500) {
		logger.info(
			{ jobId, completed, length: message.length },
			'Pushing transcription event'
		)
	} else {
		logger.info(
			{ jobId, completed, length: message.length },
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

	jobLogger.info(
		'Starting transcription job (using @ybd-project/ytdl-core)...'
	)

	// Check cookie file existence (logging only - remains the same)
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
		jobLogger.info(`Fetching video info via @ybd-project/ytdl-core...`)

		try {
			videoInfo = await getVideoInfoWithYtdlProject(url)
			jobLogger.info(
				`Successfully fetched video info for title: ${videoInfo.title}`
			)
		} catch (err: any) {
			jobLogger.error(
				{ error: err.message, stack: err.stack },
				'Failed to get video info from @ybd-project/ytdl-core.'
			)
			// Use the specific error message generated by getVideoInfoWithYtdlProject
			let errorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (ytdl-project). URL, server yoki cookie faylini (${persistentCookieFilePath}) tekshiring. (${err.message || 'Unknown ytdl-project info error'})`

			// Translate specific technical errors to user-friendly messages
			if (err.message?.includes('YouTube access error')) {
				if (
					err.message?.includes('age confirmation') ||
					err.message?.includes('410') ||
					err.message?.includes('Status: 410')
				) {
					errorMessage = `Video ma'lumotlarini olib bo'lmadi (ytdl-project). YouTube yosh tekshiruvini talab qilmoqda yoki video cheklangan (410?). Cookie faylini (${persistentCookieFilePath}) yangilang/tekshiring. (${err.message})`
				} else {
					errorMessage = `Video ma'lumotlarini olib bo'lmadi (ytdl-project). YouTube kirish xatosi (maxfiy/mavjud emas/cookie yaroqsiz?). Cookie faylini (${persistentCookieFilePath}) tekshiring. (${err.message})`
				}
			} else if (err.message?.includes('Invalid YouTube URL?')) {
				errorMessage = `Video ma'lumotlarini olib bo'lmadi (ytdl-project): Noto'g'ri YouTube URL? (${err.message})`
			} else if (err.message?.includes('Network error')) {
				errorMessage = `Video ma'lumotlarini olib bo'lmadi (ytdl-project): Tarmoq xatosi. Internet ulanishini tekshiring. (${err.message})`
			} // No need for a generic 'else' here, the default `errorMessage` covers it.

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
			'Ovoz yuklanmoqda (ybd-ytdl)...',
			false,
			broadcast
		)
		await delay(500)
		const segmentDuration = 150
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
			const safeActualDuration = Math.max(0.1, actualDuration) // Ensure duration is not zero for ffmpeg
			const segmentEndTime = segmentStartTime + safeActualDuration

			const destFileName = `segment_${jobId}_${segmentNumber}.mp3`
			const gcsUri = `gs://${bucketName}/${destFileName}`

			let segmentProcessedSuccessfully = false
			let attempt = 0
			const maxAttempts = 2 // Keep retries low for stream errors

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
				let audioStream: Readable | null = null

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
					await delay(1000 * attempt) // Simple delay
				}

				try {
					await pushTranscriptionEvent(
						jobId,
						`Bo'lak ${segmentNumber}/${numSegments} yuklanmoqda (ybd-ytdl)...`,
						false,
						broadcast
					)
					segmentLogger.info(
						`Attempting segment stream via ybd-ytdl...`
					)
					// Get the audio stream for the current segment attempt
					// CRITICAL: This call is now inside the try block and can throw directly
					audioStream = await streamAudioWithYtdlProject(url)

					// If streamAudioWithYtdlProject succeeded, proceed
					segmentLogger.info(
						`ybd-ytdl stream obtained. Starting FFmpeg encoding and slicing...`
					)
					ffmpegCommand = ffmpeg(audioStream) // Use the fresh stream
						.inputOption(`-ss ${segmentStartTime}`) // Seek start time
						.inputOption(`-to ${segmentEndTime}`) // Seek end time (replaces -t duration)
						.format('mp3')
						.audioCodec('libmp3lame')
						.audioBitrate('96k') // Reduced bitrate slightly
						.on('start', cmd =>
							segmentLogger.info(`FFmpeg started: ${cmd}`)
						)
						.on('error', (err, stdout, stderr) => {
							// This catches errors *during* ffmpeg processing (e.g., codec issues, stream ending abruptly)
							// It might also catch errors propagated from the input audioStream if they happen *after* ffmpeg starts
							segmentLogger.error(
								{ message: err.message, stdout, stderr },
								`FFmpeg error event during segment processing`
							)
							// The promise reject handler below will usually catch this,
							// but logging here provides more context.
						})
						.on('end', () =>
							segmentLogger.info(
								`FFmpeg processing finished (stream ended).`
							)
						)

					// --- Wrap ffmpeg processing and upload in a Promise ---
					await new Promise<void>((resolve, reject) => {
						if (!ffmpegCommand || !audioStream) {
							return reject(
								new Error(
									'FFmpeg command or audio stream was not initialized (should not happen here).'
								)
							)
						}
						const ffmpegOutputStream = ffmpegCommand.pipe()
						let promiseRejected = false // Flag to prevent double rejection/cleanup

						const cleanupAndReject = (err: Error) => {
							if (promiseRejected) return
							promiseRejected = true
							segmentLogger.error(
								{ error: err.message },
								'Error during FFmpeg/Upload, attempting cleanup.'
							)
							try {
								// Destroy input stream forcefully on error ONLY if it exists and isn't already destroyed
								if (audioStream && !audioStream.destroyed) {
									segmentLogger.warn(
										'Destroying ybd-ytdl readable stream due to downstream error...'
									)
									audioStream.destroy(err)
								}
								// Kill ffmpeg process if it's running
								if (ffmpegCommand) {
									segmentLogger.warn(
										`Killing ffmpeg due to error: ${err.message}`
									)
									try {
										ffmpegCommand.kill('SIGTERM')
									} catch (e) {}
									// Use timeout for SIGKILL in case SIGTERM doesn't work immediately
									setTimeout(() => {
										try {
											ffmpegCommand?.kill('SIGKILL')
										} catch (e) {}
									}, 1000)
								}
							} catch (killErr: any) {
								segmentLogger.warn(
									{ error: killErr.message },
									'Error during cleanup attempt after main error.'
								)
							}
							reject(err) // Reject the main promise
						}

						// --- Error Handling for Streams ---
						// Handle errors from the *input* ytdl stream that might occur *after* piping starts
						audioStream.on('error', inputError => {
							segmentLogger.error(
								{
									error: inputError.message,
									code: (inputError as any).code,
									statusCode: (inputError as any).statusCode
								},
								'Error on ybd-ytdl input stream *during* ffmpeg processing'
							)
							let specificMsg = `Input stream error during processing: ${inputError.message}`
							const statusCode = (inputError as any).statusCode
							if (
								statusCode === 403 ||
								statusCode === 401 ||
								statusCode === 410 ||
								inputError.message.includes('Login required') ||
								inputError.message.includes('private video') ||
								inputError.message.includes('age-restricted') ||
								inputError.message.includes('Sign in')
							) {
								specificMsg = `Input stream error during processing: YouTube access error (Status: ${statusCode || 'N/A'}/Private/Age/Login?). Check cookie file (${persistentCookieFilePath}). Msg: ${inputError.message}`
							} else if (
								(inputError as any).code === 'ECONNRESET' ||
								(inputError as any).code === 'ETIMEDOUT' ||
								inputError.message.includes('socket hang up') ||
								inputError.message.includes('fetch failed')
							) {
								specificMsg = `Input stream error during processing: Network error (${(inputError as any).code || 'N/A'}). Connection lost during download? Msg: ${inputError.message}`
							}
							cleanupAndReject(new Error(specificMsg))
						})

						// Handle errors directly from the ffmpeg command object (redundant with .on('error') on command?)
						// ffmpegCommand.on('error', err => cleanupAndReject(new Error(`FFmpeg command failed directly: ${err.message}`)));

						// Handle errors from the stream ffmpeg *produces* (e.g., piping issues)
						ffmpegOutputStream.on('error', outputError =>
							cleanupAndReject(
								new Error(
									`FFmpeg output stream error: ${outputError.message}`
								)
							)
						)
						// --- End Error Handling ---

						// Start the GCS upload
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
										'GCS upload finished, but an error occurred earlier during processing.'
									)
									gcsUploadSucceeded = false // Mark as failed if error happened before completion
									// No need to reject here, cleanupAndReject already handled it
								}
							})
							.catch(uploadErr => {
								segmentLogger.error(
									{ error: uploadErr.message },
									'GCS upload failed.'
								)
								// Trigger cleanup and reject the main promise
								cleanupAndReject(
									new Error(
										`GCS upload failed: ${uploadErr.message}`
									)
								)
							})

						// Handle ffmpeg finishing (this usually coincides with upload finishing)
						ffmpegCommand.on('end', () => {
							if (!promiseRejected) {
								segmentLogger.info(
									'FFmpeg processing finished event received (and no prior errors).'
								)
								// Resolve might have already been called by the upload promise, this is slightly redundant but safe.
								// If upload promise finishes first, resolve() is called there.
								// If ffmpeg 'end' happens first (unlikely with slow uploads), this might resolve slightly earlier.
							}
						})
					})
					// --- End ffmpeg/upload Promise ---

					segmentLogger.info(
						'FFmpeg/Upload promise resolved successfully.'
					)

					// --- Transcriptions & Editing ---
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
						const stream11 = await getGCSFileStream(gcsUri) // Get stream for 11Labs
						transcriptElevenLabs =
							await transcribeAudioElevenLabs(stream11) // Transcribe
						if (transcriptElevenLabs) {
							segmentLogger.info(
								`ElevenLabs transcription done (length: ${transcriptElevenLabs.length}).`
							)
						} else {
							segmentLogger.warn(
								`ElevenLabs transcription returned empty/null for ${gcsUri}`
							)
							if (!transcriptGoogle) {
								await pushTranscriptionEvent(
									jobId,
									`${segmentNumber}/${numSegments}-chi ElevenLabs matnida xatolik (bo'sh natija). Google ham ishlamadi.`,
									false,
									broadcast
								)
							} else {
								await pushTranscriptionEvent(
									jobId,
									`${segmentNumber}/${numSegments}-chi ElevenLabs matnida xatolik (bo'sh natija). Google natijasi bilan davom etilmoqda...`,
									false,
									broadcast
								)
							}
						}
					} catch (elevenLabsError: any) {
						segmentLogger.error(
							{ error: elevenLabsError.message },
							`ElevenLabs transcription failed for ${gcsUri}`
						)
						if (!transcriptGoogle) {
							throw new Error( // Throw to trigger retry or job failure
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

					// Prepare inputs for Gemini, handling null/empty cases
					const googleInput = transcriptGoogle || ''
					const elevenLabsInput = transcriptElevenLabs ?? '' // Use empty string if null/failed

					// If BOTH failed or returned empty, we can't proceed with this segment
					if (googleInput === '' && elevenLabsInput === '') {
						segmentLogger.error(
							`Both Google and ElevenLabs transcription failed or returned empty for ${gcsUri}`
						)
						throw new Error( // Throw to trigger retry or job failure
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
						googleInput, // Always pass Google text (or empty string)
						elevenLabsInput // Pass ElevenLabs text (or empty string if null/failed)
					)
					if (!finalText) {
						segmentLogger.error(
							`Gemini editing returned empty/null for ${gcsUri}`
						)
						throw new Error('Gemini editing returned empty.') // Throw to retry or fail
					} else {
						segmentLogger.info(
							`Gemini editing done (length: ${finalText.length}).`
						)
					}

					editedTexts.push(finalText) // Add successful segment text
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
					// This catch block now handles errors from:
					// 1. streamAudioWithYtdlProject (initial stream setup failure)
					// 2. The ffmpeg/upload Promise (including input stream errors during processing, ffmpeg errors, upload errors)
					// 3. Transcription/Editing failures (Google, 11Labs, Gemini)

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

					// --- Check for Fatal Errors (Errors that should stop the entire job) ---

					// Check for errors originating from the *initial* ytdl stream setup
					if (
						segmentErr.message?.includes(
							'ytdl-project stream init failed'
						)
					) {
						let userMsg = `YouTube audio streamni boshlashda xatolik (${segmentNumber}/${numSegments}). Cookie faylini (${persistentCookieFilePath})/URL/Video holatini tekshiring. Jarayon to'xtatildi. (${segmentErr.message})`
						if (
							segmentErr.message?.includes(
								"Couldn't find a suitable audio format"
							)
						) {
							userMsg = `Video uchun mos audio format topilmadi (ytdl-project, ${segmentNumber}/${numSegments}). Jarayon to'xtatildi. (${segmentErr.message})`
						} else if (
							segmentErr.message?.includes('YouTube access error')
						) {
							if (segmentErr.message?.includes('Status: 410')) {
								userMsg = `YouTube kirish xatosi (${segmentNumber}/${numSegments}): Video topilmadi yoki cheklangan (410). Cookie fayli (${persistentCookieFilePath}) eskirgan yoki video o'chirilgan bo'lishi mumkin? Jarayon to'xtatildi. (${segmentErr.message})`
							} else {
								userMsg = `YouTube kirish xatosi (${segmentNumber}/${numSegments}): Cookie fayli (${persistentCookieFilePath}) yaroqsiz/eskirgan yoki video maxfiy/yosh tekshiruvi? Jarayon to'xtatildi. (${segmentErr.message})`
							}
						} else if (
							segmentErr.message?.includes('Network error')
						) {
							userMsg = `Tarmoq xatosi (${segmentNumber}/${numSegments}): YouTube'ga ulanish uzildi (ytdl-project network error). Jarayon to'xtatildi. (${segmentErr.message})`
						}
						segmentLogger.error(
							'Fatal ybd-ytdl stream initiation error occurred. Aborting job.'
						)
						await pushTranscriptionEvent(
							jobId,
							userMsg,
							true,
							broadcast
						)
						throw new Error( // Re-throw to break out and fail the job
							`Aborting job due to fatal ybd-ytdl stream initiation failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}
					// Check for errors from the ytdl stream *during* processing (caught by promise cleanup)
					else if (
						segmentErr.message?.includes(
							'Input stream error during processing:'
						)
					) {
						let userMsg = `YouTube yuklashda/kirishda xatolik (ytdl-project ${segmentNumber}/${numSegments} davomida). Cookie faylini (${persistentCookieFilePath})/URL/Video holatini tekshiring. Jarayon to'xtatildi. (${segmentErr.message})`
						if (
							segmentErr.message?.includes('YouTube access error')
						) {
							if (segmentErr.message?.includes('Status: 410')) {
								userMsg = `YouTube kirish xatosi (${segmentNumber}/${numSegments} davomida): Video topilmadi/cheklangan (410?). Cookie (${persistentCookieFilePath}) eskirgan? Jarayon to'xtatildi. (${segmentErr.message})`
							} else {
								userMsg = `YouTube kirish xatosi (${segmentNumber}/${numSegments} davomida): Cookie fayli (${persistentCookieFilePath}) yaroqsiz/eskirgan yoki video maxfiy/yosh tekshiruvi? Jarayon to'xtatildi. (${segmentErr.message})`
							}
						} else if (
							segmentErr.message?.includes('Network error')
						) {
							userMsg = `Tarmoq xatosi (${segmentNumber}/${numSegments} davomida): YouTube'ga ulanish uzildi (ytdl-project network error). Jarayon to'xtatildi. (${segmentErr.message})`
						}
						segmentLogger.error(
							'Fatal ybd-ytdl stream error during processing occurred. Aborting job.'
						)
						await pushTranscriptionEvent(
							jobId,
							userMsg,
							true,
							broadcast
						)
						throw new Error( // Re-throw to fail the job
							`Aborting job due to fatal ybd-ytdl stream failure during processing on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}
					// Check for fatal FFmpeg errors (caught by promise cleanup)
					else if (
						segmentErr.message?.includes('FFmpeg command failed') ||
						segmentErr.message?.includes(
							'FFmpeg output stream error'
						) ||
						segmentErr.message?.includes('FFmpeg error event') // Catch errors logged by the event handler too
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
						throw new Error( // Re-throw to fail the job
							`Aborting job due to fatal FFmpeg failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}
					// Check for fatal GCS upload errors (caught by promise cleanup)
					else if (
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
						throw new Error( // Re-throw to fail the job
							`Aborting job due to fatal GCS upload failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}
					// Check for fatal transcription/editing errors where both sources failed or Gemini failed
					else if (
						segmentErr.message?.includes(
							'Both Google and ElevenLabs transcription failed'
						) ||
						segmentErr.message?.includes(
							'Gemini editing returned empty'
						) ||
						segmentErr.message?.includes(
							'ElevenLabs failed and Google text is also empty'
						) // From 11labs catch block
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
						throw new Error( // Re-throw to fail the job
							`Aborting job due to fatal transcription/editing failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}
					// If the error is not considered fatal (e.g., maybe a temporary network blip during transcription API call, which might recover on retry), allow the retry loop to continue.
					await delay(2000 + attempt * 1000) // Wait longer before retrying
				} finally {
					// --- Cleanup after each attempt ---
					// Ensure stream is destroyed if it exists and wasn't destroyed by an error handler already
					// Especially important if the attempt fails *after* the stream was obtained but *before* an error handler destroyed it
					if (
						audioStream &&
						!audioStream.destroyed &&
						!segmentProcessedSuccessfully
					) {
						segmentLogger.warn(
							`Ensuring ybd-ytdl audio stream is destroyed in finally block (attempt ${attempt}, success: ${segmentProcessedSuccessfully}).`
						)
						audioStream.destroy() // Ensure stream resources are released
					}
					// Force kill ffmpeg if it exists and the segment failed (as a safeguard)
					if (
						ffmpegCommand &&
						!segmentProcessedSuccessfully &&
						attempt >= maxAttempts
					) {
						segmentLogger.warn(
							`Force killing ffmpeg in finally block after max attempts failed.`
						)
						try {
							ffmpegCommand.kill('SIGKILL')
						} catch (e) {}
					}

					// Delete the GCS file only if the upload *definitely* succeeded
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
						// Log if skipping delete because upload didn't happen or failed before completion
						segmentLogger.info(
							`Skipping GCS delete for ${destFileName} as upload did not succeed (gcsUploadSucceeded: ${gcsUploadSucceeded}).`
						)
					}
					await delay(300) // Small delay before next attempt or next segment
				}
			} // --- End retry loop ---

			// If segment still failed after all attempts, abort the job
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
				// Throw an error to exit the main segment loop and enter the final catch block
				throw new Error(
					`Failed to process segment ${segmentNumber} after ${maxAttempts} attempts.`
				)
			}

			i++ // Move to the next segment
		} // --- End segment loop ---

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

		// Join edited texts, clean up extra newlines, and trim
		const combinedResult = editedTexts
			.join('\n\n') // Join segments with double newline
			.replace(/(\n\s*){3,}/g, '\n\n') // Replace 3+ newlines with just two
			.trim() // Remove leading/trailing whitespace

		const duration = performance.now() - startTime
		jobLogger.info(`Job completed in ${formatDuration(duration)}`)
		await pushTranscriptionEvent(
			jobId,
			`Yakuniy matn jamlandi!`,
			false,
			broadcast
		)
		await delay(500)

		// Format the final output with title, duration notice, and converted text
		const finalTitle = videoInfo.title || "Noma'lum Sarlavha"
		const finalTranscript = `<i style="display: block; font-style: italic; text-align: center;">🕒 Transkripsiya uchun ${formatDuration(duration)} vaqt ketdi.</i><h1 style="font-weight: 700; font-size: 1.8rem; margin: 1rem 0; text-align: center; line-height: 1;">${finalTitle}</h1>\n\n<p style="text-indent: 30px;">${convertToUzbekLatin(combinedResult)}</p>`

		// Save the final result and push it to the client
		await transcriptService.saveFinalTranscript(jobId, finalTranscript)
		jobLogger.info(`Final transcript saved.`)
		await pushTranscriptionEvent(jobId, finalTranscript, true, broadcast) // Mark as completed
		jobStatusUpdated = true // Ensure status is marked correctly
	} catch (err: any) {
		// Catch errors from the main try block (including re-thrown fatal errors from segment loop)
		jobLogger.error(
			{ error: err.message, stack: err.stack },
			'Critical error caught in runTranscriptionJob main try-catch block'
		)

		// Ensure the job status is marked as 'error' in the database if it wasn't already
		if (!jobStatusUpdated) {
			try {
				await transcriptService.error(jobId)
				jobStatusUpdated = true // Mark as updated now
			} catch (dbErr: any) {
				jobLogger.error(
					{ error: dbErr.message },
					'Failed to mark job as error in DB during final catch block'
				)
			}
		}

		// --- Update Final Error Reporting to Client ---
		if (broadcast) {
			try {
				// Use the specific user-friendly messages generated within the catch blocks above
				// The error `err` here will contain the message from the `throw new Error(...)` call that aborted the job.
				let clientErrorMessage = `Serverda kutilmagan xatolik yuz berdi. Jarayon to'xtatildi. (${err.message?.substring(0, 150) || 'No details'}...)` // Default

				// Check if the error message matches one of the specific fatal error formats we threw
				// Note: We check the *start* of the user-facing error message pushed previously
				if (
					err.message?.startsWith(
						'Aborting job due to fatal ybd-ytdl stream initiation failure'
					)
				) {
					// Extract the user message part from the thrown error
					const match = err.message.match(/:\s(.*)/)
					const reason = match ? match[1] : err.message
					// Reconstruct a user-friendly message based on the reason
					if (
						reason.includes("Couldn't find a suitable audio format")
					) {
						clientErrorMessage = `Xatolik: Video uchun mos audio format topilmadi (ytdl-project). Jarayon to'xtatildi. (${reason.substring(0, 100)}...)`
					} else if (reason.includes('YouTube access error')) {
						if (reason.includes('Status: 410')) {
							clientErrorMessage = `Xatolik: YouTube audio streamni boshlashda xatolik (Yosh tekshiruvi/410?). Cookie faylini (${persistentCookieFilePath}) yangilang/tekshiring. Jarayon to'xtatildi. (${reason.substring(0, 100)}...)`
						} else {
							clientErrorMessage = `Xatolik: YouTube audio streamni boshlashda xatolik (Cookie fayli (${persistentCookieFilePath}) yaroqsiz/video maxfiy?). Jarayon to'xtatildi. (${reason.substring(0, 100)}...)`
						}
					} else if (reason.includes('Network error')) {
						clientErrorMessage = `Xatolik: YouTube audio streamni boshlashda xatolik (Tarmoq xatosi?). Jarayon to'xtatildi. (${reason.substring(0, 100)}...)`
					} else {
						clientErrorMessage = `Xatolik: YouTube audio streamni boshlashda xatolik. Cookie faylini (${persistentCookieFilePath})/URL/Video holatini tekshiring. Jarayon to'xtatildi. (${reason.substring(0, 100)}...)`
					}
				} else if (
					err.message?.startsWith(
						'Aborting job due to fatal ybd-ytdl stream failure during processing'
					)
				) {
					const match = err.message.match(/:\s(.*)/)
					const reason = match ? match[1] : err.message
					if (reason.includes('YouTube access error')) {
						if (reason.includes('Status: 410')) {
							clientErrorMessage = `Xatolik: YouTube yuklashda xatolik (410?). Cookie faylini (${persistentCookieFilePath}) yangilang/tekshiring. Jarayon to'xtatildi. (${reason.substring(0, 100)}...)`
						} else {
							clientErrorMessage = `Xatolik: YouTube yuklashda xatolik (Cookie fayli (${persistentCookieFilePath}) yaroqsiz/video maxfiy?). Jarayon to'xtatildi. (${reason.substring(0, 100)}...)`
						}
					} else if (reason.includes('Network error')) {
						clientErrorMessage = `Xatolik: Tarmoq xatosi (YouTube'ga ulanib bo'lmadi?). Jarayon to'xtatildi. (${reason.substring(0, 100)}...)`
					} else {
						clientErrorMessage = `Xatolik: YouTube'dan yuklab bo'lmadi yoki kirishda/tarmoqda muammo (ybd-ytdl; cookie fayli: ${persistentCookieFilePath}). Jarayon to'xtatildi. (${reason.substring(0, 100)}...)`
					}
				} else if (
					err.message?.startsWith(
						'Aborting job due to fatal FFmpeg failure'
					)
				) {
					const match = err.message.match(/:\s(.*)/)
					const reason = match ? match[1] : err.message
					clientErrorMessage = `Xatolik: Audio faylni kodlashda muammo (FFmpeg). Jarayon to'xtatildi. (${reason.substring(0, 100)}...)`
				} else if (
					err.message?.startsWith(
						'Aborting job due to fatal GCS upload failure'
					)
				) {
					const match = err.message.match(/:\s(.*)/)
					const reason = match ? match[1] : err.message
					clientErrorMessage = `Xatolik: Audio bo'lakni bulutga saqlashda muammo (GCS). Jarayon to'xtatildi. (${reason.substring(0, 100)}...)`
				} else if (
					err.message?.startsWith(
						'Aborting job due to fatal transcription/editing failure'
					)
				) {
					const match = err.message.match(/:\s(.*)/)
					const reason = match ? match[1] : err.message
					clientErrorMessage = `Xatolik: Matnni o'girishda/tahrirda tuzatib bo'lmas xatolik. Jarayon to'xtatildi. (${reason.substring(0, 100)}...)`
				} else if (
					err.message?.startsWith('Failed to process segment')
				) {
					// Segment failed after retries (non-fatal error type initially, but fatal after max attempts)
					clientErrorMessage = `Xatolik: ${err.message}` // Use the specific message
				} else if (
					err.message?.includes(
						"Video ma'lumotlarini olib bo'lmadi (ytdl-project)"
					) ||
					err.message?.includes('ytdl-project info failed')
				) {
					// Error getting video info at the start (caught *before* segment loop)
					// Need to reconstruct the user message based on the original info error message inside `err.message`
					const reason = err.message // The error thrown by getVideoInfoWithYtdlProject
					if (
						reason.includes('age confirmation') ||
						reason.includes('410') ||
						reason.includes('Status: 410')
					) {
						clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (ytdl-project). YouTube yosh tekshiruvini talab qilmoqda (410?). Cookie faylini (${persistentCookieFilePath}) yangilang/tekshiring. (${reason.substring(0, 100)}...)`
					} else if (reason.includes('Invalid YouTube URL?')) {
						clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (ytdl-project). Noto'g'ri YouTube URL? (${reason.substring(0, 100)}...)`
					} else if (reason.includes('Network error')) {
						clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (ytdl-project). Tarmoq xatosi. (${reason.substring(0, 100)}...)`
					} else {
						// General info error including access errors not specifically caught above
						clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (ytdl-project). URL/Cookie faylini (${persistentCookieFilePath})/Video holatini/Tarmoqni tekshiring. (${reason.substring(0, 100)}...)`
					}
				} else if (
					err.message?.includes('GOOGLE_CLOUD_BUCKET_NAME') ||
					err.message?.includes('Bucket topilmadi')
				) {
					clientErrorMessage = `Server konfiguratsiya xatosi: Bucket topilmadi.`
				}

				// Push the final, possibly more specific, error message
				await pushTranscriptionEvent(
					jobId,
					clientErrorMessage,
					true, // Mark as completed (with error)
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
