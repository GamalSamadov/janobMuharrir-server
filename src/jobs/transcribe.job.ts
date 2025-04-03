import ytdl from '@distube/ytdl-core'
import ffmpeg from 'fluent-ffmpeg'
import path from 'path'
import { performance } from 'perf_hooks'
import { PassThrough, Readable, Writable } from 'stream'
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

// --- MODIFIED Helper to get request options with cookies ---
async function getRequestOptionsWithCookies(
	logPrefix: string
): Promise<ytdl.downloadOptions['requestOptions']> {
	let requestOptions: ytdl.downloadOptions['requestOptions'] = {}
	try {
		const cookieFileContent = await fs.readFile(
			persistentCookieFilePath,
			'utf-8'
		)
		if (cookieFileContent.trim().length > 0) {
			const jar = new CookieJar()
			const lines = cookieFileContent.split('\n')
			let loadedCookies = 0
			const youtubeUrl = 'https://www.youtube.com/' // URL for getting cookies

			for (const line of lines) {
				const trimmedLine = line.trim()
				if (!trimmedLine || trimmedLine.startsWith('#')) {
					continue
				}
				try {
					jar.setCookieSync(trimmedLine, youtubeUrl, {
						ignoreError: true
					})
					loadedCookies++
				} catch (cookieParseError: any) {
					logger.warn(
						{ error: cookieParseError.message, line: trimmedLine },
						`${logPrefix}: Failed to parse cookie line, skipping.`
					)
				}
			}

			if (loadedCookies > 0) {
				logger.info(
					`${logPrefix}: Successfully loaded ${loadedCookies} cookies from file.`
				)
				// Use async getCookieString as it's the standard way
				const cookieString = await jar.getCookieString(youtubeUrl)
				if (cookieString) {
					logger.info(
						`${logPrefix}: Adding 'Cookie' header to request options.`
					)
					requestOptions = {
						headers: {
							Cookie: cookieString
							// Consider adding a realistic User-Agent if issues persist
							// 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
						}
					}
				} else {
					logger.warn(
						`${logPrefix}: Loaded cookies but failed to get cookie string for ${youtubeUrl}.`
					)
				}
			} else if (lines.some(l => l.trim() && !l.trim().startsWith('#'))) {
				logger.warn(
					`${logPrefix}: Cookie file was not empty, but failed to load any valid cookies.`
				)
			} else {
				logger.info(
					`${logPrefix}: Cookie file was empty. Proceeding without cookies.`
				)
			}
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
	return requestOptions
}
// --- End Helper ---

async function getVideoInfoWithYtdlCore(
	youtubeUrl: string
): Promise<VideoInfo> {
	const logPrefix = 'ytdl-core-info'
	logger.info(`${logPrefix}: Fetching info for ${youtubeUrl}.`)

	const cookieRequestOptions = await getRequestOptionsWithCookies(logPrefix)
	const options: ytdl.getInfoOptions = {
		requestOptions: cookieRequestOptions
	}

	if (options.requestOptions) {
		logger.info(
			`${logPrefix}: Using request options with cookies for getInfo.`
		)
	} else {
		logger.info(
			`${logPrefix}: No cookie options generated, using default request options for getInfo.`
		)
	}

	try {
		logger.info(`${logPrefix}: Calling ytdl.getInfo()...`)
		const info = await ytdl.getInfo(youtubeUrl, options)

		const title = info.videoDetails?.title
		const durationStr = info.videoDetails?.lengthSeconds
		const duration = durationStr ? parseInt(durationStr, 10) : 0

		// --- Add check for formats ---
		if (!info.formats || info.formats.length === 0) {
			logger.error(
				{ videoDetails: info.videoDetails, formats: info.formats },
				`${logPrefix}: Video info fetched but no formats found. Possible region lock, premiere, or parsing issue?`
			)
			// Use a specific error message
			throw new Error(
				'No downloadable formats found' // Keep it simple; specific handling below
			)
		}
		// --- End format check ---

		if (!title || isNaN(duration) || duration <= 0) {
			logger.error(
				{ videoDetails: info.videoDetails },
				`${logPrefix}: Invalid video info structure received (missing title or duration).`
			)
			throw new Error(
				'Invalid video info structure received (missing title/duration).'
			)
		}

		logger.info(
			`${logPrefix}: Successfully fetched info for title: "${title}"`
		)
		return { title, duration }
	} catch (error: any) {
		const errorMessage = error?.message || 'Unknown ytdl-core info error'
		const statusCode = error?.statusCode // Capture status code if available (often is for HTTP errors)
		logger.error(
			{
				error: errorMessage,
				statusCode, // Log status code
				stack: error?.stack?.substring(0, 500)
			},
			`${logPrefix}: Failed to get video info.`
		)

		// --- Refined Error Message Handling ---
		let specificError = `Failed to get video info (ytdl-core).` // Default

		if (errorMessage.includes('Could not extract functions')) {
			// ** This is the specific check for the user's error **
			specificError = `YouTube page parsing failed (ytdl-core 'Could not extract functions'): YouTube might have updated its site structure. Please update the 'ytdl-core' library dependency. Also check if the video URL is correct and accessible. (Msg: ${errorMessage})`
		} else if (errorMessage.includes('No downloadable formats found')) {
			specificError = `YouTube info parsing failed (ytdl-core 'No downloadable formats found'): The video might be region-locked, private, an unstarted live stream/premiere, or require stronger authentication (check cookies). (Msg: ${errorMessage})`
		} else if (
			errorMessage.includes('private video') ||
			errorMessage.includes('Login required') ||
			errorMessage.includes('confirm your age') ||
			errorMessage.includes('unavailable') || // Covers general unavailability
			statusCode === 403 || // Forbidden (often cookies/login/region)
			statusCode === 401 || // Unauthorized
			statusCode === 410 || // Gone (often premieres before start or deleted videos)
			errorMessage.includes('age-restricted') ||
			errorMessage.includes('Terms of Service')
		) {
			specificError = `YouTube access error (ytdl-core info): Video might be private/unavailable/premiere, require login/age confirmation/TOS agreement, or cookie file (${persistentCookieFilePath}) is invalid/expired/rejected. (Status: ${statusCode || 'N/A'}, Msg: ${errorMessage})`
		} else if (
			errorMessage.includes('No video id found') ||
			errorMessage.includes('Not a YouTube domain')
		) {
			specificError = `Invalid YouTube URL? (ytdl-core info): (${errorMessage})`
		} else if (
			error?.code === 'ENOTFOUND' || // DNS lookup failed
			error?.code === 'ECONNRESET' || // Connection reset
			error?.code === 'ETIMEDOUT' || // Connection timed out
			statusCode === 500 || // Internal Server Error (YouTube side)
			statusCode === 502 || // Bad Gateway
			statusCode === 503 || // Service Unavailable
			statusCode === 504 || // Gateway Timeout
			errorMessage.includes('socket hang up') ||
			errorMessage.includes('fetch failed') // Generic network layer errors
		) {
			specificError = `Network error (ytdl-core info): Failed to connect to YouTube or connection interrupted. Check network/firewall/YouTube status. (${error.code || `Status: ${statusCode || 'N/A'}`}). (${errorMessage})`
		} else {
			// Generic fallback for other errors
			specificError = `Failed to get video info (ytdl-core): (Status: ${statusCode || 'N/A'}, Code: ${error?.code || 'N/A'}, Msg: ${errorMessage})`
		}

		throw new Error(specificError) // Re-throw the processed, more informative error
	}
}

async function streamAudioWithYtdlCore(youtubeUrl: string): Promise<Readable> {
	const logPrefix = 'ytdl-core-stream'
	logger.info(`${logPrefix}: Initiating audio stream for ${youtubeUrl}.`)

	const cookieRequestOptions = await getRequestOptionsWithCookies(logPrefix)
	const options: ytdl.downloadOptions = {
		filter: 'audioonly',
		quality: 'highestaudio',
		requestOptions: cookieRequestOptions
	}

	if (options.requestOptions) {
		logger.info(
			`${logPrefix}: Using request options with cookies for download.`
		)
	} else {
		logger.info(
			`${logPrefix}: No cookie options generated, using default request options for download.`
		)
	}

	logger.info(`${logPrefix}: Calling ytdl()...`)
	const stream = ytdl(youtubeUrl, options)

	stream.on('error', (err: any) => {
		const statusCode = err?.statusCode
		// Provide more context for common stream errors
		let specificMsg = `ytdl stream error: ${err.message}`
		if (statusCode === 403 || statusCode === 401 || statusCode === 410) {
			specificMsg = `ytdl stream error: YouTube access denied during download (Status ${statusCode}). Check cookies (${persistentCookieFilePath}) or video status/restrictions. Msg: ${err.message}`
		} else if (
			err.code === 'ECONNRESET' ||
			err.code === 'ETIMEDOUT' ||
			err.message.includes('socket hang up')
		) {
			specificMsg = `ytdl stream error: Network error during download (${err.code}). Connection lost? Msg: ${err.message}`
		} else if (err.message?.includes('formats')) {
			// This might indicate an issue found during stream setup if getInfo passed somehow
			specificMsg = `ytdl stream setup error: Problem finding suitable audio format. Check video status/restrictions. Msg: ${err.message}`
		}

		logger.error(
			{ error: specificMsg, code: err.code, statusCode }, // Log the enhanced message
			`${logPrefix}: Error event emitted on ytdl stream during download.`
		)
		// The ffmpeg promise rejection handler will catch this and abort the segment
	})

	stream.on('progress', (chunkLength, downloaded, total) => {
		/* Optional logging */
	})
	stream.on('end', () => {
		logger.info(`${logPrefix}: ytdl stream ended normally.`)
	})
	stream.on('close', () => {
		logger.info(`${logPrefix}: ytdl stream closed.`)
	}) // Underlying connection closed
	stream.on('response', response => {
		logger.info(
			`${logPrefix}: Received response with status code ${response.statusCode}`
		)
		// Warn aggressively on non-2xx status codes during download
		if (response.statusCode < 200 || response.statusCode >= 300) {
			logger.error(
				// Changed to error as this usually precedes a stream 'error' event
				`${logPrefix}: ytdl download received non-success status code: ${response.statusCode}. Stream likely to fail.`
			)
			// It might be useful to destroy the stream proactively here, though ytdl-core usually emits 'error'
			// if (!stream.destroyed) stream.destroy(new Error(`Download failed with status code ${response.statusCode}`));
		}
	})

	logger.info(`${logPrefix}: ytdl stream initiated. Event handlers attached.`)
	return stream
}

// --- pushTranscriptionEvent remains the same ---
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
	try {
		await transcriptEventService.create(jobId, message, completed)
		if (broadcast) {
			broadcast(message, completed)
		}
	} catch (eventError: any) {
		logger.error(
			{ error: eventError.message, jobId },
			'Failed to create or broadcast transcription event'
		)
	}
}

// --- runTranscriptionJob uses the more detailed errors from getVideoInfo ---
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
		jobLogger.info(
			`Persistent cookie file found at ${persistentCookieFilePath} (${stats.size > 0 ? 'not empty' : 'empty'}). ${stats.size > 0 ? 'Cookies will be loaded if valid.' : 'No cookies will be loaded.'}`
		)
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
			videoInfo = await getVideoInfoWithYtdlCore(url) // Uses updated error handling
			jobLogger.info(
				`Successfully fetched video info for title: "${videoInfo.title}"`
			)
		} catch (err: any) {
			// Catch the specifically formatted error
			jobLogger.error(
				{ error: err.message }, // Log the full processed error message
				'Failed to get video info from ytdl-core.'
			)

			// --- Translate specific errors for the user ---
			let userErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi. Sabab: ${err.message}` // Default uses the detailed error

			if (err.message?.includes('Could not extract functions')) {
				userErrorMessage = `Xatolik: YouTube sahifasini o'qishda muammo ('Could not extract functions'). YouTube sayt tuzilishini o'zgartirgan bo'lishi mumkin. **Iltimos, serverdagi 'ytdl-core' kutubxonasini yangilang.** Video URL to'g'riligini va unga kirish mumkinligini tekshiring.`
			} else if (err.message?.includes('No downloadable formats found')) {
				userErrorMessage = `Xatolik: Video uchun yuklab olinadigan formatlar topilmadi. Video hudud bo'yicha cheklangan, maxfiy, hali boshlanmagan jonli efir/premyera bo'lishi yoki cookie fayllar yetarli bo'lmasligi mumkin (${persistentCookieFilePath}).`
			} else if (err.message?.includes('YouTube access error')) {
				userErrorMessage = `Xatolik: YouTube video ma'lumotiga kirishda muammo (maxfiy/mavjud emas/yosh cheklovi/login/cookie?). Cookie faylini tekshiring (${persistentCookieFilePath}). (${err.message})`
			} else if (err.message?.includes('Invalid YouTube URL?')) {
				userErrorMessage = `Xatolik: Noto'g'ri YouTube URL kiritildi. (${err.message})`
			} else if (err.message?.includes('Network error')) {
				userErrorMessage = `Xatolik: Tarmoq xatosi (YouTube'ga ulanish/javob olishda muammo). Internet ulanishingizni yoki YouTube statusini tekshiring. (${err.message})`
			}
			// Other errors will use the detailed message generated by getVideoInfoWithYtdlCore

			await pushTranscriptionEvent(
				jobId,
				userErrorMessage,
				true,
				broadcast
			)
			await transcriptService.error(jobId)
			jobStatusUpdated = true
			return
		}
		// --- End Get Video Info ---

		const title = videoInfo.title
		const totalDuration = videoInfo.duration

		if (isNaN(totalDuration) || totalDuration <= 0) {
			// This case should be less likely if getInfo succeeded, but keep check
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

		// --- Segment Processing Loop (No changes needed within the loop itself) ---
		// It relies on the improved error handling in streamAudioWithYtdlCore
		// and the promise rejection logic already in place.
		// The final catch block below will handle fatal errors propagated from the loop.

		await pushTranscriptionEvent(
			jobId,
			'Ovoz yuklanmoqda (ytdl-core)...',
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
			const safeActualDuration = Math.max(0.1, actualDuration)
			const segmentEndTime = segmentStartTime + safeActualDuration

			const destFileName = `segment_${jobId}_${segmentNumber}.mp3`
			const gcsUri = `gs://${bucketName}/${destFileName}`

			let segmentProcessedSuccessfully = false
			let attempt = 0
			const maxAttempts = 2

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
				let ffmpegOutputStream: Writable | PassThrough | null = null

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
					await delay(1000 * attempt)
				}

				try {
					await pushTranscriptionEvent(
						jobId,
						`Bo'lak ${segmentNumber}/${numSegments} yuklanmoqda (ytdl-core)...`,
						false,
						broadcast
					)

					segmentLogger.info(
						`Attempting segment stream via ytdl-core...`
					)
					audioStream = await streamAudioWithYtdlCore(url) // Uses updated stream error logging

					segmentLogger.info(
						`Starting FFmpeg encoding and slicing...`
					)

					// --- Wrap ffmpeg processing and upload in a Promise ---
					await new Promise<void>((resolve, reject) => {
						if (!audioStream) {
							return reject(
								new Error(
									'ytdl audio stream was not initialized before ffmpeg.'
								)
							)
						}
						let promiseRejected = false

						const cleanupAndReject = (
							err: Error,
							context: string
						) => {
							if (promiseRejected) return
							promiseRejected = true
							segmentLogger.error(
								{ error: err.message, context },
								'Error during FFmpeg/Upload, attempting cleanup.'
							)
							try {
								if (
									ffmpegOutputStream &&
									!ffmpegOutputStream.destroyed
								) {
									segmentLogger.warn(
										'Destroying ffmpeg output stream due to error...'
									)
									ffmpegOutputStream.destroy(err)
								}
								if (ffmpegCommand) {
									segmentLogger.warn(
										`Killing ffmpeg process due to error: ${err.message}`
									)
									ffmpegCommand.kill('SIGKILL')
								}
								if (audioStream && !audioStream.destroyed) {
									segmentLogger.warn(
										'Destroying ytdl stream due to error...'
									)
									audioStream.destroy(err)
								}
							} catch (killErr: any) {
								segmentLogger.warn(
									{ error: killErr.message },
									'Error during cleanup after main error'
								)
							}
							reject(err)
						}

						// --- Create FFmpeg Command ---
						try {
							ffmpegCommand = ffmpeg(audioStream)
								.inputOption(`-ss ${segmentStartTime}`)
								.inputOption(`-to ${segmentEndTime}`)
								.format('mp3')
								.audioCodec('libmp3lame')
								.audioBitrate('96k')
								.on('start', cmd =>
									segmentLogger.info(`FFmpeg started: ${cmd}`)
								)
								.on('codecData', data => {
									// Usually not needed unless debugging codecs
									// segmentLogger.info( `FFmpeg codec data: ${JSON.stringify(data)}` );
								})
								.on('stderr', stderrLine => {
									// Log specific FFMPEG warnings/errors if needed, otherwise keep as debug
									segmentLogger.debug(
										`FFmpeg stderr: ${stderrLine}`
									)
								})
								.on('error', (err, stdout, stderr) => {
									segmentLogger.error(
										{
											message: err.message,
											stdout, // Can be large, consider logging conditionally
											stderr // Often contains useful error details
										},
										`FFmpeg command error event`
									)
									let ffmpegErrMsg = `FFmpeg command failed: ${err.message}`
									if (
										err.message.includes('Pipe closed') ||
										err.message.includes(
											'Input/output error'
										) ||
										stderr?.includes(
											'Server returned 403'
										) ||
										stderr?.includes('Input stream error')
									) {
										ffmpegErrMsg +=
											' (Likely caused by input ytdl stream ending prematurely - check previous logs for ytdl errors)'
									}
									cleanupAndReject(
										new Error(ffmpegErrMsg),
										'ffmpeg_error_event'
									)
								})
								.on('end', () => {
									// This 'end' only means ffmpeg *finished*, not necessarily successfully
									if (!promiseRejected) {
										segmentLogger.info(
											'FFmpeg processing finished (end event). GCS upload should finalize.'
										)
									} else {
										segmentLogger.warn(
											'FFmpeg end event received, but an error occurred earlier.'
										)
									}
								})

							ffmpegOutputStream = ffmpegCommand.pipe()

							if (!ffmpegOutputStream) {
								throw new Error(
									'ffmpegCommand.pipe() returned null or undefined.'
								)
							}
						} catch (ffmpegInitError: any) {
							return cleanupAndReject(
								new Error(
									`FFmpeg initialization failed: ${ffmpegInitError.message}`
								),
								'ffmpeg_init'
							)
						}
						// --- End Create FFmpeg Command ---

						// Handle errors from the *input* stream (ytdl-core)
						audioStream.on('error', inputError => {
							// Logging is done in streamAudioWithYtdlCore, just reject here
							segmentLogger.error(
								// Still log contextually
								{
									error: inputError.message,
									code: (inputError as any).code
								},
								'ytdl-core input stream feeding ffmpeg emitted error event'
							)
							// Format a detailed error message based on status/code etc.
							let specificMsg = `Input stream error: ${inputError.message}`
							const statusCode = (inputError as any).statusCode
							if (
								statusCode === 403 ||
								statusCode === 401 ||
								statusCode === 410
							) {
								specificMsg = `Input stream error: YouTube access error during download (Status ${statusCode}). Check cookies (${persistentCookieFilePath}). Msg: ${inputError.message}`
							} else if (
								(inputError as any).code === 'ECONNRESET' ||
								(inputError as any).code === 'ETIMEDOUT' ||
								inputError.message.includes('socket hang up')
							) {
								specificMsg = `Input stream error: Network error during download (${(inputError as any).code}). Msg: ${inputError.message}`
							}
							cleanupAndReject(
								new Error(specificMsg),
								'ytdl_stream_error'
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
									`FFmpeg output stream error (pipe to GCS): ${outputError.message}`
								),
								'ffmpeg_output_stream_error'
							)
						})
						ffmpegOutputStream.on('end', () => {
							segmentLogger.info(
								'ffmpegOutputStream (pipe destination) saw end event.'
							)
						})
						ffmpegOutputStream.on('close', () => {
							segmentLogger.info(
								'ffmpegOutputStream (pipe destination) saw close event.'
							)
						})

						// Handle GCS upload success/failure
						uploadStreamToGCS(ffmpegOutputStream, destFileName)
							.then(() => {
								if (!promiseRejected) {
									gcsUploadSucceeded = true
									segmentLogger.info(
										`Segment successfully uploaded to ${gcsUri}`
									)
									resolve()
								} else {
									segmentLogger.warn(
										'GCS upload finished, but an error occurred earlier. Promise already rejected.'
									)
									gcsUploadSucceeded = false
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
									),
									'gcs_upload_error'
								)
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
					const transcriptGoogle = await transcribeWithGoogle(gcsUri) // Add timeout?
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
							await transcribeAudioElevenLabs(stream11) // Add timeout?
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
							// Only fail job if both sources failed
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
						transcriptElevenLabs || (transcriptGoogle ? '' : null) // Use Google if 11Labs failed/empty, else null/empty

					if (googleInput === '' && elevenLabsInput === null) {
						// Check if *both* are effectively empty
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
					) // Pass empty string if null, add timeout?
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
						{
							error: segmentErr.message,
							stack: segmentErr.stack?.substring(0, 500)
						},
						`Error processing segment ${segmentNumber} on attempt ${attempt}`
					)
					await pushTranscriptionEvent(
						jobId,
						`Xatolik (${segmentNumber}/${numSegments}, urinish ${attempt}): ${segmentErr.message.substring(0, 150)}...`,
						false,
						broadcast
					)

					// --- Check for Fatal Errors (using the formatted errors from stream/ffmpeg/other steps) ---
					// These errors will stop the *entire job*
					if (
						segmentErr.message?.includes('Input stream error:') || // Covers ytdl network/access errors during download
						segmentErr.message?.includes(
							'FFmpeg command failed:'
						) || // Covers ffmpeg execution errors
						segmentErr.message?.includes(
							'FFmpeg output stream error:'
						) || // Covers pipe errors
						segmentErr.message?.includes(
							'FFmpeg initialization failed'
						) || // Covers ffmpeg setup errors
						segmentErr.message?.includes('GCS upload failed') || // Covers storage errors
						segmentErr.message?.includes(
							'Both Google and ElevenLabs transcription failed'
						) || // Covers fatal transcription failure
						segmentErr.message?.includes(
							'Gemini editing returned empty'
						) || // Covers fatal editing failure
						segmentErr.message?.includes(
							'ytdl audio stream was not initialized'
						) // Covers internal error
					) {
						let userMsg = `Jarayon ${segmentNumber}/${numSegments}-chi bo'lakda tuzatib bo'lmas xatolik tufayli to'xtatildi: ${segmentErr.message}`
						// Add specific Uzbek translation based on error content
						if (
							segmentErr.message?.includes(
								'Input stream error: YouTube access error'
							)
						) {
							userMsg = `Xatolik: YouTube kirish xatosi (yuklash paytida ${segmentNumber}/${numSegments}). Cookie fayli (${persistentCookieFilePath}) yaroqsiz/video maxfiy? Jarayon to'xtatildi.`
						} else if (
							segmentErr.message?.includes(
								'Input stream error: Network error'
							)
						) {
							userMsg = `Xatolik: Tarmoq xatosi (YouTube'ga ulanish uzildi ${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`
						} else if (segmentErr.message?.includes('FFmpeg')) {
							userMsg = `Xatolik: Audio kodlashda muammo (FFmpeg ${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`
						} else if (segmentErr.message?.includes('GCS')) {
							userMsg = `Xatolik: Audio bo'lakni saqlashda muammo (GCS ${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`
						} // Keep others using the detailed technical message

						segmentLogger.error(
							`Fatal error occurred during segment ${segmentNumber} processing. Aborting job.`
						)
						await pushTranscriptionEvent(
							jobId,
							userMsg,
							true,
							broadcast
						)
						throw segmentErr // Re-throw to exit the main try block
					}
					// Non-fatal errors (e.g., temporary transcription hiccups if one source works) allow retry loop

					await delay(2000 + attempt * 1000) // Backoff before retry
				} finally {
					segmentLogger.debug('Entering segment finally block.')
					// Cleanup streams, especially if an error occurred mid-process or before upload
					if (
						!segmentProcessedSuccessfully &&
						attempt < maxAttempts
					) {
						// Only log if retrying
						segmentLogger.warn(
							`Segment ${segmentNumber} attempt ${attempt} failed. Cleaning up before retry.`
						)
					}
					// Ensure streams are destroyed, especially the input stream if ffmpeg failed
					try {
						if (
							ffmpegOutputStream &&
							!ffmpegOutputStream.destroyed
						) {
							ffmpegOutputStream.destroy()
						}
						if (audioStream && !audioStream.destroyed) {
							audioStream.destroy()
						}
					} catch (finalCleanupErr: any) {
						segmentLogger.warn(
							{ error: finalCleanupErr.message },
							'Error during final stream cleanup check.'
						)
					}

					// Delete GCS file *only* if upload succeeded on this attempt
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
					} else if (
						attempt === maxAttempts &&
						!segmentProcessedSuccessfully
					) {
						// If all attempts failed, try deleting the GCS file just in case it was uploaded on a failed attempt
						segmentLogger.warn(
							`Segment ${segmentNumber} failed all attempts. Attempting cleanup delete of GCS file ${destFileName} just in case.`
						)
						try {
							await deleteGCSFile(destFileName)
							segmentLogger.info(
								`Cleanup deletion of ${destFileName} succeeded (or file didn't exist).`
							)
						} catch (cleanupDeleteErr: any) {
							// Don't warn too loudly if cleanup fails
							segmentLogger.info(
								{
									error: cleanupDeleteErr.message,
									file: destFileName
								},
								`Cleanup deletion of ${destFileName} failed (may not exist).`
							)
						}
					}
					// No delay needed here unless debugging rate limits etc.
				}
			} // End retry loop

			if (!segmentProcessedSuccessfully) {
				// This means all retries failed for a segment
				jobLogger.error(
					`Segment ${segmentNumber}/${numSegments} failed after ${maxAttempts} attempts. Aborting job.`
				)
				await pushTranscriptionEvent(
					jobId,
					`Xatolik: ${segmentNumber}/${numSegments}-chi bo'lakni ${maxAttempts} urinishda ham ishlab bo'lmadi. Jarayon to'xtatildi.`,
					true,
					broadcast
				)
				// Throw a specific error indicating retry exhaustion
				throw new Error(
					`Failed to process segment ${segmentNumber} after ${maxAttempts} attempts.`
				)
			}

			i++ // Move to the next segment
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
			.replace(/(\n\s*){3,}/g, '\n\n')
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
		const finalTranscript = `<i style="display: block; font-style: italic; text-align: center; margin-bottom: 1rem; font-size: 0.9em; color: #555;">🕒 Transkripsiya uchun ${formatDuration(duration)} vaqt ketdi.</i>
<h1 style="font-weight: 700; font-size: 1.8rem; margin: 0.5rem 0 1.5rem; text-align: center; line-height: 1.2;">${finalTitle}</h1>
<p style="text-indent: 30px; line-height: 1.6;">${convertToUzbekLatin(combinedResult)}</p>`

		await transcriptService.saveFinalTranscript(jobId, finalTranscript)
		jobLogger.info(`Final transcript saved.`)
		await pushTranscriptionEvent(jobId, finalTranscript, true, broadcast) // Mark success
		jobStatusUpdated = true
	} catch (err: any) {
		// Main catch block for errors from getInfo or fatal errors from segment loop
		jobLogger.error(
			{ error: err.message, stack: err.stack?.substring(0, 700) }, // Log full processed error
			'Critical error caught in runTranscriptionJob main try-catch block'
		)

		if (!jobStatusUpdated) {
			// Ensure status is marked as error only once
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

		// --- Final Error Reporting to User (using detailed error message) ---
		if (broadcast) {
			try {
				// Default message if specific translation isn't available
				let clientErrorMessage = `Serverda kutilmagan xatolik yuz berdi. Jarayon to'xtatildi. (${err.message || 'No details'})`

				// Use specific translations based on the error message content
				if (err.message?.includes('Could not extract functions')) {
					clientErrorMessage = `Xatolik: YouTube sahifasini o'qishda muammo ('Could not extract functions'). YouTube sayt tuzilishini o'zgartirgan bo'lishi mumkin. **Iltimos, serverdagi 'ytdl-core' kutubxonasini yangilang.** Video URL to'g'riligini va unga kirish mumkinligini tekshiring.`
				} else if (
					err.message?.includes('No downloadable formats found')
				) {
					clientErrorMessage = `Xatolik: Video uchun yuklab olinadigan formatlar topilmadi. Video hudud bo'yicha cheklangan, maxfiy, hali boshlanmagan jonli efir/premyera bo'lishi yoki cookie fayllar yetarli bo'lmasligi mumkin (${persistentCookieFilePath}).`
				} else if (err.message?.includes('YouTube access error')) {
					clientErrorMessage = `Xatolik: YouTube kirish xatosi (info/yuklash). Video maxfiy/mavjud emas/yosh cheklovi/eskirgan cookie (${persistentCookieFilePath})? Jarayon to'xtatildi. (${err.message})`
				} else if (err.message?.includes('Invalid YouTube URL?')) {
					clientErrorMessage = `Xatolik: Noto'g'ri YouTube URL kiritildi. (${err.message})`
				} else if (err.message?.includes('Network error')) {
					clientErrorMessage = `Xatolik: Tarmoq xatosi (YouTube'ga ulanish/javob olishda muammo). Internet ulanishingizni yoki YouTube statusini tekshiring. (${err.message})`
				} else if (err.message?.includes('FFmpeg')) {
					clientErrorMessage = `Xatolik: Audio faylni kodlashda muammo (FFmpeg). Jarayon to'xtatildi. (${err.message})`
				} else if (err.message?.includes('GCS')) {
					clientErrorMessage = `Xatolik: Audio bo'lakni bulutga saqlashda muammo (GCS). Jarayon to'xtatildi. (${err.message})`
				} else if (
					err.message?.includes('transcription failed') ||
					err.message?.includes('Gemini editing')
				) {
					clientErrorMessage = `Xatolik: Matnni o'girishda/tahrirda tuzatib bo'lmas xatolik. Jarayon to'xtatildi. (${err.message})`
				} else if (err.message?.includes('Failed to process segment')) {
					// Retry exhaustion
					clientErrorMessage = `Xatolik: ${err.message}` // Pass the specific segment failure message
				} else if (
					err.message?.includes('GOOGLE_CLOUD_BUCKET_NAME') ||
					err.message?.includes('Bucket topilmadi')
				) {
					clientErrorMessage = `Server konfiguratsiya xatosi: Bucket topilmadi.`
				}
				// Other unexpected errors will use the default message with the technical error details

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
