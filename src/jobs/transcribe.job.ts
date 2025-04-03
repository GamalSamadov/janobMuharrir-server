import ytdl from '@distube/ytdl-core'
import ffmpeg from 'fluent-ffmpeg'
import path from 'path'
import { performance } from 'perf_hooks'
import { PassThrough, Readable, Writable } from 'stream'
import * as toughCookie from 'tough-cookie'

// Import the whole module

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

// --- REVISED Helper to get request options with cookies ---
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
			// Use tough-cookie's CookieJar for proper parsing and handling
			const jar = new toughCookie.CookieJar()
			const lines = cookieFileContent.split('\n')
			let attemptedLoads = 0
			let successfullySetCookies = 0
			const youtubeUrl = 'https://www.youtube.com/' // URL for context and getting cookies
			const now = Date.now()

			for (const line of lines) {
				const trimmedLine = line.trim()
				// Skip comments and empty lines
				if (!trimmedLine || trimmedLine.startsWith('#')) {
					continue
				}
				attemptedLoads++

				// --- Parse Netscape cookie format line ---
				// Format: domain<TAB>includeSubdomains<TAB>path<TAB>secure<TAB>expires<TAB>name<TAB>value
				const parts = trimmedLine.split('\t')
				if (parts.length !== 7) {
					logger.warn(
						{ line: trimmedLine.substring(0, 100) }, // Log truncated line
						`${logPrefix}: Skipping cookie line due to incorrect number of tab-separated parts (${parts.length}). Expected 7.`
					)
					continue
				}

				const [domain, , path, secureStr, expiresStr, name, value] =
					parts

				// Basic validation
				if (!domain || !path || !name) {
					logger.warn(
						{ line: trimmedLine.substring(0, 100) },
						`${logPrefix}: Skipping cookie line due to missing essential parts (domain, path, name).`
					)
					continue
				}

				try {
					const expiresTimestamp = parseInt(expiresStr, 10)
					let expiresDate: Date | undefined | 'Infinity' = undefined // tough-cookie uses 'Infinity' or Date

					if (!isNaN(expiresTimestamp) && expiresTimestamp > 0) {
						// Check if expired
						if (expiresTimestamp * 1000 <= now) {
							// logger.debug(`${logPrefix}: Skipping expired cookie: ${name} for ${domain}`);
							continue // Skip expired cookie
						}
						expiresDate = new Date(expiresTimestamp * 1000)
					} else if (expiresStr === '0') {
						expiresDate = undefined // Session cookie
					} else {
						// Attempt to handle potential non-standard large numbers or treat as session
						// If it's a very large number, treat it as non-expiring ('Infinity')
						// Otherwise, log a warning and treat as session? For now, treat non-zero, non-timestamp as session.
						if (
							!isNaN(expiresTimestamp) &&
							expiresTimestamp !== 0
						) {
							// Potentially a very large number representing no expiry
							// tough-cookie might handle this better internally, but let's be safe
							// We'll let setCookie handle it, maybe logging if it fails below.
							expiresDate = new Date(expiresTimestamp * 1000) // Try creating date anyway
							if (isNaN(expiresDate.getTime())) {
								logger.warn(
									{
										line: trimmedLine.substring(0, 100),
										expires: expiresStr
									},
									`${logPrefix}: Could not parse expiry timestamp ${expiresStr} into valid Date, treating as session cookie.`
								)
								expiresDate = undefined
							}
						} else {
							logger.warn(
								{
									line: trimmedLine.substring(0, 100),
									expires: expiresStr
								},
								`${logPrefix}: Non-numeric, non-zero expiry value '${expiresStr}', treating as session cookie.`
							)
							expiresDate = undefined // Treat as session cookie
						}
					}

					const isSecure = secureStr.toUpperCase() === 'TRUE'
					// HttpOnly is not standard in Netscape format - assume false
					const isHttpOnly = false // YouTube often uses HttpOnly, but we can't know from standard export

					// Create the Cookie object using tough-cookie's factory/constructor
					// Need to handle domain format (leading dot means include subdomains)
					const cookieDomain = domain.startsWith('.')
						? domain.substring(1)
						: domain
					const hostOnly = !domain.startsWith('.')

					const cookie = toughCookie.Cookie.fromJSON({
						key: name,
						value: value,
						domain: cookieDomain,
						path: path,
						expires: expiresDate, // Use ISO string format if date exists
						secure: isSecure,
						httpOnly: isHttpOnly,
						hostOnly: hostOnly,
						creation: new Date().toISOString() // Optional: add creation time
					})

					if (!cookie) {
						logger.warn(
							{ line: trimmedLine.substring(0, 100) },
							`${logPrefix}: Failed to create tough-cookie object from parsed line.`
						)
						continue
					}

					// Use setCookie (async) with the parsed object
					// Provide the URL the cookie should be associated with (helps with domain/path matching)
					// Use the domain from the cookie itself as the context URL for setting
					// Construct a plausible URL for context
					const sourceUrl = `${isSecure ? 'https://' : 'http://'}${domain.replace(/^\./, '')}${path}`
					await jar.setCookie(cookie, sourceUrl)
					successfullySetCookies++
					// logger.debug(`${logPrefix}: Successfully parsed and set cookie: ${name}`);
				} catch (cookieParseSetError: any) {
					logger.warn(
						{
							error: cookieParseSetError.message,
							line: trimmedLine.substring(0, 100)
						},
						`${logPrefix}: Error processing/setting cookie from line, skipping.`
					)
				}
			} // End loop through lines

			if (successfullySetCookies > 0) {
				logger.info(
					`${logPrefix}: Successfully parsed and stored ${successfullySetCookies} potential cookies (out of ${attemptedLoads} lines attempted).`
				)

				// Now, get the cookie string specifically for the target YouTube URL
				const cookieString = await jar.getCookieString(youtubeUrl)

				if (cookieString) {
					logger.info(
						`${logPrefix}: Adding 'Cookie' header with ${cookieString.split(';').length} cookies for ${youtubeUrl}.`
					)
					requestOptions = {
						headers: {
							Cookie: cookieString
							// Consider adding a realistic User-Agent if issues persist
							// 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
						}
					}
				} else {
					// This warning now means cookies were parsed OK, but none matched the youtubeUrl requested
					logger.warn(
						`${logPrefix}: Parsed ${successfullySetCookies} cookies, but none were applicable for the URL '${youtubeUrl}' (check domain/path matching and expiration in the source file).`
					)
					// Optional: Log jar contents for debugging
					// logger.debug(`${logPrefix}: Cookie Jar state: ${JSON.stringify(jar.toJSON())}`);
				}
			} else if (attemptedLoads > 0) {
				logger.warn(
					`${logPrefix}: Cookie file was not empty (${attemptedLoads} lines attempted), but failed to parse or store any valid, non-expired cookies.`
				)
			} else {
				logger.info(
					`${logPrefix}: Cookie file contained no valid cookie lines to process.`
				)
			}
		} else {
			logger.info(
				// Info level, as an empty file is not an error itself
				`${logPrefix}: Persistent cookie file exists but is empty: ${persistentCookieFilePath}. Proceeding without cookies.`
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
// --- End REVISED Helper ---

async function getVideoInfoWithYtdlCore(
	youtubeUrl: string
): Promise<VideoInfo> {
	const logPrefix = 'ytdl-core-info'
	logger.info(`${logPrefix}: Fetching info for ${youtubeUrl}.`)

	// Use the revised helper to get options
	const cookieRequestOptions = await getRequestOptionsWithCookies(logPrefix)
	const options: ytdl.getInfoOptions = {
		requestOptions: cookieRequestOptions
	}

	if (options.requestOptions?.headers) {
		// Check if Cookie header was actually set
		logger.info(
			`${logPrefix}: Using request options with cookies for getInfo.`
		)
	} else {
		logger.info(
			`${logPrefix}: No valid/applicable cookies found or loaded, using default request options for getInfo.`
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
				`${logPrefix}: Video info fetched but no formats found. Possible region lock, premiere, member-only, or parsing issue?`
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
		const statusCode = error?.statusCode // Capture status code if available
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
			specificError = `YouTube info parsing failed (ytdl-core 'No downloadable formats found'): The video might be region-locked, private, member-only, an unstarted live stream/premiere, or require stronger authentication (check cookies). (Msg: ${errorMessage})`
		} else if (
			errorMessage.includes('private video') ||
			errorMessage.includes('Login required') ||
			errorMessage.includes('members-only') ||
			errorMessage.includes('confirm your age') ||
			errorMessage.includes('unavailable') || // Covers general unavailability
			statusCode === 403 || // Forbidden (often cookies/login/region/member)
			statusCode === 401 || // Unauthorized
			statusCode === 410 || // Gone (often premieres before start or deleted videos)
			errorMessage.includes('age-restricted') ||
			errorMessage.includes('Terms of Service')
		) {
			specificError = `YouTube access error (ytdl-core info): Video might be private/unavailable/premiere/member-only, require login/age confirmation/TOS agreement, or cookie file (${persistentCookieFilePath}) is invalid/expired/rejected/insufficient. (Status: ${statusCode || 'N/A'}, Msg: ${errorMessage})`
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

	// Use the revised helper to get options
	const cookieRequestOptions = await getRequestOptionsWithCookies(logPrefix)
	const options: ytdl.downloadOptions = {
		filter: 'audioonly',
		quality: 'highestaudio',
		requestOptions: cookieRequestOptions
	}

	if (options.requestOptions?.headers) {
		// Check if Cookie header was actually set
		logger.info(
			`${logPrefix}: Using request options with cookies for download.`
		)
	} else {
		logger.info(
			`${logPrefix}: No valid/applicable cookies found or loaded, using default request options for download.`
		)
	}

	logger.info(`${logPrefix}: Calling ytdl()...`)
	const stream = ytdl(youtubeUrl, options)

	stream.on('error', (err: any) => {
		const statusCode = err?.statusCode
		// Provide more context for common stream errors
		let specificMsg = `ytdl stream error: ${err.message}`
		if (statusCode === 403 || statusCode === 401 || statusCode === 410) {
			specificMsg = `ytdl stream error: YouTube access denied during download (Status ${statusCode}). Check cookies (${persistentCookieFilePath}) or video status/restrictions (private/member/region?). Msg: ${err.message}`
		} else if (
			err.code === 'ECONNRESET' ||
			err.code === 'ETIMEDOUT' ||
			err.message.includes('socket hang up')
		) {
			specificMsg = `ytdl stream error: Network error during download (${err.code}). Connection lost? Msg: ${err.message}`
		} else if (err.message?.includes('formats')) {
			// This might indicate an issue found during stream setup if getInfo passed somehow
			specificMsg = `ytdl stream setup error: Problem finding suitable audio format. Check video status/restrictions. Msg: ${err.message}`
		} else if (err.message?.includes('Could not extract functions')) {
			// This error can sometimes happen at the download stage too
			specificMsg = `ytdl stream setup error: YouTube page parsing failed (ytdl-core 'Could not extract functions') during download setup. Update 'ytdl-core'? Msg: ${err.message}`
		}

		logger.error(
			{ error: specificMsg, code: err.code, statusCode }, // Log the enhanced message
			`${logPrefix}: Error event emitted on ytdl stream during download.`
		)
		// The ffmpeg promise rejection handler will catch this and abort the segment
	})

	stream.on('progress', (chunkLength, downloaded, total) => {
		/* Optional logging */
		// logger.debug(`${logPrefix}: Progress - Chunk: ${chunkLength}, Downloaded: ${downloaded}, Total: ${total}`);
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
			// Consider destroying the stream proactively, although ytdl-core usually emits 'error'
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
			`Persistent cookie file found at ${persistentCookieFilePath} (${stats.size > 0 ? 'not empty' : 'empty'}). ${stats.size > 0 ? 'Cookies will be attempted to load if valid.' : 'No cookies will be loaded.'}`
		)
	} catch (err: any) {
		if (err.code === 'ENOENT') {
			jobLogger.warn(
				`Persistent cookie file not found at ${persistentCookieFilePath}. Transcription may fail for private/restricted/member videos.`
			)
		} else {
			jobLogger.error(
				{ error: err.message, file: persistentCookieFilePath },
				`Error accessing persistent cookie file. Transcription may fail for private/restricted/member videos.`
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

			// Use the more specific error messages generated by getVideoInfoWithYtdlCore
			if (err.message?.includes('Could not extract functions')) {
				userErrorMessage = `Xatolik: YouTube sahifasini o'qishda muammo ('Could not extract functions'). YouTube sayt tuzilishini o'zgartirgan bo'lishi mumkin. **Iltimos, serverdagi 'ytdl-core' kutubxonasini yangilang.** Video URL to'g'riligini va unga kirish mumkinligini tekshiring.`
			} else if (err.message?.includes('No downloadable formats found')) {
				userErrorMessage = `Xatolik: Video uchun yuklab olinadigan formatlar topilmadi. Video hudud bo'yicha cheklangan, maxfiy, a'zolar uchun, hali boshlanmagan jonli efir/premyera bo'lishi yoki cookie fayllar yetarli bo'lmasligi mumkin (${persistentCookieFilePath}).`
			} else if (err.message?.includes('YouTube access error')) {
				// Includes private, login, age, member, unavailable, 403, 401 etc.
				userErrorMessage = `Xatolik: YouTube video ma'lumotiga kirishda muammo (maxfiy/a'zolar uchun/mavjud emas/yosh cheklovi/login/cookie?). Cookie faylini tekshiring (${persistentCookieFilePath}). (${err.message})`
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

		// --- Segment Processing Loop ---
		// Relies on improved error handling in streamAudioWithYtdlCore and promise rejection.
		// The final catch block below will handle fatal errors propagated from the loop.

		await pushTranscriptionEvent(
			jobId,
			'Ovoz yuklanmoqda (ytdl-core)...',
			false,
			broadcast
		)
		await delay(500)
		const segmentDuration = 150 // 2.5 minutes
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
			// Ensure duration is slightly positive for ffmpeg
			const safeActualDuration = Math.max(0.1, actualDuration)
			const segmentEndTime = segmentStartTime + safeActualDuration

			const destFileName = `segment_${jobId}_${segmentNumber}.mp3`
			const gcsUri = `gs://${bucketName}/${destFileName}`

			let segmentProcessedSuccessfully = false
			let attempt = 0
			const maxAttempts = 2 // Max attempts per segment

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
					await delay(1500 * attempt) // Increased backoff
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
					// streamAudioWithYtdlCore includes cookie handling and error logging
					audioStream = await streamAudioWithYtdlCore(url)

					segmentLogger.info(
						`Starting FFmpeg encoding and slicing...`
					)

					// --- Wrap ffmpeg processing and upload in a Promise ---
					await new Promise<void>((resolve, reject) => {
						if (!audioStream) {
							// Safety check, should not happen if streamAudio didn't throw
							return reject(
								new Error(
									'ytdl audio stream was not initialized before ffmpeg.'
								)
							)
						}
						let promiseRejected = false // Flag to prevent multiple rejections

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
							// Try to clean up resources
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
								// ffmpegCommand.kill() can sometimes throw if process already exited
								if (ffmpegCommand) {
									try {
										segmentLogger.warn(
											`Killing ffmpeg process due to error: ${err.message}`
										)
										ffmpegCommand.kill('SIGKILL') // Force kill
									} catch (killError: any) {
										segmentLogger.warn(
											{ error: killError.message },
											'Ignoring error during ffmpeg kill (process might have already exited).'
										)
									}
								}
								if (audioStream && !audioStream.destroyed) {
									segmentLogger.warn(
										'Destroying ytdl stream due to error...'
									)
									audioStream.destroy(err) // Destroy the input stream
								}
							} catch (cleanupErr: any) {
								segmentLogger.warn(
									{ error: cleanupErr.message },
									'Error during cleanup after main error'
								)
							}
							reject(err) // Reject the main promise
						}

						// --- Create FFmpeg Command ---
						try {
							ffmpegCommand = ffmpeg(audioStream)
								// Input options MUST come before .input() or be attached to it if using file path
								// For streams, input options are often placed before format/codec
								.inputOption(
									`-ss ${segmentStartTime.toFixed(3)}`
								) // Use more precision for start time
								// .inputOption(`-to ${segmentEndTime.toFixed(3)}`) // -to can be less reliable with streams than -t/-fs
								.inputOption(
									`-t ${safeActualDuration.toFixed(3)}`
								) // Use duration instead of -to for streams
								.noVideo() // Explicitly disable video processing
								.format('mp3')
								.audioCodec('libmp3lame')
								.audioBitrate('96k')
								.outputOptions('-map_metadata -1') // Strip metadata
								.on('start', cmd =>
									segmentLogger.info(`FFmpeg started: ${cmd}`)
								)
								.on('codecData', data => {
									// segmentLogger.info( `FFmpeg codec data: ${JSON.stringify(data)}` );
								})
								.on('stderr', stderrLine => {
									// Log potentially useful ffmpeg messages
									if (
										stderrLine.includes('error') ||
										stderrLine.includes('failed') ||
										stderrLine.includes('panic')
									) {
										segmentLogger.warn(
											`FFmpeg stderr: ${stderrLine}`
										)
									} else {
										segmentLogger.debug(
											`FFmpeg stderr: ${stderrLine}`
										)
									}
								})
								.on('error', (err, stdout, stderr) => {
									// This catches errors from the ffmpeg process itself
									segmentLogger.error(
										{
											message: err.message,
											// stdout, // Usually empty for audio streams
											stderr // Often contains the crucial error detail
										},
										`FFmpeg command error event`
									)
									let ffmpegErrMsg = `FFmpeg command failed: ${err.message}`
									// Check stderr for clues about the input stream failing
									if (
										stderr?.includes(
											'Input/output error'
										) ||
										stderr?.includes('Server returned 4') || // e.g., 403 Forbidden from source
										stderr?.includes(
											'pipe:0: End of file'
										) ||
										err.message.includes('Pipe closed') ||
										err.message.includes(
											'Exited with code'
										) || // General failure
										err.message.includes(
											'Input stream error'
										)
									) {
										ffmpegErrMsg +=
											' (Often caused by input ytdl stream ending prematurely or access issues - check previous logs for ytdl errors/status codes)'
									}
									cleanupAndReject(
										new Error(ffmpegErrMsg),
										'ffmpeg_error_event'
									)
								})
								.on('end', () => {
									// IMPORTANT: This 'end' event means ffmpeg *finished processing its input*.
									// It does *not* guarantee the piped output (GCS upload) has finished.
									// GCS upload success is handled by the uploadStreamToGCS promise.
									if (!promiseRejected) {
										segmentLogger.info(
											'FFmpeg processing finished (end event received). Waiting for GCS upload promise...'
										)
										// Do NOT resolve the promise here. Wait for the upload.
									} else {
										segmentLogger.warn(
											'FFmpeg end event received, but an error occurred earlier. Upload might be compromised.'
										)
									}
								})

							// Pipe the output of ffmpeg directly
							ffmpegOutputStream = ffmpegCommand.pipe() // Returns a PassThrough stream

							if (!ffmpegOutputStream) {
								// This should realistically not happen with .pipe()
								throw new Error(
									'ffmpegCommand.pipe() returned null or undefined.'
								)
							}
						} catch (ffmpegInitError: any) {
							// Catch errors during ffmpeg setup itself
							return cleanupAndReject(
								new Error(
									`FFmpeg command initialization failed: ${ffmpegInitError.message}`
								),
								'ffmpeg_init'
							)
						}
						// --- End Create FFmpeg Command ---

						// --- Handle Errors on the Streams ---

						// 1. Errors from the *INPUT* stream (ytdl-core) feeding FFmpeg
						audioStream.on('error', inputError => {
							// Logging is already done in streamAudioWithYtdlCore's 'error' handler
							// We just need to reject the promise here.
							let specificMsg = `Input stream error during FFmpeg pipe: ${inputError.message}`
							const statusCode = (inputError as any).statusCode
							const errCode = (inputError as any).code
							// Add context based on common errors caught by streamAudioWithYtdlCore
							if (
								statusCode === 403 ||
								statusCode === 401 ||
								statusCode === 410
							) {
								specificMsg = `Input stream error (ytdl): YouTube access denied (Status ${statusCode}). Check cookies/video status. Msg: ${inputError.message}`
							} else if (
								errCode === 'ECONNRESET' ||
								errCode === 'ETIMEDOUT' ||
								inputError.message.includes('socket hang up')
							) {
								specificMsg = `Input stream error (ytdl): Network error (${errCode}). Connection lost? Msg: ${inputError.message}`
							} else if (
								inputError.message?.includes(
									'Could not extract functions'
								)
							) {
								specificMsg = `Input stream error (ytdl): YouTube page parsing failed ('Could not extract functions'). Update 'ytdl-core'? Msg: ${inputError.message}`
							}

							cleanupAndReject(
								new Error(specificMsg), // Use the formatted message
								'ytdl_stream_error_pipe' // Context: error during pipe
							)
						})
						audioStream.on('end', () => {
							segmentLogger.info('ytdl input stream ended.') // Good sign if ffmpeg hasn't errored yet
						})
						audioStream.on('close', () => {
							segmentLogger.info('ytdl input stream closed.')
						})

						// 2. Errors from the *OUTPUT* stream (ffmpeg's output being piped to GCS)
						ffmpegOutputStream.on('error', outputPipeError => {
							// This could be an error during the piping process itself,
							// or potentially an error surfaced from ffmpeg not caught by its 'error' event.
							segmentLogger.error(
								{ error: outputPipeError.message },
								'Error on ffmpeg output stream during GCS pipe.'
							)
							cleanupAndReject(
								new Error(
									`FFmpeg output pipe error (to GCS): ${outputPipeError.message}`
								),
								'ffmpeg_output_stream_error'
							)
						})
						ffmpegOutputStream.on('finish', () => {
							// This means the *destination* (GCS upload stream) has finished writing.
							// This is a good indicator, but success is still determined by the upload promise.
							segmentLogger.info(
								'ffmpegOutputStream (pipe destination/GCS) saw finish event.'
							)
						})
						ffmpegOutputStream.on('close', () => {
							segmentLogger.info(
								'ffmpegOutputStream (pipe destination/GCS) saw close event.'
							)
						})

						// --- Handle GCS Upload Completion ---
						uploadStreamToGCS(ffmpegOutputStream, destFileName)
							.then(() => {
								// Upload promise resolved successfully
								if (!promiseRejected) {
									gcsUploadSucceeded = true
									segmentLogger.info(
										`Segment successfully uploaded to ${gcsUri}`
									)
									resolve() // Resolve the main promise HERE
								} else {
									// This can happen if an error occurred (e.g., input stream died)
									// but the upload technically 'finished' writing whatever data it got.
									// The promise was already rejected, so do nothing here.
									segmentLogger.warn(
										'GCS upload finished, but an error occurred earlier and the promise was already rejected. File might be incomplete/corrupt.'
									)
									gcsUploadSucceeded = false // Mark as failed due to earlier error
								}
							})
							.catch(uploadErr => {
								// Upload promise was rejected
								segmentLogger.error(
									{ error: uploadErr.message },
									'GCS upload failed.'
								)
								cleanupAndReject(
									// Reject the main promise
									new Error(
										`GCS upload failed: ${uploadErr.message}`
									),
									'gcs_upload_error'
								)
							})
					}) // --- End ffmpeg/upload Promise ---

					segmentLogger.info(
						'FFmpeg/Upload promise resolved successfully.'
					)

					// --- Transcriptions & Editing ---
					// Only proceed if the promise above resolved (meaning ffmpeg ran and upload succeeded)

					segmentLogger.info('Starting Google transcription...')
					await pushTranscriptionEvent(
						jobId,
						`Google matnni o'girmoqda ${segmentNumber}/${numSegments}...`,
						false,
						broadcast
					)
					const transcriptGoogle = await transcribeWithGoogle(gcsUri) // Add timeout? Maybe inside helper
					if (transcriptGoogle) {
						segmentLogger.info(
							`Google transcription done (length: ${transcriptGoogle.length}).`
						)
					} else {
						segmentLogger.warn(
							`Google transcription returned empty/null for ${gcsUri}.`
						)
						// Don't push error yet, wait to see if ElevenLabs works
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
						const stream11 = await getGCSFileStream(gcsUri) // Get a fresh stream
						transcriptElevenLabs =
							await transcribeAudioElevenLabs(stream11) // Add timeout? Maybe inside helper
						if (transcriptElevenLabs) {
							segmentLogger.info(
								`ElevenLabs transcription done (length: ${transcriptElevenLabs.length}).`
							)
						} else {
							segmentLogger.warn(
								`ElevenLabs transcription returned empty/null for ${gcsUri}`
							)
							if (!transcriptGoogle) {
								// If Google also failed/empty
								await pushTranscriptionEvent(
									jobId,
									`${segmentNumber}/${numSegments}-chi bo'lak: Google va ElevenLabs matnlari bo'sh. Xatolik.`,
									false,
									broadcast
								)
							} else {
								await pushTranscriptionEvent(
									jobId,
									`${segmentNumber}/${numSegments}-chi bo'lak: ElevenLabs matni bo'sh. Google natijasi ishlatiladi.`,
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
							// FATAL for this segment if both sources failed
							throw new Error(
								`ElevenLabs failed (${elevenLabsError.message}) and Google text is also empty/null.`
							)
						} else {
							// Non-fatal if Google text exists
							await pushTranscriptionEvent(
								jobId,
								`${segmentNumber}/${numSegments}-chi ElevenLabs matnida xatolik (${elevenLabsError.message}). Google natijasi bilan davom etilmoqda...`,
								false,
								broadcast
							)
						}
					}

					const googleInput = transcriptGoogle || ''
					// Use Google text if 11Labs failed or returned null/empty, ONLY IF Google text exists.
					// If Google text is also empty, keep elevenLabsInput null.
					const elevenLabsInput =
						transcriptElevenLabs ?? (googleInput ? '' : null)

					// Check if *both* are effectively empty or failed
					if (googleInput === '' && elevenLabsInput === null) {
						segmentLogger.error(
							`Both Google and ElevenLabs transcription failed or returned empty for ${gcsUri}`
						)
						throw new Error( // This will trigger retry or job failure
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
					// Provide Google text always. Provide 11Labs text if it exists (is not null).
					const finalText = await editTranscribed(
						googleInput,
						elevenLabsInput // Pass null if 11Labs failed/empty and Google was also empty
					) // Add timeout? Maybe inside helper
					if (!finalText) {
						segmentLogger.error(
							`Gemini editing returned empty/null for ${gcsUri}. Input G: ${googleInput.length}, 11L: ${elevenLabsInput?.length ?? 'null'}`
						)
						throw new Error(
							'Gemini editing returned empty or failed.'
						) // Trigger retry/fail
					} else {
						segmentLogger.info(
							`Gemini editing done (length: ${finalText.length}).`
						)
					}

					editedTexts.push(finalText)
					segmentProcessedSuccessfully = true // Mark success for this attempt
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi bo'lak tayyor!`,
						false,
						broadcast
					)
					segmentLogger.info(
						`Segment ${segmentNumber} processed successfully on attempt ${attempt}.`
					)
				} catch (segmentErr: any) {
					// Catch errors from the ffmpeg/upload promise or transcription/editing steps
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

					// --- Check for Fatal Errors that should stop the *entire job* ---
					// Use the detailed, formatted error messages from various stages.
					// These errors indicate problems unlikely to be fixed by retrying the segment.
					const fatalErrorMessages = [
						'Input stream error (ytdl): YouTube access denied', // Cookie/permission issues
						'Input stream error (ytdl): YouTube page parsing failed', // ytdl-core needs update
						'FFmpeg command initialization failed', // ffmpeg setup problem
						'GCS upload failed', // Storage issue
						'GOOGLE_CLOUD_BUCKET_NAME', // Config error found later
						'Bucket topilmadi', // Config error
						// Don't make transcription/Gemini failures fatal immediately, allow retry
						// 'Both Google and ElevenLabs transcription failed',
						// 'Gemini editing returned empty',
						// Consider making persistent network errors fatal?
						'Input stream error (ytdl): Network error' // Maybe fatal if persistent
						// FFmpeg errors related to input stream often reflect the ytdl errors above
						// 'Often caused by input ytdl stream ending prematurely'
					]

					const isFatal = fatalErrorMessages.some(msg =>
						segmentErr.message?.includes(msg)
					)

					if (isFatal) {
						let userMsg = `Jarayon ${segmentNumber}/${numSegments}-chi bo'lakda tuzatib bo'lmas xatolik tufayli to'xtatildi: ${segmentErr.message}`
						// Add specific Uzbek translation based on error content
						if (
							segmentErr.message?.includes(
								'Input stream error (ytdl): YouTube access denied'
							)
						) {
							userMsg = `Xatolik: YouTube kirish xatosi (yuklash paytida ${segmentNumber}/${numSegments}). Cookie fayli (${persistentCookieFilePath}) yaroqsiz/video maxfiy/a'zolar uchun? Jarayon to'xtatildi.`
						} else if (
							segmentErr.message?.includes(
								'Input stream error (ytdl): YouTube page parsing failed'
							)
						) {
							userMsg = `Xatolik: YouTube sahifasini o'qishda muammo (ytdl-core ${segmentNumber}/${numSegments}). Kutubxonani yangilang? Jarayon to'xtatildi.`
						} else if (
							segmentErr.message?.includes(
								'Input stream error (ytdl): Network error'
							)
						) {
							userMsg = `Xatolik: Tarmoq xatosi (YouTube'ga ulanish uzildi ${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`
						} else if (segmentErr.message?.includes('FFmpeg')) {
							// Use generic if cause isn't input stream
							if (!segmentErr.message?.includes('ytdl stream')) {
								userMsg = `Xatolik: Audio kodlashda muammo (FFmpeg ${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`
							} // else: handled by input stream messages
						} else if (segmentErr.message?.includes('GCS')) {
							userMsg = `Xatolik: Audio bo'lakni saqlashda muammo (GCS ${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`
						} // Other fatal errors use the detailed technical message

						segmentLogger.error(
							`Fatal error occurred during segment ${segmentNumber} processing. Aborting job.`
						)
						await pushTranscriptionEvent(
							jobId,
							userMsg,
							true, // Mark job as completed (with error)
							broadcast
						)
						throw segmentErr // Re-throw the fatal error to exit the main try block
					}

					// If the error wasn't deemed fatal, the retry loop will continue (if attempts remain)
					// Add delay before retry
					await delay(2000 + attempt * 1500) // Exponential backoff
				} finally {
					segmentLogger.debug('Entering segment finally block.')
					// --- Cleanup for the current attempt ---
					// Ensure streams are destroyed to prevent leaks, especially if an error occurred mid-process
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
						if (ffmpegCommand) {
							// Attempt to kill ffmpeg process if it's lingering after error/completion
							// This might fail if already exited, so wrap in try-catch
							try {
								ffmpegCommand.kill('SIGKILL')
							} catch (e) {
								/* ignore */
							}
						}
					} catch (finalCleanupErr: any) {
						segmentLogger.warn(
							{ error: finalCleanupErr.message },
							'Error during final stream cleanup check in finally block.'
						)
					}

					// --- GCS File Cleanup ---
					// Delete GCS file ONLY if the upload succeeded in this attempt,
					// regardless of whether subsequent transcription/editing failed (to allow retry)
					if (gcsUploadSucceeded) {
						try {
							segmentLogger.info(
								`Attempting to delete GCS file post-processing: ${destFileName}`
							)
							await deleteGCSFile(destFileName)
							segmentLogger.info(
								`Successfully deleted GCS file: ${destFileName}`
							)
						} catch (deleteErr: any) {
							// Log as warning, don't fail the job for failed cleanup
							segmentLogger.warn(
								{
									error: deleteErr.message,
									file: destFileName
								},
								`Failed to delete GCS segment file after processing: ${destFileName}. Continuing job.`
							)
						}
					} else if (
						!segmentProcessedSuccessfully &&
						attempt >= maxAttempts
					) {
						// If all attempts failed for this segment, try a final cleanup delete
						// in case a file was partially uploaded on the last failed attempt.
						segmentLogger.warn(
							`Segment ${segmentNumber} failed all attempts. Attempting cleanup delete of GCS file ${destFileName} just in case.`
						)
						try {
							await deleteGCSFile(destFileName)
							segmentLogger.info(
								`Cleanup deletion of ${destFileName} succeeded (or file didn't exist).`
							)
						} catch (cleanupDeleteErr: any) {
							// Don't warn too loudly if cleanup fails, file might not exist
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
				} // End finally block for segment attempt
			} // --- End retry loop for segment ---

			// After the retry loop, check if the segment was processed
			if (!segmentProcessedSuccessfully) {
				// This means all retries failed for this segment
				jobLogger.error(
					`Segment ${segmentNumber}/${numSegments} failed after ${maxAttempts} attempts. Aborting job.`
				)
				// Push final error message to user
				await pushTranscriptionEvent(
					jobId,
					`Xatolik: ${segmentNumber}/${numSegments}-chi bo'lakni ${maxAttempts} urinishda ham ishlab bo'lmadi. Oxirgi xato loglarda bo'lishi kerak. Jarayon to'xtatildi.`,
					true, // Mark job completed (with error)
					broadcast
				)
				// Throw a specific error indicating retry exhaustion
				throw new Error(
					`Failed to process segment ${segmentNumber} after ${maxAttempts} attempts.`
				)
			}

			// If successful, move to the next segment
			i++
		} // --- End segment loop (while i < numSegments) ---

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

		// Combine results, remove excessive newlines, trim whitespace
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

		const finalTitle = videoInfo.title || "Noma'lum Sarlavha"
		// Format the final HTML output
		const finalTranscript = `<i style="display: block; font-style: italic; text-align: center; margin-bottom: 1rem; font-size: 0.9em; color: #555;">🕒 Transkripsiya uchun ${formatDuration(duration)} vaqt ketdi.</i>
<h1 style="font-weight: 700; font-size: 1.8rem; margin: 0.5rem 0 1.5rem; text-align: center; line-height: 1.2;">${finalTitle}</h1>
<p style="text-indent: 30px; line-height: 1.6;">${convertToUzbekLatin(combinedResult)}</p>`

		await transcriptService.saveFinalTranscript(jobId, finalTranscript)
		jobLogger.info(`Final transcript saved.`)
		await pushTranscriptionEvent(jobId, finalTranscript, true, broadcast) // Mark success
		jobStatusUpdated = true // Ensure status isn't marked as error in final catch
	} catch (err: any) {
		// --- Main Catch Block ---
		// Catches errors from:
		// 1. Initial getVideoInfoWithYtdlCore (already translated for user)
		// 2. Fatal errors thrown from within the segment loop (already translated for user)
		// 3. Retry exhaustion error from the segment loop (already translated for user)
		// 4. Bucket name missing error (already translated for user)
		// 5. Any other unexpected errors before or after the loop.

		jobLogger.error(
			{ error: err.message, stack: err.stack?.substring(0, 700) },
			'Critical error caught in runTranscriptionJob main try-catch block'
		)

		// Ensure the job status is marked as 'error' in the DB if it wasn't already
		if (!jobStatusUpdated) {
			try {
				await transcriptService.error(jobId)
				jobStatusUpdated = true // Mark it as updated now
			} catch (dbErr: any) {
				jobLogger.error(
					{ error: dbErr.message },
					'Failed to mark job as error in DB during final catch block'
				)
			}
		}

		// --- Final Error Reporting to User ---
		// If a specific user message wasn't already pushed by the code that threw the error,
		// push a generic one here. Check if the error message suggests it was already handled.
		if (broadcast && !jobStatusUpdated) {
			// Check jobStatusUpdated again; if true, message was likely sent
			// Check if error message looks like one of our pre-translated fatal errors
			const alreadyHandledMessages = [
				'Xatolik:', // Our Uzbek error prefix
				'Failed to process segment', // Retry exhaustion message
				'Server konfiguratsiya xatosi' // Bucket error
			]
			const alreadyHandled = alreadyHandledMessages.some(msg =>
				err.message?.includes(msg)
			)

			if (!alreadyHandled) {
				try {
					// Provide a generic fallback message if the specific error wasn't caught/translated earlier
					let clientErrorMessage = `Serverda kutilmagan xatolik yuz berdi va jarayon to'xtatildi. Texnik tafsilotlar: (${err.message || 'No details'})`

					// Push this generic message
					await pushTranscriptionEvent(
						jobId,
						clientErrorMessage,
						true, // Mark completed (with error)
						broadcast
					)
				} catch (sseErr: any) {
					jobLogger.error(
						{ error: sseErr.message },
						'Failed to send final generic error event in main catch block'
					)
				}
			}
		}
	} finally {
		// This block always runs, regardless of success or failure
		const finalDuration = performance.now() - startTime
		jobLogger.info(
			`Transcription job function finished execution after ${formatDuration(finalDuration)}.`
		)
		// Consider final cleanup steps if necessary (e.g., releasing locks), though stream/file cleanup is handled per-segment.
	}
}
