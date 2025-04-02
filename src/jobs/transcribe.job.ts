// import { SecretManagerServiceClient } from '@google-cloud/secret-manager'
import { spawn } from 'child_process'
import ffmpeg from 'fluent-ffmpeg'
import os from 'os'
import path from 'path'
import { performance } from 'perf_hooks'
import { Readable } from 'stream'
import { v4 as uuidv4 } from 'uuid'

// For unique temp file names

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

// --- Helper: Fetch Secret (Example - Adapt to your setup) ---
// const secretManager = new SecretManagerServiceClient()
// async function getSecret(secretName: string): Promise<string | undefined> {
// 	try {
// 		const [version] = await secretManager.accessSecretVersion({
// 			name: `projects/${process.env.GOOGLE_CLOUD_PROJECT_ID}/secrets/${secretName}/versions/latest`
// 		})
// 		const payload = version.payload?.data?.toString()
// 		if (!payload) {
// 			logger.warn(`Secret ${secretName} payload is empty.`)
// 			return undefined
// 		}
// 		logger.info(`Successfully fetched secret: ${secretName}`)
// 		return payload
// 	} catch (error: any) {
// 		logger.error(
// 			{ error: error.message, secretName },
// 			'Failed to fetch secret from Secret Manager.'
// 		)
// 		return undefined // Or throw if the secret is critical
// 	}
// }

// --- Helper: Manage Temporary Cookie File ---
async function useCookieFile(
	cookieValue: string | undefined,
	logPrefix: string = 'yt-dlp'
): Promise<{ cookieFilePath?: string; cleanup: () => Promise<void> }> {
	if (!cookieValue) {
		return { cleanup: async () => {} } // No cookie, nothing to do
	}

	const tempDir = os.tmpdir() // Get system's temp directory (/tmp in Cloud Run)
	const uniqueId = uuidv4()
	const cookieFilePath = path.join(tempDir, `youtube_cookies_${uniqueId}.txt`)

	const cleanup = async () => {
		try {
			await fs.unlink(cookieFilePath)
			logger.info(
				`${logPrefix}: Successfully deleted temp cookie file: ${cookieFilePath}`
			)
		} catch (err: any) {
			// Log error but don't fail the main process if cleanup fails
			if (err.code !== 'ENOENT') {
				// Ignore if file doesn't exist
				logger.warn(
					{ error: err.message, file: cookieFilePath },
					`${logPrefix}: Failed to delete temp cookie file.`
				)
			}
		}
	}

	try {
		// Write the cookie value to the temp file (Netscape format is often preferred, but yt-dlp might handle simple text too)
		// Ensure the cookieValue is in the correct format if needed. If it's just the 'Cookie: ...' header value, it might need processing.
		// Assuming cookieValue is the raw content needed for the file.
		await fs.writeFile(cookieFilePath, cookieValue, {
			encoding: 'utf-8',
			mode: 0o600
		}) // Set permissions
		logger.info(
			`${logPrefix}: Successfully created temp cookie file: ${cookieFilePath}`
		)
		return { cookieFilePath, cleanup }
	} catch (err: any) {
		logger.error(
			{ error: err.message, file: cookieFilePath },
			`${logPrefix}: Failed to create temp cookie file.`
		)
		await cleanup() // Attempt cleanup even if creation failed
		throw new Error(`Failed to write cookie file: ${err.message}`) // Re-throw to signal failure
	}
}

// --- Helper Functions for yt-dlp (Modified) ---
interface VideoInfo {
	title: string
	duration: number // in seconds
}

async function getVideoInfoWithYtDlp(
	youtubeUrl: string,
	cookie?: string
): Promise<VideoInfo> {
	let cookieHandler: {
		cookieFilePath?: string
		cleanup: () => Promise<void>
	} | null = null
	try {
		cookieHandler = await useCookieFile(cookie, 'yt-dlp-info')
		const args = [
			'-v',
			'--no-warnings',
			'--no-call-home',
			'--ignore-config',
			'--dump-json',
			'--skip-download',
			// '--force-ipv4', // Uncomment if IPv6 issues are suspected
			'--user-agent',
			'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',

			youtubeUrl
		]

		if (cookieHandler.cookieFilePath) {
			args.unshift('--cookies', cookieHandler.cookieFilePath)
			logger.info('Using temp cookie file with yt-dlp info command.')
		} else {
			logger.warn('No YouTube cookie provided for yt-dlp info command.')
		}

		logger.info(
			`Spawning yt-dlp to get video info: yt-dlp ${args.join(' ')}`
		)

		return await new Promise<VideoInfo>((resolve, reject) => {
			const ytDlpProcess = spawn('yt-dlp', args)
			let jsonData = ''
			let errorData = ''

			ytDlpProcess.stdout.on('data', data => {
				jsonData += data.toString()
			})
			ytDlpProcess.stderr.on('data', data => {
				const errLine = data.toString()
				errorData += errLine
				// Log stderr lines that aren't just warnings
				if (!errLine.includes('WARNING:')) {
					logger.warn(`yt-dlp info stderr: ${errLine.trim()}`)
				}
			})
			ytDlpProcess.on('error', err => {
				logger.error(
					{ error: err },
					'Failed to spawn yt-dlp process for info.'
				)
				reject(
					new Error(`Failed to start yt-dlp for info: ${err.message}`)
				)
			})
			ytDlpProcess.on('close', code => {
				if (code !== 0) {
					logger.error(
						`yt-dlp info process exited with code ${code}. Stderr: ${errorData}`
					)
					// Enhance error detection
					let specificError = `yt-dlp info process exited with code ${code}.`
					if (
						errorData.includes('Private video') ||
						errorData.includes('login required') ||
						errorData.includes('confirm your age') ||
						errorData.includes('unavailable') ||
						errorData.includes('Sign in') ||
						errorData.includes('403') ||
						errorData.includes('Premiere') ||
						errorData.includes('confirm you') // For "confirm you're not a bot"
					) {
						specificError = `YouTube access error (yt-dlp info): Video might be private/unavailable/premiere, require login/age confirmation, or cookie invalid/rejected. Code ${code}.`
					} else if (errorData.includes('ModuleNotFoundError')) {
						specificError = `yt-dlp execution failed (ModuleNotFoundError). Ensure Python environment and yt-dlp installation are correct. Code ${code}.`
					}
					reject(
						new Error(
							`${specificError} Stderr: ${errorData.substring(0, 500)}`
						)
					)
				} else {
					try {
						if (!jsonData) {
							// ... (rest of the JSON parsing logic remains the same)
							logger.error(
								'yt-dlp info command closed successfully but produced no JSON output. Stderr: ' +
									errorData
							)
							reject(
								new Error(
									'yt-dlp returned empty JSON output for video info.'
								)
							)
							return
						}
						const info = JSON.parse(jsonData)
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
						// ... (rest of the JSON parsing logic remains the same)
						logger.error(
							{
								error: parseErr,
								rawJson: jsonData,
								stderr: errorData
							},
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
	} finally {
		// Ensure cookie file is deleted
		if (cookieHandler) {
			await cookieHandler.cleanup()
		}
	}
}

async function streamAudioWithYtDlp(
	youtubeUrl: string,
	startTime: number,
	duration: number,
	cookie?: string
): Promise<Readable> {
	// Note: We need to manage the cookie file slightly differently for streaming
	// because the cleanup should happen *after* the stream is fully consumed or errored.
	// We'll create it here, pass the path, and rely on the caller or stream events for cleanup.
	// Simpler approach for now: Create/cleanup within this function scope, assuming ffmpeg consumes it quickly.
	// A more robust approach might involve passing the cleanup function back or managing it externally.

	let cookieHandler: {
		cookieFilePath?: string
		cleanup: () => Promise<void>
	} | null = null

	try {
		cookieHandler = await useCookieFile(cookie, 'yt-dlp-stream')

		const args = [
			'-v',
			'--no-warnings',
			'--no-call-home',
			'--ignore-config',
			'-f',
			'bestaudio/best', // Fetch best audio quality
			'--output',
			'-', // Output to stdout
			// '--force-ipv4', // Uncomment if IPv6 issues are suspected
			// Use FFmpeg for seeking/duration via postprocessor args *if needed*,
			// but yt-dlp can sometimes handle this directly depending on format.
			// Using postprocessor args is generally reliable for precise segments.
			'--postprocessor-args',
			`ffmpeg:-ss ${startTime} -to ${startTime + duration}`, // Use -to for more precise end time
			// Alternative: Direct download range (might be less reliable)
			// '--download-sections', `*${startTime}-${startTime + duration}`,
			'--user-agent',
			'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',

			youtubeUrl
		]

		if (cookieHandler.cookieFilePath) {
			args.unshift('--cookies', cookieHandler.cookieFilePath)
			logger.info('Using temp cookie file with yt-dlp stream command.')
		} else {
			logger.warn('No YouTube cookie provided for yt-dlp stream command.')
		}

		logger.info(
			`Spawning yt-dlp for audio segment: yt-dlp ${args.join(' ')}`
		)
		const ytDlpProcess = spawn('yt-dlp', args, {
			stdio: ['ignore', 'pipe', 'pipe'] // stdin, stdout, stderr
		})

		const outputAudioStream = ytDlpProcess.stdout
		let stderrData = ''

		ytDlpProcess.stderr.on('data', data => {
			const errLine = data.toString()
			stderrData += errLine
			if (
				!errLine.includes('WARNING:') &&
				!errLine.includes('Output stream #') && // Filter ffmpeg noise if it appears here
				!errLine.includes('[download]') && // Filter download progress
				!/frame=/.test(errLine) && // Filter ffmpeg progress
				!/size=/.test(errLine) &&
				!/time=/.test(errLine) &&
				!/bitrate=/.test(errLine) &&
				!/speed=/.test(errLine)
			) {
				logger.warn(`yt-dlp stream stderr: ${errLine.trim()}`)
			}
		})

		ytDlpProcess.on('error', err => {
			logger.error(
				{ error: err, stderr: stderrData },
				'Failed to spawn yt-dlp process for streaming.'
			)
			outputAudioStream.emit(
				'error',
				new Error(
					`Failed to start yt-dlp stream process: ${err.message}`
				)
			)
		})

		ytDlpProcess.on('close', async code => {
			// --- Crucial: Cleanup cookie file after process closes ---
			if (cookieHandler) {
				await cookieHandler.cleanup()
				cookieHandler = null // Prevent double cleanup in finally
			}
			// --- End Cleanup ---

			if (code !== 0) {
				const detailedErrorMessage = `yt-dlp stream process exited with error code ${code}. Stderr: ${stderrData.substring(0, 1000)}`
				logger.error(detailedErrorMessage)
				let specificError = detailedErrorMessage
				if (stderrData.includes('ModuleNotFoundError')) {
					specificError = `yt-dlp execution failed (ModuleNotFoundError). Check container setup. Code ${code}.`
				} else if (
					stderrData.includes('403 Forbidden') ||
					stderrData.includes('Sign in') ||
					stderrData.includes('confirm you') || // Bot check
					stderrData.includes('login required')
				) {
					specificError = `yt-dlp download failed (Authentication Error - 403/Login/Bot Check?). Check cookie validity/freshness. Code ${code}.`
				} else if (stderrData.includes('Socket error')) {
					specificError = `yt-dlp download failed (Network/Socket error). Code ${code}.`
				}
				outputAudioStream.emit('error', new Error(specificError))
			} else {
				logger.info('yt-dlp stream process finished successfully.')
			}
		})

		// Handle potential errors on the stream itself after the process has started
		outputAudioStream.on('error', async err => {
			logger.error(
				{ error: err },
				'Error emitted directly on yt-dlp output stream.'
			)
			// Ensure cleanup happens if the stream errors *before* the process closes naturally
			if (cookieHandler) {
				await cookieHandler.cleanup()
				cookieHandler = null
			}
		})

		return outputAudioStream
	} catch (error: any) {
		// Catch errors from useCookieFile
		logger.error(
			{ error: error.message },
			'Error setting up yt-dlp stream (e.g., cookie file creation failed)'
		)
		// Ensure cleanup if cookieHandler was partially created
		if (cookieHandler) {
			await cookieHandler.cleanup()
		}
		// Need to return a stream that immediately emits an error
		const errorStream = new Readable({
			read() {
				this.emit('error', error)
				this.push(null) // End the stream
			}
		})
		return errorStream
	}
	// Note: The 'finally' block here would run *before* the async operations inside 'close' or stream events.
	// Cleanup is handled within the 'close' and 'error' handlers for the process/stream.
}

// --- Main Transcription Job Logic (Largely unchanged, but incorporating new yt-dlp calls) ---

export async function pushTranscriptionEvent(
	jobId: string,
	content: string,
	completed = false,
	broadcast?: (content: string, completed: boolean) => void
) {
	// ... (implementation remains the same)
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
	const operationId = `job-${jobId}-${Date.now()}`
	const jobLogger = logger.child({ jobId, operationId })

	jobLogger.info('Starting transcription job...')

	// --- TODO: Securely Fetch Cookie ---
	// Option 1: From Env var (current)
	const youtubeCookie = process.env.YOUTUBE_COOKIE
	// Option 2: From Secret Manager (Recommended)
	// const youtubeCookieSecretName = 'youtube-secret'
	// const youtubeCookie = await getSecret(youtubeCookieSecretName)
	// if (!youtubeCookie) {
	// 	jobLogger.error(
	// 		`Critical: YouTube cookie secret "${youtubeCookieSecretName}" not found or empty.`
	// 	)
	// 	await pushTranscriptionEvent(
	// 		jobId,
	// 		'Server konfiguratsiya xatosi: Cookie topilmadi.',
	// 		true,
	// 		broadcast
	// 	)
	// 	await transcriptService.error(jobId)
	// 	return // Stop the job
	// }
	// -----------------------------------

	try {
		await transcriptService.running(jobId)
		jobStatusUpdated = true
		await delay(1000) // Give DB time

		// --- Get Video Info ---
		let videoInfo: VideoInfo
		jobLogger.info(
			{
				hasCookie: !!youtubeCookie,
				cookieLength: youtubeCookie?.length ?? 0
			},
			'Checking YouTube cookie presence before yt-dlp info call'
		)

		try {
			jobLogger.info(`Fetching video info via yt-dlp for URL: ${url}`)
			// Pass the fetched/retrieved cookie string to the helper function
			videoInfo = await getVideoInfoWithYtDlp(url, youtubeCookie)
			jobLogger.info(
				`Successfully fetched video info via yt-dlp for title: ${videoInfo.title}`
			)
		} catch (err: any) {
			jobLogger.error(
				{ error: err.message, stack: err.stack, url: url },
				'Failed to get video info from yt-dlp.'
			)
			let errorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). URL, server yoki cookie'ni tekshiring. (${err.message || 'Unknown yt-dlp info error'})`
			// Use the more specific error message from the helper
			if (err.message?.includes('YouTube access error')) {
				errorMessage = `Video ma'lumotlarini olib bo'lmadi (yt-dlp). YouTube kirish xatosi (maxfiy/yosh/bot tekshiruvi/cookie yaroqsiz?). (${err.message})`
			} else if (err.message?.includes('ModuleNotFoundError')) {
				errorMessage = `Server xatosi: yt-dlp ishga tushmadi (ModuleNotFoundError). (${err.message})`
			} else if (err.message?.includes('write cookie file')) {
				errorMessage = `Server xatosi: Cookie faylini yozib bo'lmadi. (${err.message})`
			}

			await pushTranscriptionEvent(jobId, errorMessage, true, broadcast)
			await transcriptService.error(jobId)
			jobStatusUpdated = true
			return
		}
		// --- End Get Video Info ---

		// ... (Rest of the logic for title update, UI messages, segment calculation remains IDENTICAL) ...
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
			'Ovoz yuklanmoqda (yt-dlp)...',
			false,
			broadcast
		)
		await delay(500)
		const segmentDuration = 150 // seconds per segment
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
			// Ensure duration is positive, even for very short final segments
			const safeActualDuration = Math.max(0.1, actualDuration)

			const destFileName = `segment_${jobId}_${segmentNumber}.mp3` // Consistent definition
			const gcsUri = `gs://${bucketName}/${destFileName}`

			let segmentProcessedSuccessfully = false
			let attempt = 0
			const maxAttempts = 2 // Retry logic remains

			while (!segmentProcessedSuccessfully && attempt < maxAttempts) {
				attempt++
				const segmentLogger = jobLogger.child({
					segment: segmentNumber,
					attempt
				})
				let gcsUploadSucceeded = false // Reset for each attempt

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
					await delay(1000 * attempt) // Simple backoff
				}

				try {
					await pushTranscriptionEvent(
						jobId,
						`Bo'lak ${segmentNumber}/${numSegments} yuklanmoqda (yt-dlp)...`,
						false,
						broadcast
					)

					// --- Stream Audio Segment (Use the updated function) ---
					segmentLogger.info(
						`Attempting segment download via yt-dlp (start: ${segmentStartTime}s, duration: ${safeActualDuration}s)...`
					)
					// Pass the cookie string here as well
					const audioStream = await streamAudioWithYtDlp(
						url,
						segmentStartTime,
						safeActualDuration,
						youtubeCookie // Pass the cookie
					)

					// --- Create ffmpeg command (No changes here needed) ---
					segmentLogger.info(`Starting FFmpeg encoding...`)
					const ffmpegCommand = ffmpeg(audioStream)
						.format('mp3')
						.audioCodec('libmp3lame')
						.audioBitrate('96k') // Keep bitrate
						// .audioQuality(undefined) // REMOVED - good
						.on('start', cmd =>
							segmentLogger.info(`FFmpeg started: ${cmd}`)
						)
						.on('error', (err, stdout, stderr) => {
							// Log details on error event
							segmentLogger.error(
								{ message: err.message, stdout, stderr },
								`FFmpeg error event processing segment (command level)`
							)
							// Note: The promise rejection below handles control flow
						})
						.on('end', () => {
							segmentLogger.info(
								`FFmpeg processing seemingly finished.`
							) // Might fire even if upload fails later
						})

					// --- Wrap ffmpeg processing and upload in a Promise (No changes here needed) ---
					await new Promise<void>((resolve, reject) => {
						const ffmpegOutputStream = ffmpegCommand.pipe()
						let promiseRejected = false // Prevent double rejection

						// Handle input stream errors (yt-dlp)
						audioStream.on('error', inputError => {
							if (promiseRejected) return
							promiseRejected = true
							segmentLogger.error(
								{ error: inputError.message },
								'Error emitted on yt-dlp input stream for ffmpeg'
							)
							try {
								// Attempt to kill ffmpeg if input fails
								ffmpegCommand.kill('SIGKILL')
							} catch (killErr: any) {
								segmentLogger.warn(
									{ error: killErr.message },
									'Error trying to kill ffmpeg after input stream error'
								)
							}
							// Reject the promise wrapping ffmpeg/upload
							reject(
								new Error(
									`Input stream error: ${inputError.message}`
								)
							)
						})

						// Handle ffmpeg's own errors more directly
						ffmpegCommand.on('error', err => {
							if (promiseRejected) return
							promiseRejected = true
							// Logged above by the other listener, just reject here
							reject(
								new Error(
									`FFmpeg command failed directly: ${err.message}`
								)
							)
						})

						// Handle output stream errors (piping/upload issues)
						ffmpegOutputStream.on('error', outputError => {
							if (promiseRejected) return
							promiseRejected = true
							segmentLogger.error(
								{ error: outputError.message },
								'Error emitted on ffmpeg output stream during upload pipe.'
							)
							reject(
								new Error(
									`FFmpeg output stream error: ${outputError.message}`
								)
							)
						})

						// Pipe ffmpeg output to GCS upload
						uploadStreamToGCS(ffmpegOutputStream, destFileName)
							.then(() => {
								if (!promiseRejected) {
									gcsUploadSucceeded = true // Mark GCS upload as successful only if no prior errors
									segmentLogger.info(
										`Segment successfully encoded and uploaded to ${gcsUri}`
									)
									resolve() // Success
								} else {
									segmentLogger.warn(
										'GCS upload technically finished, but an error occurred earlier in the ffmpeg/stream process.'
									)
									// Do not resolve; let the existing rejection stand.
								}
							})
							.catch(uploadErr => {
								if (promiseRejected) return
								promiseRejected = true
								segmentLogger.error(
									{ error: uploadErr.message },
									'GCS upload failed.'
								)
								reject(
									new Error(
										`GCS upload failed: ${uploadErr.message}`
									)
								)
							})
					})
					// --- End ffmpeg/upload Promise ---

					// --- Transcriptions & Editing (No changes needed here) ---
					await pushTranscriptionEvent(
						jobId,
						`Google matnni o'girmoqda ${segmentNumber}/${numSegments}...`,
						false,
						broadcast
					)
					const transcriptGoogle = await transcribeWithGoogle(gcsUri)
					if (!transcriptGoogle) {
						segmentLogger.error(
							`Google transcription returned empty/null`
						)
						await pushTranscriptionEvent(
							jobId,
							`${segmentNumber}/${numSegments}-chi Google matnida xatolik (bo'sh natija). Qayta uriniladi...`,
							false,
							broadcast
						)
						continue // Trigger retry in the while loop
					}
					segmentLogger.info(`Google transcription done.`)

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
					} catch (elevenLabsError: any) {
						segmentLogger.error(
							{ error: elevenLabsError.message },
							`ElevenLabs transcription failed`
						)
						await pushTranscriptionEvent(
							jobId,
							`${segmentNumber}/${numSegments}-chi ElevenLabs matnida xatolik (${elevenLabsError.message}). Qayta uriniladi...`,
							false,
							broadcast
						)
						continue // Trigger retry
					}
					if (!transcriptElevenLabs) {
						segmentLogger.error(
							`ElevenLabs transcription returned empty/null`
						)
						await pushTranscriptionEvent(
							jobId,
							`${segmentNumber}/${numSegments}-chi ElevenLabs matnida xatolik (bo'sh natija). Qayta uriniladi...`,
							false,
							broadcast
						)
						continue // Trigger retry
					}
					segmentLogger.info(`ElevenLabs transcription done.`)

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
						segmentLogger.error(
							`Gemini editing returned empty/null`
						)
						await pushTranscriptionEvent(
							jobId,
							`${segmentNumber}/${numSegments}-chi Gemini tahririda xatolik (bo'sh natija). Qayta uriniladi...`,
							false,
							broadcast
						)
						continue // Trigger retry
					}
					segmentLogger.info(`Gemini editing done.`)
					// --- End Transcriptions & Editing ---

					// --- Segment Success ---
					editedTexts.push(finalText)
					segmentProcessedSuccessfully = true // Mark success for this segment
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi bo'lak tayyor!`,
						false,
						broadcast
					)
				} catch (segmentErr: any) {
					// Log segment error (unchanged, but context helps)
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

					// --- Check for Fatal Errors ---
					// Prioritize yt-dlp/stream errors as they are often unrecoverable without intervention (e.g., bad cookie)
					if (
						segmentErr.message?.includes('Input stream error:') || // Error reading from yt-dlp
						segmentErr.message?.includes(
							'yt-dlp stream process exited'
						) || // yt-dlp process failed
						segmentErr.message?.includes('403 Forbidden') || // Specific yt-dlp auth errors
						segmentErr.message?.includes('Sign in') ||
						segmentErr.message?.includes('confirm you') ||
						segmentErr.message?.includes('login required') ||
						segmentErr.message?.includes('Authentication Error') || // Catch our specific message
						segmentErr.message?.includes('ModuleNotFoundError') || // yt-dlp didn't run
						segmentErr.message?.includes(
							'cookie file creation failed'
						) // Couldn't setup cookie
					) {
						segmentLogger.error(
							'Fatal yt-dlp/stream related error occurred. Aborting job.'
						)
						let userMsg = `YouTube yuklashda/kirishda xatolik (yt-dlp ${segmentNumber}/${numSegments}). Cookie/URL/Video holatini tekshiring. Jarayon to'xtatildi.`
						if (
							segmentErr.message?.includes('ModuleNotFoundError')
						) {
							userMsg = `Server xatosi: yt-dlp ishga tushmadi (${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`
						} else if (
							segmentErr.message?.includes(
								'cookie file creation failed'
							)
						) {
							userMsg = `Server xatosi: Cookie faylini sozlab bo'lmadi (${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`
						}
						await pushTranscriptionEvent(
							jobId,
							userMsg,
							true,
							broadcast
						)
						// Throw to exit the main try block
						throw new Error(
							`Aborting job due to fatal stream/auth failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}
					// Check for fatal ffmpeg errors
					else if (
						segmentErr.message?.includes('FFmpeg command failed')
					) {
						segmentLogger.error(
							'Fatal FFmpeg error occurred during processing. Aborting job.'
						)
						await pushTranscriptionEvent(
							jobId,
							`Audio kodlashda xatolik (FFmpeg ${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`,
							true,
							broadcast
						)
						// Throw to exit the main try block
						throw new Error(
							`Aborting job due to fatal FFmpeg failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}
					// Check for fatal GCS upload errors
					else if (
						segmentErr.message?.includes('GCS upload failed')
					) {
						segmentLogger.error(
							'Fatal GCS upload error occurred. Aborting job.'
						)
						await pushTranscriptionEvent(
							jobId,
							`Audio bo'lakni saqlashda xatolik (GCS ${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`,
							true,
							broadcast
						)
						// Throw to exit the main try block
						throw new Error(
							`Aborting job due to fatal GCS upload failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}

					// If not a fatal error, allow retry loop to continue after delay
					await delay(2000 + attempt * 1000) // Slightly longer delay for retries
				} finally {
					// --- Cleanup GCS File ---
					if (gcsUploadSucceeded) {
						try {
							// Check destFileName has a value - belt and suspenders
							if (!destFileName) {
								segmentLogger.error(
									'destFileName was unexpectedly empty or undefined before GCS delete attempt!'
								)
							} else {
								segmentLogger.info(
									`Attempting to delete GCS file: ${destFileName}`
								)
								await deleteGCSFile(destFileName)
								segmentLogger.info(
									`Successfully deleted GCS file: ${destFileName}`
								)
							}
						} catch (deleteErr: any) {
							// Log details but don't fail the job for cleanup error
							segmentLogger.error(
								{
									error: deleteErr.message,
									file: destFileName
								},
								`Failed to delete GCS segment file: ${destFileName}. Continuing job.`
							)
							// Check for specific known issues if necessary
							if (
								deleteErr.message?.includes(
									'file name must be specified'
								)
							) {
								segmentLogger.error(
									`Potential bug in deleteGCSFile or filename generation: ${deleteErr.message}`
								)
							}
						}
					} else {
						// Log if deletion is skipped
						if (destFileName) {
							// Only log if we had a filename defined
							segmentLogger.info(
								`Skipping GCS delete for ${destFileName} because upload did not succeed or an earlier error occurred.`
							)
						}
					}
					await delay(300) // Small delay after cleanup/skip
				}
			} // End retry loop (while !segmentProcessedSuccessfully && attempt < maxAttempts)

			// If segment failed after all attempts (unchanged)
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
				// Throw to exit the main try block
				throw new Error(
					`Failed to process segment ${segmentNumber} after ${maxAttempts} attempts.`
				)
			}

			i++ // Move to the next segment
		} // End segment loop (while i < numSegments)

		// --- Combine and Finalize (No changes needed here) ---
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
		// Construct final HTML output
		const finalTranscript = `<i style="display: block; font-style: italic; text-align: center;">🕒 Transkripsiya uchun ${formatDuration(duration)} vaqt ketdi.</i><h1 style="font-weight: 700; font-size: 1.8rem; margin: 1rem 0; text-align: center; line-height: 1;">${finalTitle}</h1>\n\n<p style="text-indent: 30px;">${convertToUzbekLatin(combinedResult)}</p>`

		await transcriptService.saveFinalTranscript(jobId, finalTranscript)
		jobLogger.info(`Final transcript saved.`)
		await pushTranscriptionEvent(jobId, finalTranscript, true, broadcast) // Send final result and mark completed
		jobStatusUpdated = true // Mark status update happened successfully
	} catch (err: any) {
		// --- Final Error Handling (Adjusted for new error types) ---
		jobLogger.error(
			{ error: err.message, stack: err.stack },
			'Critical error in runTranscriptionJob'
		)

		// Ensure job status is marked as error if it hasn't been updated yet
		if (!jobStatusUpdated) {
			try {
				await transcriptService.error(jobId)
				jobStatusUpdated = true // Mark it now
			} catch (dbErr: any) {
				jobLogger.error(
					{ error: dbErr.message },
					'Failed to mark job as error in DB during final catch block'
				)
			}
		}

		// Send a final error message to the client via SSE
		if (broadcast) {
			try {
				// Provide more specific user-facing error messages based on the caught error
				let clientErrorMessage = `Serverda kutilmagan xatolik yuz berdi. (${err.message?.substring(0, 100) || 'No details'}...)`

				if (
					err.message?.includes(
						'Aborting job due to fatal stream/auth failure'
					)
				) {
					clientErrorMessage = `Xatolik: YouTube'dan yuklab bo'lmadi yoki kirishda muammo (maxfiy/yosh/bot/cookie?/server xato?). Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
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
				} else if (err.message?.includes('Failed to process segment')) {
					// Error from segment retry exhaustion
					clientErrorMessage = `Xatolik: ${err.message}` // Use the specific message
				} else if (
					err.message?.includes('yt-dlp info process exited')
				) {
					// Error from initial info fetch
					clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). URL/Cookie/Video holatini tekshiring. (${err.message?.substring(0, 100)}...)`
				} else if (
					err.message?.includes('GOOGLE_CLOUD_BUCKET_NAME') ||
					err.message?.includes('Bucket topilmadi')
				) {
					clientErrorMessage = `Server konfiguratsiya xatosi: Bucket topilmadi.`
				} else if (err.message?.includes('Cookie topilmadi')) {
					clientErrorMessage = `Server konfiguratsiya xatosi: Cookie topilmadi.`
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
					'Failed to send final error SSE event'
				)
			}
		}
	} finally {
		jobLogger.info('Transcription job finished execution.')
		// Final cleanup attempt for any lingering cookie files (should be handled earlier, but as a safeguard)
		// Note: This might not be effective if cookieHandler scope was lost due to errors.
		// The cleanup within the specific yt-dlp function calls is more reliable.
	}
}
