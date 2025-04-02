// import { SecretManagerServiceClient } from '@google-cloud/secret-manager'
import { spawn } from 'child_process'
import ffmpeg from 'fluent-ffmpeg'
// Use promises for async file operations
import os from 'os'
// To get temporary directory
import path from 'path'
// To join paths reliably
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

// --- Helper: Manage Temporary Cookie File (Handles Base64 Decoding) ---
async function useCookieFile(
	base64CookieValue: string | undefined, // Renamed to indicate expectation
	logPrefix: string = 'yt-dlp'
): Promise<{ cookieFilePath?: string; cleanup: () => Promise<void> }> {
	if (!base64CookieValue) {
		logger.warn(`${logPrefix}: No Base64 cookie value provided.`)
		return { cleanup: async () => {} } // No cookie, nothing to do
	}

	let decodedCookieContent: string
	try {
		// --- Decode the Base64 content ---
		decodedCookieContent = Buffer.from(
			base64CookieValue,
			'base64'
		).toString('utf-8')
		// --- End Decoding ---

		// Basic sanity check for Netscape format (optional but helpful)
		if (
			!decodedCookieContent
				.trim()
				.startsWith('# Netscape HTTP Cookie File')
		) {
			logger.warn(
				`${logPrefix}: Decoded cookie content does not appear to start with the expected Netscape header.`
			)
			// Consider throwing an error here if format is strictly required
			// throw new Error("Decoded cookie content is not in expected Netscape format.");
		}
		logger.info(
			`${logPrefix}: Successfully decoded Base64 cookie string (length: ${decodedCookieContent.length}).`
		)
	} catch (decodeError: any) {
		logger.error(
			{ error: decodeError.message },
			`${logPrefix}: Failed to decode Base64 cookie string.`
		)
		throw new Error(
			`Failed to decode Base64 cookie: ${decodeError.message}`
		)
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
			if (err.code !== 'ENOENT') {
				logger.warn(
					{ error: err.message, file: cookieFilePath },
					`${logPrefix}: Failed to delete temp cookie file.`
				)
			}
		}
	}

	try {
		// Write the *decoded* Netscape format content to the file
		await fs.writeFile(cookieFilePath, decodedCookieContent, {
			encoding: 'utf-8',
			mode: 0o600 // Set permissions (read/write for owner only)
		})
		logger.info(
			`${logPrefix}: Successfully created temp cookie file from decoded Base64: ${cookieFilePath}`
		)
		return { cookieFilePath, cleanup }
	} catch (writeErr: any) {
		logger.error(
			{ error: writeErr.message, file: cookieFilePath },
			`${logPrefix}: Failed to create temp cookie file.`
		)
		await cleanup() // Attempt cleanup even if creation failed
		throw new Error(
			`Failed to write decoded cookie file: ${writeErr.message}`
		)
	}
}

// --- Helper Functions for yt-dlp (Modified to use useCookieFile) ---
interface VideoInfo {
	title: string
	duration: number // in seconds
}

async function getVideoInfoWithYtDlp(
	youtubeUrl: string,
	base64Cookie?: string // Expects Base64 encoded cookie string
): Promise<VideoInfo> {
	let cookieHandler: {
		cookieFilePath?: string
		cleanup: () => Promise<void>
	} | null = null
	try {
		// Pass the Base64 string from ENV var to the helper
		cookieHandler = await useCookieFile(base64Cookie, 'yt-dlp-info')

		const args = [
			'--no-warnings',
			'--no-call-home',
			'--ignore-config',
			'--dump-json',
			'--skip-download',
			// '--force-ipv4', // Uncomment if IPv6 issues are suspected on Cloud Run
			youtubeUrl
		]

		if (cookieHandler.cookieFilePath) {
			// Use the path to the file containing the *decoded* content
			args.unshift('--cookies', cookieHandler.cookieFilePath)
			logger.info(
				'Using temp cookie file (from decoded Base64) with yt-dlp info command.'
			)
		} else {
			logger.warn(
				'No YouTube cookie provided (or decode failed) for yt-dlp info command.'
			)
		}

		logger.info(
			`Spawning yt-dlp to get video info: yt-dlp ${args.join(' ')}`
		)

		// --- Promise wrapper for spawn remains the same ---
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
				if (!errLine.includes('WARNING:')) {
					// Still log non-warning stderr
					// Avoid logging the specific "Sign in..." error repeatedly if it's the main issue
					if (
						!errLine.includes('Sign in to confirm') &&
						!errLine.includes('confirm you')
					) {
						logger.warn(`yt-dlp info stderr: ${errLine.trim()}`)
					}
				}
			})
			ytDlpProcess.on('error', err => {
				// Handle spawn errors (e.g., yt-dlp not found)
				logger.error(
					{ error: err },
					'Failed to spawn yt-dlp process for info.'
				)
				reject(
					new Error(`Failed to start yt-dlp for info: ${err.message}`)
				)
			})
			ytDlpProcess.on('close', code => {
				// Handle yt-dlp exit
				if (code !== 0) {
					logger.error(
						`yt-dlp info process exited with code ${code}. Stderr: ${errorData.substring(0, 1000)}` // Log more stderr on error
					)
					let specificError = `yt-dlp info process exited with code ${code}.`
					// Detect the specific bot/auth error
					if (
						errorData.includes('Sign in to confirm') ||
						errorData.includes('confirm your age') ||
						errorData.includes('confirm you') || // Bot check variations
						errorData.includes('login required') ||
						errorData.includes('Private video') ||
						errorData.includes('unavailable') ||
						errorData.includes('403') ||
						errorData.includes('Premiere')
					) {
						specificError = `YouTube access error (yt-dlp info): Authentication failed or video requires login/confirmation (e.g., bot check, age gate, premiere, private). Likely invalid/expired/incomplete cookie, or IP flagged. Code ${code}.`
					} else if (errorData.includes('ModuleNotFoundError')) {
						specificError = `yt-dlp execution failed (ModuleNotFoundError). Ensure Python environment and yt-dlp installation are correct in container. Code ${code}.`
					}
					// Add check for cookie decoding failure message if thrown earlier
					else if (
						errorData.includes('Failed to decode Base64 cookie')
					) {
						specificError = `Cookie handling error: Failed to decode Base64 cookie provided in environment variable. Code ${code}.`
					}
					reject(
						new Error(
							`${specificError} Stderr: ${errorData.substring(0, 500)}`
						) // Keep error message concise for wrapper
					)
				} else {
					// Success (code 0)
					try {
						// Parse JSON output
						if (!jsonData) {
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
								'yt-dlp JSON missing title or duration.'
							)
							reject(
								new Error(
									'Failed to parse title/duration from yt-dlp JSON.'
								)
							)
						}
					} catch (parseErr: any) {
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
		// --- End Promise wrapper ---
	} catch (err: any) {
		// Catch errors from useCookieFile (e.g., Base64 decode failure)
		logger.error(
			{ error: err.message },
			'Error during cookie file setup for yt-dlp info.'
		)
		// Ensure cleanup happens if handler was partially created
		if (cookieHandler) {
			await cookieHandler.cleanup()
		}
		throw err // Re-throw the error to be caught by the main job handler
	} finally {
		// Ensure cookie file is always deleted after the operation attempt
		if (cookieHandler) {
			await cookieHandler.cleanup()
		}
	}
}

async function streamAudioWithYtDlp(
	youtubeUrl: string,
	startTime: number,
	duration: number,
	base64Cookie?: string // Expects Base64 encoded cookie string
): Promise<Readable> {
	let cookieHandler: {
		cookieFilePath?: string
		cleanup: () => Promise<void>
	} | null = null

	try {
		// Pass the Base64 string from ENV var to the helper
		cookieHandler = await useCookieFile(base64Cookie, 'yt-dlp-stream')

		const args = [
			'--no-warnings',
			'--no-call-home',
			'--ignore-config',
			'-f',
			'bestaudio/best',
			'--output',
			'-',
			// '--force-ipv4', // Consider uncommenting if persistent network/auth issues on Cloud Run
			'--postprocessor-args',
			`ffmpeg:-ss ${startTime} -to ${startTime + duration}`,
			youtubeUrl
		]

		if (cookieHandler.cookieFilePath) {
			// Use the path to the file containing the *decoded* content
			args.unshift('--cookies', cookieHandler.cookieFilePath)
			logger.info(
				'Using temp cookie file (from decoded Base64) with yt-dlp stream command.'
			)
		} else {
			logger.warn(
				'No YouTube cookie provided (or decode failed) for yt-dlp stream command.'
			)
		}

		logger.info(
			`Spawning yt-dlp for audio segment: yt-dlp ${args.join(' ')}`
		)
		const ytDlpProcess = spawn('yt-dlp', args, {
			stdio: ['ignore', 'pipe', 'pipe']
		})

		const outputAudioStream = ytDlpProcess.stdout
		let stderrData = '' // Accumulate stderr

		// --- Stderr/Error/Close handlers remain largely the same, ---
		// --- but ensure cleanup happens in close/error ---

		ytDlpProcess.stderr.on('data', data => {
			const errLine = data.toString()
			stderrData += errLine
			// Filter noisy ffmpeg/download progress, but keep other warnings/errors
			if (
				!errLine.includes('WARNING:') &&
				!errLine.includes('[download]') &&
				!errLine.includes('Output stream #') &&
				!/frame=/.test(errLine) &&
				!/size=/.test(errLine) &&
				!/time=/.test(errLine) &&
				!/bitrate=/.test(errLine) &&
				!/speed=/.test(errLine) &&
				// Avoid logging the specific "Sign in..." error repeatedly
				!errLine.includes('Sign in to confirm') &&
				!errLine.includes('confirm you')
			) {
				logger.warn(`yt-dlp stream stderr: ${errLine.trim()}`)
			}
		})

		ytDlpProcess.on('error', err => {
			// Handle spawn errors
			logger.error(
				{ error: err, stderr: stderrData },
				'Failed to spawn yt-dlp process for streaming.'
			)
			// Emit error on the stream *before* trying cleanup
			outputAudioStream.emit(
				'error',
				new Error(
					`Failed to start yt-dlp stream process: ${err.message}`
				)
			)
			// Try cleanup immediately after emitting error
			if (cookieHandler) {
				cookieHandler.cleanup().catch(cleanupErr => {
					logger.warn(
						{ error: cleanupErr.message },
						'Error during cleanup after yt-dlp spawn error'
					)
				})
				cookieHandler = null // Prevent double cleanup
			}
		})

		ytDlpProcess.on('close', async code => {
			// --- Crucial: Cleanup cookie file after process closes ---
			if (cookieHandler) {
				await cookieHandler.cleanup()
				cookieHandler = null // Prevent double cleanup in finally
			}
			// --- End Cleanup ---

			if (code !== 0) {
				// Handle yt-dlp exit errors
				const detailedErrorMessage = `yt-dlp stream process exited with error code ${code}. Stderr: ${stderrData.substring(0, 1000)}`
				logger.error(detailedErrorMessage)
				let specificError = detailedErrorMessage
				// Detect common errors
				if (stderrData.includes('ModuleNotFoundError')) {
					specificError = `yt-dlp execution failed (ModuleNotFoundError). Check container setup. Code ${code}.`
				} else if (
					stderrData.includes('Sign in to confirm') ||
					stderrData.includes('confirm you') || // Bot check
					stderrData.includes('login required') ||
					stderrData.includes('403 Forbidden')
				) {
					specificError = `yt-dlp download failed (Authentication Error - e.g., 403/Login/Bot Check). Check cookie validity/freshness/completeness or IP reputation. Code ${code}.`
				} else if (
					stderrData.includes('Socket error') ||
					stderrData.includes('timed out')
				) {
					specificError = `yt-dlp download failed (Network/Socket error). Code ${code}.`
				}
				// Check for cookie decoding failure message
				else if (
					stderrData.includes('Failed to decode Base64 cookie')
				) {
					specificError = `Cookie handling error: Failed to decode Base64 cookie provided in environment variable. Code ${code}.`
				}
				outputAudioStream.emit('error', new Error(specificError))
			} else {
				logger.info('yt-dlp stream process finished successfully.')
				// Note: 'end' event on the stream signals completion of data transfer
			}
		})

		// Handle errors emitted directly *by* the stream (less common for yt-dlp stdout)
		outputAudioStream.on('error', async err => {
			logger.error(
				{ error: err },
				'Error emitted directly on yt-dlp output stream.'
			)
			// Ensure cleanup happens if the stream errors *before* the process closes
			if (cookieHandler) {
				await cookieHandler.cleanup()
				cookieHandler = null
			}
		})

		return outputAudioStream
	} catch (error: any) {
		// Catch errors from useCookieFile (e.g., Base64 decode failure)
		logger.error(
			{ error: error.message },
			'Error setting up yt-dlp stream (e.g., cookie file creation/decode failed)'
		)
		if (cookieHandler) {
			await cookieHandler.cleanup()
		}
		// Return a stream that immediately emits the error
		const errorStream = new Readable({
			read() {
				this.emit('error', error)
				this.push(null) // End the stream
			}
		})
		return errorStream
	}
}

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

// --- Main Transcription Job Logic ---

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

	// --- Fetch Base64 Encoded Cookie from Environment Variable ---
	// IMPORTANT: The YOUTUBE_COOKIE env var MUST contain the Base64 encoded
	//            content of a *valid* and *fresh* Netscape-formatted cookie file.
	const base64YoutubeCookie = process.env.YOUTUBE_COOKIE
	// ---

	try {
		// Log presence and length (safer than logging content)
		if (base64YoutubeCookie) {
			jobLogger.info(
				`Found YOUTUBE_COOKIE environment variable (length: ${base64YoutubeCookie.length}). Expecting Base64 encoded Netscape format.`
			)
		} else {
			jobLogger.warn(
				'YOUTUBE_COOKIE environment variable is not set. yt-dlp might fail for private/restricted videos.'
			)
			// Decide if this is a fatal error for your use case:
			// await pushTranscriptionEvent(jobId, 'Server konfiguratsiya xatosi: Cookie topilmadi.', true, broadcast);
			// await transcriptService.error(jobId);
			// return;
		}

		await transcriptService.running(jobId)
		jobStatusUpdated = true
		await delay(1000)

		// --- Get Video Info (Pass Base64 Cookie) ---
		let videoInfo: VideoInfo
		try {
			jobLogger.info(`Fetching video info via yt-dlp for URL: ${url}`)
			videoInfo = await getVideoInfoWithYtDlp(url, base64YoutubeCookie)
			jobLogger.info(
				`Successfully fetched video info for title: ${videoInfo.title}`
			)
		} catch (err: any) {
			jobLogger.error(
				{ error: err.message, stack: err.stack, url: url },
				'Failed to get video info from yt-dlp.'
			)
			// Refine error message for the user based on the specific yt-dlp error
			let errorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). URL, server yoki cookie'ni tekshiring. (${err.message || 'Unknown yt-dlp info error'})`
			if (
				err.message?.includes('YouTube access error') ||
				err.message?.includes('Authentication failed')
			) {
				errorMessage = `Video ma'lumotlarini olib bo'lmadi (yt-dlp). YouTube kirish xatosi (maxfiy/yosh/bot tekshiruvi?) yoki cookie yaroqsiz/to'liq emas. Cookie'ni yangilang. (${err.message})`
			} else if (err.message?.includes('ModuleNotFoundError')) {
				errorMessage = `Server xatosi: yt-dlp ishga tushmadi (ModuleNotFoundError). (${err.message})`
			} else if (
				err.message?.includes('cookie file') ||
				err.message?.includes('cookie handling error')
			) {
				errorMessage = `Server xatosi: Cookie faylini yozib/o'qib bo'lmadi. (${err.message})`
			} else if (err.message?.includes('Failed to decode Base64')) {
				errorMessage = `Server xatosi: Taqdim etilgan cookie (Base64) noto'g'ri formatda. (${err.message})`
			}

			await pushTranscriptionEvent(jobId, errorMessage, true, broadcast)
			await transcriptService.error(jobId)
			jobStatusUpdated = true
			return
		}
		// --- End Get Video Info ---

		// --- Title Update, Segment Calculation (No changes needed) ---
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
				'Failed to update job title.'
			)
		}
		await pushTranscriptionEvent(
			jobId,
			'Ovoz yuklanmoqda (yt-dlp)...',
			false,
			broadcast
		)
		await delay(500)
		const segmentDuration = 150
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
			jobLogger.error('Missing GOOGLE_CLOUD_BUCKET_NAME env var.')
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
		// --- End Setup ---

		// --- Segment Processing Loop ---
		while (i < numSegments) {
			const segmentNumber = i + 1
			const segmentStartTime = i * segmentDuration
			const actualDuration = Math.min(
				segmentDuration,
				totalDuration - segmentStartTime
			)
			const safeActualDuration = Math.max(0.1, actualDuration)
			const destFileName = `segment_${jobId}_${segmentNumber}.mp3`
			const gcsUri = `gs://${bucketName}/${destFileName}`
			let segmentProcessedSuccessfully = false
			let attempt = 0
			const maxAttempts = 2

			while (!segmentProcessedSuccessfully && attempt < maxAttempts) {
				attempt++
				const segmentLogger = jobLogger.child({
					segment: segmentNumber,
					attempt
				})
				let gcsUploadSucceeded = false

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
					await delay(1500 * attempt) // Exponential backoff-ish
				}

				try {
					await pushTranscriptionEvent(
						jobId,
						`Bo'lak ${segmentNumber}/${numSegments} yuklanmoqda (yt-dlp)...`,
						false,
						broadcast
					)

					// --- Stream Audio Segment (Pass Base64 Cookie) ---
					segmentLogger.info(
						`Attempting segment download via yt-dlp (start: ${segmentStartTime}s, duration: ${safeActualDuration}s)...`
					)
					const audioStream = await streamAudioWithYtDlp(
						url,
						segmentStartTime,
						safeActualDuration,
						base64YoutubeCookie // Pass the Base64 cookie
					)

					// --- FFmpeg and Upload Promise (No changes needed) ---
					segmentLogger.info(`Starting FFmpeg encoding...`)
					const ffmpegCommand = ffmpeg(audioStream)
						.format('mp3')
						.audioCodec('libmp3lame')
						.audioBitrate('96k')
						.on('start', cmd =>
							segmentLogger.info(`FFmpeg started: ${cmd}`)
						)
						.on('error', (err, stdout, stderr) => {
							segmentLogger.error(
								{ message: err.message, stdout, stderr },
								`FFmpeg error event (command level)`
							)
						})
						.on('end', () =>
							segmentLogger.info(
								`FFmpeg processing seemingly finished.`
							)
						)

					await new Promise<void>((resolve, reject) => {
						// ... Identical promise logic as before ...
						const ffmpegOutputStream = ffmpegCommand.pipe()
						let promiseRejected = false
						audioStream.on('error', inputError => {
							// Handle yt-dlp stream errors
							if (promiseRejected) return
							promiseRejected = true
							segmentLogger.error(
								{ error: inputError.message },
								'Error on yt-dlp input stream for ffmpeg'
							)
							try {
								ffmpegCommand.kill('SIGKILL')
							} catch (killErr: any) {
								/* ignore */
							}
							reject(
								new Error(
									`Input stream error: ${inputError.message}`
								)
							)
						})
						ffmpegCommand.on('error', err => {
							// Handle ffmpeg command errors
							if (promiseRejected) return
							promiseRejected = true
							reject(
								new Error(
									`FFmpeg command failed directly: ${err.message}`
								)
							)
						})
						ffmpegOutputStream.on('error', outputError => {
							// Handle pipe/upload stream errors
							if (promiseRejected) return
							promiseRejected = true
							segmentLogger.error(
								{ error: outputError.message },
								'Error on ffmpeg output stream during upload'
							)
							reject(
								new Error(
									`FFmpeg output stream error: ${outputError.message}`
								)
							)
						})
						uploadStreamToGCS(ffmpegOutputStream, destFileName) // Pipe to GCS
							.then(() => {
								if (!promiseRejected) {
									gcsUploadSucceeded = true
									segmentLogger.info(
										`Segment encoded and uploaded to ${gcsUri}`
									)
									resolve()
								} else {
									segmentLogger.warn(
										'GCS upload finished, but an earlier error occurred.'
									)
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
					// --- End FFmpeg / Upload ---

					// --- Transcriptions & Editing (No changes needed) ---
					await pushTranscriptionEvent(
						jobId,
						`Google matnni o'girmoqda ${segmentNumber}/${numSegments}...`,
						false,
						broadcast
					)
					const transcriptGoogle = await transcribeWithGoogle(gcsUri)
					if (!transcriptGoogle) {
						segmentLogger.error(`Google transcription empty`)
						await pushTranscriptionEvent(
							jobId,
							`${segmentNumber}/${numSegments}-chi Google matnida xatolik (bo'sh natija).`,
							false,
							broadcast
						) // Don't retry endlessly for this
						throw new Error(
							'Google transcription returned empty result.'
						) // Treat as segment failure
					}
					segmentLogger.info(`Google transcription done.`)
					// --- ElevenLabs ---
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
						if (!transcriptElevenLabs) {
							throw new Error('ElevenLabs returned empty result.')
						}
						segmentLogger.info(`ElevenLabs transcription done.`)
					} catch (elevenLabsError: any) {
						segmentLogger.error(
							{ error: elevenLabsError.message },
							`ElevenLabs transcription failed`
						)
						await pushTranscriptionEvent(
							jobId,
							`${segmentNumber}/${numSegments}-chi ElevenLabs matnida xatolik (${elevenLabsError.message}).`,
							false,
							broadcast
						)
						throw elevenLabsError // Treat as segment failure
					}
					// --- Gemini Edit ---
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
						segmentLogger.error(`Gemini editing returned empty`)
						await pushTranscriptionEvent(
							jobId,
							`${segmentNumber}/${numSegments}-chi Gemini tahririda xatolik (bo'sh natija).`,
							false,
							broadcast
						)
						throw new Error('Gemini editing returned empty result.') // Treat as segment failure
					}
					segmentLogger.info(`Gemini editing done.`)
					// --- End Transcriptions & Editing ---

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
					// Prioritize unrecoverable errors like auth or setup failures
					let isFatal = false
					let fatalUserMsg = ''

					if (
						segmentErr.message?.includes('Input stream error:') || // Error reading from yt-dlp stream
						segmentErr.message?.includes('Authentication Error') || // Specific yt-dlp auth error
						segmentErr.message?.includes('login required') ||
						segmentErr.message?.includes('Sign in to confirm') ||
						segmentErr.message?.includes('confirm you') ||
						segmentErr.message?.includes('403 Forbidden') ||
						segmentErr.message?.includes('ModuleNotFoundError') || // yt-dlp didn't run
						segmentErr.message?.includes(
							'Failed to start yt-dlp'
						) || // Spawn failed
						segmentErr.message?.includes(
							'cookie file creation failed'
						) || // Error writing temp file
						segmentErr.message?.includes(
							'Failed to decode Base64 cookie'
						) // Error decoding cookie
					) {
						isFatal = true
						segmentLogger.error(
							'Fatal yt-dlp/stream/cookie related error occurred. Aborting job.'
						)
						if (
							segmentErr.message?.includes(
								'Authentication Error'
							) ||
							segmentErr.message?.includes('Sign in')
						) {
							fatalUserMsg = `YouTube yuklashda/kirishda xatolik (yt-dlp ${segmentNumber}/${numSegments}). Cookie yaroqsiz, to'liq emas yoki IP bloklangan bo'lishi mumkin. Cookie'ni yangilang. Jarayon to'xtatildi.`
						} else if (
							segmentErr.message?.includes(
								'ModuleNotFoundError'
							) ||
							segmentErr.message?.includes(
								'Failed to start yt-dlp'
							)
						) {
							fatalUserMsg = `Server xatosi: yt-dlp ishga tushmadi (${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`
						} else if (
							segmentErr.message?.includes('cookie file') ||
							segmentErr.message?.includes('decode')
						) {
							fatalUserMsg = `Server xatosi: Cookie faylini sozlab/o'qib bo'lmadi (${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`
						} else {
							fatalUserMsg = `YouTube yuklashda/kirishda noma'lum xatolik (yt-dlp ${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`
						}
					} else if (
						segmentErr.message?.includes('FFmpeg command failed')
					) {
						isFatal = true
						segmentLogger.error(
							'Fatal FFmpeg error occurred. Aborting job.'
						)
						fatalUserMsg = `Audio kodlashda xatolik (FFmpeg ${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`
					} else if (
						segmentErr.message?.includes('GCS upload failed')
					) {
						isFatal = true
						segmentLogger.error(
							'Fatal GCS upload error occurred. Aborting job.'
						)
						fatalUserMsg = `Audio bo'lakni saqlashda xatolik (GCS ${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`
					}
					// Treat transcription/editing errors as fatal for the segment now
					else if (
						segmentErr.message?.includes(
							'transcription returned empty'
						) ||
						segmentErr.message?.includes(
							'ElevenLabs returned empty'
						) ||
						segmentErr.message?.includes(
							'Gemini editing returned empty'
						) ||
						segmentErr.message?.includes(
							'ElevenLabs transcription failed'
						)
					) {
						isFatal = true
						segmentLogger.error(
							`Fatal transcription/editing error: ${segmentErr.message}. Aborting job.`
						)
						fatalUserMsg = `Matnni o'girishda/tahrirlashda xatolik (${segmentNumber}/${numSegments}: ${segmentErr.message.substring(0, 80)}). Jarayon to'xtatildi.`
					}

					// If fatal, abort the job
					if (isFatal) {
						await pushTranscriptionEvent(
							jobId,
							fatalUserMsg,
							true,
							broadcast
						)
						// Throw to exit the main try block of the job
						throw new Error(
							`Aborting job due to fatal error on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}

					// If not fatal, allow retry loop to continue after delay
					await delay(2000 + attempt * 1500) // Slightly longer delay
				} finally {
					// --- Cleanup GCS File (No changes needed) ---
					if (gcsUploadSucceeded) {
						try {
							if (!destFileName) {
								segmentLogger.error(
									'destFileName empty before GCS delete!'
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
							segmentLogger.error(
								{
									error: deleteErr.message,
									file: destFileName
								},
								`Failed to delete GCS segment file: ${destFileName}. Continuing.`
							)
						}
					} else {
						if (destFileName) {
							segmentLogger.info(
								`Skipping GCS delete for ${destFileName} (upload failed or error occurred).`
							)
						}
					}
					await delay(300)
				}
			} // End retry loop

			// If segment failed after all attempts (should only happen if non-fatal errors persisted)
			if (!segmentProcessedSuccessfully) {
				// This path should ideally not be reached if we treat most errors as fatal now
				jobLogger.error(
					`Segment ${segmentNumber}/${numSegments} failed after ${maxAttempts} attempts. Aborting job.`
				)
				await pushTranscriptionEvent(
					jobId,
					`Xatolik: ${segmentNumber}/${numSegments}-chi bo'lakni ${maxAttempts} urinishda ham ishlab bo'lmadi. Jarayon to'xtatildi.`,
					true,
					broadcast
				)
				throw new Error(
					`Failed to process segment ${segmentNumber} after ${maxAttempts} attempts.`
				)
			}

			i++ // Move to the next segment
		} // End segment loop

		// --- Combine and Finalize (No changes needed) ---
		jobLogger.info(`All ${numSegments} segments processed. Combining...`)
		try {
			await userSession.completed(sessionId)
			jobLogger.info(`Marked session ${sessionId} completed.`)
		} catch (err: any) {
			jobLogger.warn(
				{ error: err.message, sessionId },
				`Could not mark session completed`
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
		const finalTranscript = `<i style="display: block; font-style: italic; text-align: center;">🕒 Transkripsiya uchun ${formatDuration(duration)} vaqt ketdi.</i><h1 style="font-weight: 700; font-size: 1.8rem; margin: 1rem 0; text-align: center; line-height: 1;">${finalTitle}</h1>\n\n<p style="text-indent: 30px;">${convertToUzbekLatin(combinedResult)}</p>`
		await transcriptService.saveFinalTranscript(jobId, finalTranscript)
		jobLogger.info(`Final transcript saved.`)
		await pushTranscriptionEvent(jobId, finalTranscript, true, broadcast)
		jobStatusUpdated = true
	} catch (err: any) {
		// --- Final Error Handling ---
		jobLogger.error(
			{ error: err.message, stack: err.stack },
			'Critical error in runTranscriptionJob'
		)
		if (!jobStatusUpdated) {
			try {
				await transcriptService.error(jobId)
				jobStatusUpdated = true
			} catch (dbErr: any) {
				jobLogger.error(
					{ error: dbErr.message },
					'Failed to mark job as error in final catch block'
				)
			}
		}
		if (broadcast) {
			try {
				// Use the message thrown by the fatal error logic if available
				let clientErrorMessage = `Serverda kutilmagan xatolik: (${err.message?.substring(0, 100) || 'No details'}...)`
				if (
					err.message?.startsWith('Aborting job due to fatal error')
				) {
					// Extract the user-facing message we prepared earlier if possible
					const nestedError = err.message
						.split(': ')
						.slice(1)
						.join(': ')
					// Re-check the nested error message for keywords
					if (
						nestedError.includes('Authentication Error') ||
						nestedError.includes('Sign in')
					) {
						clientErrorMessage = `YouTube yuklashda/kirishda xatolik. Cookie yaroqsiz, to'liq emas yoki IP bloklangan bo'lishi mumkin. Cookie'ni yangilang. Jarayon to'xtatildi.`
					} else if (nestedError.includes('FFmpeg')) {
						clientErrorMessage = `Audio kodlashda xatolik (FFmpeg). Jarayon to'xtatildi.`
					} else if (nestedError.includes('GCS upload')) {
						clientErrorMessage = `Audio bo'lakni saqlashda xatolik (GCS). Jarayon to'xtatildi.`
					} else if (
						nestedError.includes('cookie file') ||
						nestedError.includes('decode')
					) {
						clientErrorMessage = `Server xatosi: Cookie faylini sozlab/o'qib bo'lmadi. Jarayon to'xtatildi.`
					} else if (
						nestedError.includes('transcription') ||
						nestedError.includes('ElevenLabs') ||
						nestedError.includes('Gemini')
					) {
						clientErrorMessage = `Matnni o'girishda/tahrirlashda xatolik. Jarayon to'xtatildi. (${nestedError.substring(0, 80)})`
					}
					// Fallback to the thrown error message if none of the above match well
					else {
						clientErrorMessage = err.message
					}
				} else if (
					err.message?.includes("Video ma'lumotlarini olib bo'lmadi")
				) {
					// Error during initial info fetch
					clientErrorMessage = err.message // Use the message already prepared
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
	}
}
