import { ChildProcess, spawn } from 'child_process'
// Import ChildProcess type
import ffmpeg from 'fluent-ffmpeg'
import os from 'os'
import path from 'path'
import { performance } from 'perf_hooks'
import { Readable } from 'stream'
import { v4 as uuidv4 } from 'uuid'

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
	logPrefix: string = 'yt-dlp'
): Promise<{ cookieFilePath?: string; cleanup: () => Promise<void> }> {
	if (!cookieValue) {
		logger.info(
			`${logPrefix}: No cookie value provided, skipping file creation.`
		)
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
			if (err.code !== 'ENOENT') {
				logger.warn(
					{ error: err.message, file: cookieFilePath },
					`${logPrefix}: Failed to delete temp cookie file.`
				)
			} else {
				logger.info(
					`${logPrefix}: Temp cookie file already deleted or never created: ${cookieFilePath}`
				)
			}
		}
	}

	try {
		const sanitizedCookieValue = cookieValue.trimEnd() + '\n'
		await fs.writeFile(cookieFilePath, sanitizedCookieValue, {
			encoding: 'utf-8',
			mode: 0o600
		})
		logger.info(
			`${logPrefix}: Successfully created temp cookie file: ${cookieFilePath}`
		)
		return { cookieFilePath, cleanup }
	} catch (err: any) {
		logger.error(
			{ error: err.message, file: cookieFilePath },
			`${logPrefix}: Failed to create temp cookie file.`
		)
		await cleanup()
		throw new Error(`Failed to write cookie file: ${err.message}`)
	}
}

// --- Helper Functions for yt-dlp (Modified) ---
interface VideoInfo {
	title: string
	duration: number // in seconds
}

async function getVideoInfoWithYtDlp(
	youtubeUrl: string,
	cookie?: string // Cookie content string
): Promise<VideoInfo> {
	let cookieHandler: {
		cookieFilePath?: string
		cleanup: () => Promise<void>
	} | null = null

	try {
		cookieHandler = await useCookieFile(cookie, 'yt-dlp-info')

		const args = [
			'--no-warnings',
			'--no-call-home',
			'--ignore-config',
			'--dump-json',
			'--skip-download',
			'--force-ipv4',
			youtubeUrl
		]

		if (cookieHandler.cookieFilePath) {
			logger.info(
				`yt-dlp-info: Using cookie file: ${cookieHandler.cookieFilePath}`
			)
			args.push('--cookies', cookieHandler.cookieFilePath)
		} else {
			logger.info('yt-dlp-info: No cookie file provided or created.')
		}

		logger.info(
			`Spawning yt-dlp to get video info: yt-dlp ${args.join(' ')}`
		)

		return await new Promise<VideoInfo>((resolve, reject) => {
			const ytDlpProcess: ChildProcess = spawn('yt-dlp', args) // Explicitly type if needed
			let jsonData = ''
			let errorData = ''

			ytDlpProcess.stdout?.on('data', data => {
				// Use optional chaining
				jsonData += data.toString()
			})
			ytDlpProcess.stderr?.on('data', data => {
				// Use optional chaining
				const errLine = data.toString()
				errorData += errLine
				if (!errLine.includes('WARNING:')) {
					logger.warn(`yt-dlp info stderr: ${errLine.trim()}`)
				}
			})
			ytDlpProcess.on('error', err => {
				logger.error(
					{ error: err },
					'Failed to spawn yt-dlp process for info.'
				)
				// Cleanup attempt on spawn error
				cookieHandler
					?.cleanup()
					.catch(cleanupErr => {
						logger.warn(
							{ error: cleanupErr },
							'Error during cleanup in yt-dlp info spawn error handler'
						)
					})
					.finally(() => {
						cookieHandler = null
					})
				reject(
					new Error(`Failed to start yt-dlp for info: ${err.message}`)
				)
			})
			ytDlpProcess.on('close', code => {
				cookieHandler
					?.cleanup()
					.catch(cleanupErr => {
						logger.warn(
							{ error: cleanupErr },
							'Error during cleanup in yt-dlp info close handler'
						)
					})
					.finally(() => {
						cookieHandler = null
					})

				if (code !== 0) {
					logger.error(
						`yt-dlp info process exited with code ${code}. Stderr: ${errorData}`
					)
					let specificError = `yt-dlp info process exited with code ${code}.`
					if (
						errorData.includes('Private video') ||
						errorData.includes('login required') ||
						errorData.includes('confirm your age') ||
						errorData.includes('unavailable') ||
						errorData.includes('Sign in') ||
						errorData.includes('consent') ||
						errorData.includes('403') ||
						errorData.includes('401') ||
						errorData.includes('Premiere') ||
						errorData.includes('confirm you')
					) {
						specificError = `YouTube access error (yt-dlp info): Video might be private/unavailable/premiere, require login/age confirmation, or cookie invalid/expired/rejected. Code ${code}.`
					} else if (errorData.includes('ModuleNotFoundError')) {
						specificError = `yt-dlp execution failed (ModuleNotFoundError). Ensure Python environment and yt-dlp installation are correct. Code ${code}.`
					} else if (
						errorData.includes('cookie file not found') &&
						cookieHandler?.cookieFilePath
					) {
						specificError = `yt-dlp could not find the provided cookie file (${cookieHandler.cookieFilePath}). Code ${code}.`
					} else if (
						errorData.includes(
							'ERROR: unable to download video data'
						)
					) {
						specificError = `yt-dlp info failed: Unable to download video data. Check URL, network, and cookies. Code ${code}.`
					}
					reject(
						new Error(
							`${specificError} Stderr: ${errorData.substring(0, 500)}`
						)
					)
				} else {
					try {
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
							{
								error: parseErr,
								rawJson: jsonData.substring(0, 500),
								stderr: errorData.substring(0, 500)
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
	} catch (setupError: any) {
		logger.error(
			{ error: setupError.message },
			'Error setting up yt-dlp info call (e.g., cookie file creation)'
		)
		await cookieHandler?.cleanup().catch(cleanupErr => {
			logger.warn(
				{ error: cleanupErr },
				'Error during cleanup after setup error in yt-dlp info'
			)
		})
		throw setupError
	}
}

async function streamAudioWithYtDlp(
	youtubeUrl: string,
	startTime: number,
	duration: number,
	cookie?: string // Cookie content string
): Promise<Readable> {
	let cookieHandler: {
		cookieFilePath?: string
		cleanup: () => Promise<void>
	} | null = null
	let ytDlpProcess: ChildProcess | null = null // Keep track of the process

	try {
		cookieHandler = await useCookieFile(cookie, 'yt-dlp-stream')

		const args = [
			'--no-warnings',
			'--no-call-home',
			'--ignore-config',
			'-f',
			'bestaudio/best',
			'--output',
			'-',
			'--force-ipv4',
			'--postprocessor-args',
			`"ffmpeg_i:-ss ${startTime} -to ${startTime + duration}"`, // Keep quoting for safety
			youtubeUrl
		]

		if (cookieHandler.cookieFilePath) {
			logger.info(
				`yt-dlp-stream: Using cookie file: ${cookieHandler.cookieFilePath}`
			)
			// Insert cookie args *before* postprocessor args
			const ppArgsIndex = args.indexOf('--postprocessor-args')
			if (ppArgsIndex > -1) {
				args.splice(
					ppArgsIndex,
					0,
					'--cookies',
					cookieHandler.cookieFilePath
				)
			} else {
				// Fallback if postprocessor args somehow aren't there
				args.splice(
					args.indexOf('--force-ipv4') + 1,
					0,
					'--cookies',
					cookieHandler.cookieFilePath
				)
			}
		} else {
			logger.info('yt-dlp-stream: No cookie file provided or created.')
		}

		logger.info(
			`Spawning yt-dlp for audio segment: yt-dlp ${args.map(arg => (arg.includes(' ') ? `"${arg}"` : arg)).join(' ')}`
		)

		ytDlpProcess = spawn('yt-dlp', args, {
			stdio: ['ignore', 'pipe', 'pipe']
		})

		// Ensure stdout/stderr exist before attaching listeners
		if (!ytDlpProcess.stdout || !ytDlpProcess.stderr) {
			const errMsg = 'Failed to get stdout or stderr from yt-dlp process.'
			logger.error(errMsg)
			// Cleanup attempt
			await cookieHandler?.cleanup().catch(cleanupErr => {
				logger.warn(
					{ error: cleanupErr },
					'Error during cleanup after missing stdio'
				)
			})
			throw new Error(errMsg)
		}

		const outputAudioStream = ytDlpProcess.stdout
		let stderrData = ''

		ytDlpProcess.stderr.on('data', data => {
			const errLine = data.toString()
			stderrData += errLine
			// Noise filtering... (keep as is)
			if (
				!errLine.includes('WARNING:') &&
				!/\[info\] Extracting URL:/.test(errLine) &&
				!/\[youtube\] Extracting URL:/.test(errLine) &&
				!/\[youtube\] .*? page: Downloading webpage/.test(errLine) &&
				!/\[youtube\] .*? page: Downloading android player API JSON/.test(
					errLine
				) &&
				!/\[download\] Destination: -/.test(errLine) &&
				!/\[download\] .*? has already been downloaded/.test(errLine) &&
				!/\[ExtractAudio\] Destination:/.test(errLine) &&
				!/Deleting original file/.test(errLine) &&
				!/\[ffmpeg\] Destination:/.test(errLine) &&
				!/Output stream #/.test(errLine) &&
				!/frame=/.test(errLine) &&
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
			cookieHandler
				?.cleanup()
				.catch(cleanupErr => {
					logger.warn(
						{ error: cleanupErr },
						'Error during cleanup in yt-dlp stream spawn error handler'
					)
				})
				.finally(() => {
					cookieHandler = null
				})

			// Emit error on the stream *if* it hasn't already ended/errored
			if (!outputAudioStream.destroyed) {
				outputAudioStream.emit(
					'error',
					new Error(
						`Failed to start yt-dlp stream process: ${err.message}`
					)
				)
				// Ensure stream is destroyed on spawn error
				outputAudioStream.destroy()
			}
		})

		ytDlpProcess.on('close', async code => {
			if (cookieHandler) {
				try {
					await cookieHandler.cleanup()
				} catch (cleanupErr) {
					logger.warn(
						{ error: cleanupErr },
						'Error during cleanup in yt-dlp stream close handler'
					)
				} finally {
					cookieHandler = null
				}
			}

			if (code !== 0) {
				let specificError = `yt-dlp stream process exited with error code ${code}.`
				if (stderrData.includes('ModuleNotFoundError')) {
					specificError = `yt-dlp execution failed (ModuleNotFoundError). Check container setup. Code ${code}.`
				} else if (
					stderrData.includes('403 Forbidden') ||
					stderrData.includes('401 Unauthorized') ||
					stderrData.includes('Sign in') ||
					stderrData.includes('confirm you') ||
					stderrData.includes('consent') ||
					stderrData.includes('login required')
				) {
					specificError = `yt-dlp download failed (Authentication/Authorization Error - 403/401/Login/Bot/Consent?). Check cookie validity/freshness. Code ${code}.`
				} else if (
					stderrData.includes('Socket error') ||
					stderrData.includes('timed out')
				) {
					specificError = `yt-dlp download failed (Network/Socket/Timeout error). Code ${code}.`
				} else if (stderrData.includes('Video unavailable')) {
					specificError = `yt-dlp download failed (Video unavailable). Code ${code}.`
				} else if (stderrData.includes('Private video')) {
					specificError = `yt-dlp download failed (Private video). Code ${code}.`
				} else if (
					stderrData.includes('Postprocessing:') &&
					stderrData.includes('ffmpeg exited with status')
				) {
					specificError = `yt-dlp postprocessing failed (ffmpeg error during segmenting?). Code ${code}.`
				}

				logger.error(
					`${specificError} Stderr: ${stderrData.substring(0, 1000)}`
				)
				// Emit error on the stream *if* it hasn't already ended/errored
				if (!outputAudioStream.destroyed) {
					outputAudioStream.emit('error', new Error(specificError))
					outputAudioStream.destroy() // Ensure stream ends on error
				}
			} else {
				logger.info('yt-dlp stream process finished successfully.')
				// The stream will end naturally when yt-dlp closes stdout
			}
		})

		outputAudioStream.on('error', async err => {
			logger.error(
				{ error: err.message }, // Log only message for potentially repetitive errors
				'Error emitted directly on yt-dlp output stream.'
			)
			if (cookieHandler) {
				try {
					await cookieHandler.cleanup()
				} catch (cleanupErr) {
					logger.warn(
						{ error: cleanupErr },
						"Error during cleanup in yt-dlp stream 'error' event handler"
					)
				} finally {
					cookieHandler = null
				}
			}
			// Ensure the process is killed if the stream errors out and process still exists
			if (ytDlpProcess && ytDlpProcess.pid && !ytDlpProcess.killed) {
				logger.warn(
					'Killing yt-dlp process due to output stream error.'
				)
				ytDlpProcess.kill('SIGKILL')
			}
		})

		outputAudioStream.on('end', () => {
			if (
				stderrData.trim().length > 0 &&
				!stderrData.includes('download completed') &&
				!stderrData.includes('Postprocessing finished') &&
				!stderrData.includes('already been downloaded')
			) {
				logger.warn(
					'yt-dlp output stream ended, possibly prematurely. Check stderr logs.'
				)
			} else {
				logger.info('yt-dlp output stream ended.')
			}
		})

		return outputAudioStream
	} catch (error: any) {
		logger.error(
			{ error: error.message },
			'Error setting up yt-dlp stream (e.g., cookie file creation failed)'
		)
		if (cookieHandler) {
			await cookieHandler.cleanup().catch(cleanupErr => {
				logger.warn(
					{ error: cleanupErr },
					'Error during cleanup after setup error in yt-dlp stream'
				)
			})
		}
		const errorStream = new Readable({
			read() {
				// Defer emitting the error slightly to allow listeners to attach
				process.nextTick(() => {
					if (!this.destroyed) {
						this.emit('error', error)
						this.push(null)
						this.destroy() // Ensure stream is destroyed
					}
				})
			},
			destroy(err, callback) {
				// Optional: Handle stream destruction cleanup if needed
				callback(err)
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
	if (!completed || message.length < 500) {
		logger.info(
			{ jobId, eventContent: message, completed },
			'Pushing transcription event'
		)
	} else {
		logger.info(
			{
				jobId,
				eventContent: message.substring(0, 200) + '...',
				completed
			},
			'Pushing final transcription event (truncated)'
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

	if (youtubeCookie) {
		jobLogger.info(
			`Found YOUTUBE_COOKIE environment variable (length: ${youtubeCookie.length}). Will attempt to use it.`
		)
	} else {
		jobLogger.warn(
			'YOUTUBE_COOKIE environment variable not set. Transcription may fail for private/restricted videos.'
		)
	}

	try {
		await transcriptService.running(jobId)
		jobStatusUpdated = true
		await delay(1000)

		// --- Get Video Info ---
		let videoInfo: VideoInfo
		jobLogger.info(`Fetching video info via yt-dlp for URL...`)

		try {
			videoInfo = await getVideoInfoWithYtDlp(url, youtubeCookie)
			jobLogger.info(
				`Successfully fetched video info via yt-dlp for title: ${videoInfo.title}`
			)
		} catch (err: any) {
			jobLogger.error(
				{ error: err.message, stack: err.stack },
				'Failed to get video info from yt-dlp.'
			)
			let errorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). URL, server yoki cookie'ni tekshiring. (${err.message || 'Unknown yt-dlp info error'})`
			if (err.message?.includes('YouTube access error')) {
				errorMessage = `Video ma'lumotlarini olib bo'lmadi (yt-dlp). YouTube kirish xatosi (maxfiy/mavjud emas/yosh/bot tekshiruvi/cookie yaroqsiz?). (${err.message})`
			} else if (err.message?.includes('ModuleNotFoundError')) {
				errorMessage = `Server xatosi: yt-dlp ishga tushmadi (ModuleNotFoundError). (${err.message})`
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
			'Ovoz yuklanmoqda (yt-dlp)...',
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
				let ffmpegCommand: ffmpeg.FfmpegCommand | null = null // Hold ffmpeg instance

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
						`Bo'lak ${segmentNumber}/${numSegments} yuklanmoqda (yt-dlp)...`,
						false,
						broadcast
					)

					segmentLogger.info(
						`Attempting segment download via yt-dlp...`
					)
					const audioStream = await streamAudioWithYtDlp(
						url,
						segmentStartTime,
						safeActualDuration,
						youtubeCookie
					)

					segmentLogger.info(`Starting FFmpeg encoding...`)
					// Assign to outer scope variable
					ffmpegCommand = ffmpeg(audioStream) // Initialize here
						.format('mp3')
						.audioCodec('libmp3lame')
						.audioBitrate('96k')
						.on('start', cmd =>
							segmentLogger.info(`FFmpeg started: ${cmd}`)
						)
						.on('error', (err, stdout, stderr) => {
							segmentLogger.error(
								{ message: err.message, stdout, stderr },
								`FFmpeg error event processing segment (command level)`
							)
						})
						.on('end', () => {
							segmentLogger.info(
								`FFmpeg processing seemingly finished (stream ended).`
							)
						})

					// --- Wrap ffmpeg processing and upload in a Promise ---
					await new Promise<void>((resolve, reject) => {
						// Ensure ffmpegCommand is defined before piping
						if (!ffmpegCommand) {
							return reject(
								new Error('FFmpeg command was not initialized.')
							)
						}
						const ffmpegOutputStream = ffmpegCommand.pipe()
						let promiseRejected = false

						// Handle input stream errors (yt-dlp)
						audioStream.on('error', inputError => {
							if (promiseRejected) return
							promiseRejected = true
							segmentLogger.error(
								{ error: inputError.message },
								'Error emitted on yt-dlp input stream for ffmpeg'
							)
							try {
								// *** FIX: Use ffmpegCommand.kill() directly ***
								if (ffmpegCommand) {
									// Check if command exists
									segmentLogger.warn(
										'Killing ffmpeg due to input stream error.'
									)
									ffmpegCommand.kill('SIGKILL')
								}
							} catch (killErr: any) {
								segmentLogger.warn(
									{ error: killErr.message },
									'Error trying to kill ffmpeg after input stream error (kill method)'
								)
							}
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
							// Error details already logged by the other listener
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
							// Attempt to kill ffmpeg on output error too
							try {
								if (ffmpegCommand) {
									segmentLogger.warn(
										'Killing ffmpeg due to output stream error.'
									)
									ffmpegCommand.kill('SIGKILL')
								}
							} catch (killErr: any) {
								segmentLogger.warn(
									{ error: killErr.message },
									'Error trying to kill ffmpeg after output stream error (kill method)'
								)
							}
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
									gcsUploadSucceeded = true
									segmentLogger.info(
										`Segment successfully encoded and uploaded to ${gcsUri}`
									)
									resolve()
								} else {
									segmentLogger.warn(
										'GCS upload technically finished, but an error occurred earlier. Treating as failure.'
									)
									gcsUploadSucceeded = false // Ensure it's marked false
									// Do not resolve, let the existing rejection handle it.
								}
							})
							.catch(uploadErr => {
								if (promiseRejected) return
								promiseRejected = true
								segmentLogger.error(
									{ error: uploadErr.message },
									'GCS upload failed.'
								)
								// Attempt to kill ffmpeg on upload error
								try {
									if (ffmpegCommand) {
										segmentLogger.warn(
											'Killing ffmpeg due to GCS upload error.'
										)
										ffmpegCommand.kill('SIGKILL')
									}
								} catch (killErr: any) {
									segmentLogger.warn(
										{ error: killErr.message },
										'Error trying to kill ffmpeg after GCS upload error (kill method)'
									)
								}
								reject(
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

					// --- Transcriptions & Editing ---
					segmentLogger.info('Starting Google transcription...')
					await pushTranscriptionEvent(
						jobId,
						`Google matnni o'girmoqda ${segmentNumber}/${numSegments}...`,
						false,
						broadcast
					)
					const transcriptGoogle = await transcribeWithGoogle(gcsUri)
					if (!transcriptGoogle) {
						segmentLogger.warn(
							// Changed from error to warn
							`Google transcription returned empty/null for ${gcsUri}, proceeding...`
						)
						await pushTranscriptionEvent(
							jobId,
							`${segmentNumber}/${numSegments}-chi Google matnida xatolik (bo'sh natija). Davom etilmoqda...`,
							false,
							broadcast
						)
					} else {
						segmentLogger.info(
							`Google transcription done (length: ${transcriptGoogle.length}).`
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
						if (!transcriptElevenLabs) {
							segmentLogger.warn(
								`ElevenLabs transcription returned empty/null for ${gcsUri}`
							)
							await pushTranscriptionEvent(
								jobId,
								`${segmentNumber}/${numSegments}-chi ElevenLabs matnida xatolik (bo'sh natija). Google natijasi bilan davom etilmoqda...`,
								false,
								broadcast
							)
						} else {
							segmentLogger.info(
								`ElevenLabs transcription done (length: ${transcriptElevenLabs.length}).`
							)
						}
					} catch (elevenLabsError: any) {
						segmentLogger.error(
							{ error: elevenLabsError.message },
							`ElevenLabs transcription failed for ${gcsUri}`
						)
						if (!transcriptGoogle) {
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

					// Determine input for Gemini
					const googleInput = transcriptGoogle || ''
					// Use ElevenLabs if available, otherwise fallback to Google if it exists
					const elevenLabsInput =
						transcriptElevenLabs || (transcriptGoogle ? '' : null) // Pass "" if only Google, null if neither

					// Check if we have *any* text to edit
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

					// --- End Transcriptions & Editing ---

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

					// --- Check for Fatal Errors ---
					if (
						segmentErr.message?.includes('Input stream error:') ||
						segmentErr.message?.includes(
							'yt-dlp stream process exited'
						) ||
						segmentErr.message?.includes(
							'yt-dlp download failed'
						) ||
						segmentErr.message?.includes(
							'Authentication/Authorization Error'
						) ||
						segmentErr.message?.includes(
							'Network/Socket/Timeout error'
						) ||
						segmentErr.message?.includes('ModuleNotFoundError') ||
						segmentErr.message?.includes(
							'cookie file creation failed'
						)
					) {
						segmentLogger.error(
							'Fatal yt-dlp/stream related error occurred. Aborting job.'
						)
						let userMsg = `YouTube yuklashda/kirishda xatolik (yt-dlp ${segmentNumber}/${numSegments}). Cookie/URL/Video holatini tekshiring. Jarayon to'xtatildi. (${segmentErr.message})`
						if (
							segmentErr.message?.includes('ModuleNotFoundError')
						) {
							userMsg = `Server xatosi: yt-dlp ishga tushmadi (${segmentNumber}/${numSegments}). Jarayon to'xtatildi. (${segmentErr.message})`
						} else if (
							segmentErr.message?.includes(
								'cookie file creation failed'
							)
						) {
							userMsg = `Server xatosi: Cookie faylini sozlab bo'lmadi (${segmentNumber}/${numSegments}). Jarayon to'xtatildi. (${segmentErr.message})`
						} else if (
							segmentErr.message?.includes(
								'Authentication/Authorization Error'
							)
						) {
							userMsg = `YouTube kirish xatosi (${segmentNumber}/${numSegments}): Cookie yaroqsiz/eskirgan yoki video maxfiy/yosh chegaralangan/bot tekshiruvi? (${segmentErr.message})`
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
						throw new Error(
							`Aborting job due to fatal stream/auth/network failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					} else if (
						segmentErr.message?.includes('FFmpeg command failed') ||
						segmentErr.message?.includes(
							'FFmpeg output stream error'
						)
					) {
						segmentLogger.error(
							'Fatal FFmpeg error occurred during processing. Aborting job.'
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
					}

					await delay(2000 + attempt * 1000)
				} finally {
					// *** FIX: Use ffmpegCommand.kill() and check segmentProcessedSuccessfully ***
					// Ensure ffmpeg is killed *only if* the segment failed and the command exists
					if (ffmpegCommand && !segmentProcessedSuccessfully) {
						try {
							segmentLogger.warn(
								'Ensuring FFmpeg process is killed in finally block (segment failed).'
							)
							ffmpegCommand.kill('SIGKILL')
						} catch (killErr: any) {
							segmentLogger.warn(
								{ error: killErr.message },
								'Error killing ffmpeg in finally block (kill method)'
							)
						}
					}

					// --- Cleanup GCS File ---
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
							segmentLogger.error(
								{
									error: deleteErr.message,
									file: destFileName
								},
								`Failed to delete GCS segment file: ${destFileName}. Continuing job.`
							)
						}
					} else {
						if (destFileName) {
							segmentLogger.info(
								`Skipping GCS delete for ${destFileName} because upload did not succeed or an earlier error occurred.`
							)
						}
					}
					await delay(300)
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
				throw new Error(
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
					'Failed to mark job as error in DB during final catch block'
				)
			}
		}

		if (broadcast) {
			try {
				let clientErrorMessage = `Serverda kutilmagan xatolik yuz berdi. (${err.message?.substring(0, 100) || 'No details'}...)`
				if (
					err.message?.includes(
						'Aborting job due to fatal stream/auth/network failure'
					)
				) {
					clientErrorMessage = `Xatolik: YouTube'dan yuklab bo'lmadi yoki kirishda/tarmoqda muammo (maxfiy/yosh/bot/cookie?/server xato?/timeout?). Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
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
					clientErrorMessage = `Xatolik: ${err.message}`
				} else if (
					err.message?.includes('yt-dlp info process exited') ||
					err.message?.includes("Video ma'lumotlarini olib bo'lmadi")
				) {
					clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). URL/Cookie/Video holatini/Tarmoqni tekshiring. (${err.message?.substring(0, 100)}...)`
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
					'Failed to send final error SSE event'
				)
			}
		}
	} finally {
		const finalDuration = performance.now() - startTime
		jobLogger.info(
			`Transcription job finished execution after ${formatDuration(finalDuration)}.`
		)
	}
}
