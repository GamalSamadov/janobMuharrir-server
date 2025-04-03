import { ChildProcess, spawn } from 'child_process'
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
		return { cleanup: async () => {} }
	}

	// *** ADDED LOGGING: Log length and snippet of provided cookie value ***
	logger.info(
		`${logPrefix}: Received cookie value (length: ${cookieValue.length}). Starting file creation.`
		// Avoid logging full cookie: `Snippet: ${cookieValue.substring(0, 30)}...${cookieValue.substring(cookieValue.length - 30)}`
	)

	const tempDir = os.tmpdir()
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
		// *** ADDED LOGGING: Log length being written ***
		logger.info(
			`${logPrefix}: Writing ${sanitizedCookieValue.length} bytes to cookie file: ${cookieFilePath}`
		)
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
		await cleanup() // Attempt cleanup even if creation failed
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

	logger.info(
		`getVideoInfoWithYtDlp called for ${youtubeUrl}. Cookie provided: ${!!cookie}`
	)

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

		// *** Verification Step Added ***
		if (cookieHandler.cookieFilePath) {
			try {
				const stats = await fs.stat(cookieHandler.cookieFilePath)
				if (stats.size > 0) {
					logger.info(
						`yt-dlp-info: Verified cookie file exists and has size > 0: ${cookieHandler.cookieFilePath} (Size: ${stats.size} bytes)`
					)
					args.push('--cookies', cookieHandler.cookieFilePath)
				} else {
					logger.warn(
						`yt-dlp-info: Cookie file exists but is EMPTY: ${cookieHandler.cookieFilePath}. Proceeding without --cookies.`
					)
					// Do not add the --cookies argument if the file is empty
				}
			} catch (statErr: any) {
				// Log if stat fails (e.g., file doesn't exist after creation - should be rare)
				logger.error(
					{ error: statErr, file: cookieHandler.cookieFilePath },
					`yt-dlp-info: Failed to stat cookie file just before spawn. Proceeding without --cookies.`
				)
				// Do not add the --cookies argument if we can't verify it
			}
		} else {
			logger.info(
				'yt-dlp-info: No cookie file generated. Proceeding without --cookies argument.'
			)
		}
		// *** End Verification Step ***

		logger.info(
			`Spawning yt-dlp to get video info: yt-dlp ${args.join(' ')}`
		)

		// Rest of the Promise logic remains IDENTICAL to the previous version
		return await new Promise<VideoInfo>((resolve, reject) => {
			const ytDlpProcess: ChildProcess = spawn('yt-dlp', args)
			let jsonData = ''
			let errorData = ''
			const MAX_STDERR_LOG = 2000

			ytDlpProcess.stdout?.on('data', data => {
				jsonData += data.toString()
			})
			ytDlpProcess.stderr?.on('data', data => {
				const errLine = data.toString()
				errorData += errLine
				logger.warn(`yt-dlp info stderr chunk: ${errLine.trim()}`)
			})
			ytDlpProcess.on('error', err => {
				/* ... error handling ... */
			})
			ytDlpProcess.on('close', code => {
				const finalStderr = errorData
				cookieHandler
					?.cleanup()
					.catch(/* ... */)
					.finally(() => {
						cookieHandler = null
					})

				if (code !== 0) {
					logger.error(
						`yt-dlp info process exited with code ${code}. Full Stderr: ${finalStderr}`
					)
					let specificError = `yt-dlp info process exited with code ${code}.`
					// *** Make sure this check is robust ***
					if (
						finalStderr.includes('Private video') ||
						finalStderr.includes('login required') ||
						finalStderr.includes(
							'Sign in to confirm you’re not a bot'
						) || // Explicit check
						finalStderr.includes('confirm your age') ||
						finalStderr.includes('unavailable') ||
						finalStderr.includes('Sign in') ||
						finalStderr.includes('consent') ||
						finalStderr.includes('403') ||
						finalStderr.includes('401') ||
						finalStderr.includes('Premiere') ||
						finalStderr.includes('confirm you')
					) {
						specificError = `YouTube access error (yt-dlp info): Video might be private/unavailable/premiere, require login/age/bot confirmation, or cookie invalid/expired/rejected. Code ${code}.`
					} // ... other specific error checks ...
					else if (finalStderr.includes('ModuleNotFoundError')) {
						/* ... */
					} else if (finalStderr.includes('cookie file not found')) {
						/* ... */
					} else if (
						finalStderr.includes(
							'ERROR: unable to download video data'
						)
					) {
						/* ... */
					}

					reject(
						new Error(
							`${specificError} Stderr: ${finalStderr.substring(0, MAX_STDERR_LOG)}`
						)
					)
				} else {
					// ... success handling ...
					try {
						// ... JSON parsing ...
					} catch (parseErr: any) {
						// ... parsing error handling ...
					}
				}
			})
		})
	} catch (setupError: any) {
		// ... setup error handling ...
		logger.error(
			{ error: setupError.message },
			'Error setting up yt-dlp info call (e.g., cookie file creation)'
		)
		await cookieHandler?.cleanup().catch(/* ... */)
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
	let ytDlpProcess: ChildProcess | null = null

	logger.info(
		`streamAudioWithYtDlp called for ${youtubeUrl}. Cookie provided: ${!!cookie}`
	)

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
			`"ffmpeg_i:-ss ${startTime} -to ${startTime + duration}"`,
			youtubeUrl
		]

		// *** Verification Step Added ***
		if (cookieHandler.cookieFilePath) {
			try {
				const stats = await fs.stat(cookieHandler.cookieFilePath)
				if (stats.size > 0) {
					logger.info(
						`yt-dlp-stream: Verified cookie file exists and has size > 0: ${cookieHandler.cookieFilePath} (Size: ${stats.size} bytes)`
					)
					// Add cookies arg correctly
					const ppArgsIndex = args.indexOf('--postprocessor-args')
					if (ppArgsIndex > -1) {
						args.splice(
							ppArgsIndex,
							0,
							'--cookies',
							cookieHandler.cookieFilePath
						)
					} else {
						args.splice(
							args.indexOf('--force-ipv4') + 1,
							0,
							'--cookies',
							cookieHandler.cookieFilePath
						)
					}
				} else {
					logger.warn(
						`yt-dlp-stream: Cookie file exists but is EMPTY: ${cookieHandler.cookieFilePath}. Proceeding without --cookies.`
					)
				}
			} catch (statErr: any) {
				logger.error(
					{ error: statErr, file: cookieHandler.cookieFilePath },
					`yt-dlp-stream: Failed to stat cookie file just before spawn. Proceeding without --cookies.`
				)
			}
		} else {
			logger.info(
				'yt-dlp-stream: No cookie file generated. Proceeding without --cookies argument.'
			)
		}
		// *** End Verification Step ***

		logger.info(
			`Spawning yt-dlp for audio segment: yt-dlp ${args.map(arg => (arg.includes(' ') ? `"${arg}"` : arg)).join(' ')}`
		)

		// Rest of the function (spawn, stdio checks, event handlers, Promise logic)
		// remains IDENTICAL to the previous version...
		ytDlpProcess = spawn('yt-dlp', args, {
			stdio: ['ignore', 'pipe', 'pipe']
		})
		if (!ytDlpProcess.stdout || !ytDlpProcess.stderr) {
			/* ... stdio error handling ... */
		}
		const outputAudioStream = ytDlpProcess.stdout
		let stderrData = ''
		const MAX_STDERR_LOG = 2000
		ytDlpProcess.stderr.on('data', data => {
			/* ... collect stderr ... */
		})
		ytDlpProcess.on('error', err => {
			/* ... spawn error handling ... */
		})
		ytDlpProcess.on('close', async code => {
			const finalStderr = stderrData
			if (cookieHandler) {
				/* ... cleanup ... */
			}
			if (code !== 0) {
				logger.error(
					`yt-dlp stream process exited with code ${code}. Full Stderr: ${finalStderr}`
				)
				let specificError = `yt-dlp stream process exited with error code ${code}.`
				// Ensure robust check for bot error
				if (
					finalStderr.includes('403 Forbidden') ||
					finalStderr.includes('401 Unauthorized') ||
					finalStderr.includes(
						'Sign in to confirm you’re not a bot'
					) || // Explicit check
					finalStderr.includes('Sign in') ||
					finalStderr.includes('confirm you') ||
					finalStderr.includes('consent') ||
					finalStderr.includes('login required')
				) {
					specificError = `yt-dlp download failed (Authentication/Authorization Error - 403/401/Login/Bot/Consent?). Check cookie validity/freshness. Code ${code}.`
				} // ... other specific error checks ...
				else if (finalStderr.includes('ModuleNotFoundError')) {
					/* ... */
				} else if (
					finalStderr.includes('Socket error') ||
					finalStderr.includes('timed out')
				) {
					/* ... */
				}
				// ... etc ...

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
				logger.info('yt-dlp stream process finished successfully.')
			}
		})
		outputAudioStream.on('error', async err => {
			/* ... stream error handling ... */
		})
		outputAudioStream.on('end', () => {
			/* ... stream end handling ... */
		})

		return outputAudioStream
	} catch (error: any) {
		// ... setup error handling ...
		logger.error(
			{ error: error.message },
			'Error setting up yt-dlp stream (e.g., cookie file creation failed)'
		)
		await cookieHandler?.cleanup().catch(/* ... */)
		const errorStream = new Readable({
			/* ... error stream creation ... */
		})
		return errorStream
	}
}

// --- Main Transcription Job Logic ---
// (No changes needed in pushTranscriptionEvent or the main runTranscriptionJob loop itself,
// except potentially adapting the final error messages if needed based on new log insights)

export async function pushTranscriptionEvent(
	jobId: string,
	content: string,
	completed = false,
	broadcast?: (content: string, completed: boolean) => void
) {
	// No changes here
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

	// *** ADDED LOGGING: More explicit check ***
	if (youtubeCookie && youtubeCookie.trim().length > 0) {
		jobLogger.info(
			`Found non-empty YOUTUBE_COOKIE environment variable (length: ${youtubeCookie.length}).`
		)
	} else if (youtubeCookie) {
		jobLogger.warn(
			'YOUTUBE_COOKIE environment variable is set but empty or only whitespace. Will proceed without cookies.'
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
			// Pass the potentially empty/null cookie string
			videoInfo = await getVideoInfoWithYtDlp(url, youtubeCookie)
			jobLogger.info(
				`Successfully fetched video info via yt-dlp for title: ${videoInfo.title}`
			)
		} catch (err: any) {
			jobLogger.error(
				{ error: err.message, stack: err.stack }, // Log full error here
				'Failed to get video info from yt-dlp.'
			)
			// Error message generation remains largely the same, using the refined messages from the helper
			let errorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). URL, server yoki cookie'ni tekshiring. (${err.message || 'Unknown yt-dlp info error'})`
			if (err.message?.includes('YouTube access error')) {
				// More specific message for the bot check
				if (err.message?.includes('bot confirmation')) {
					errorMessage = `Video ma'lumotlarini olib bo'lmadi (yt-dlp). YouTube bot tekshiruvini talab qilmoqda. Cookie faylini yangilang/tekshiring. (${err.message})`
				} else {
					errorMessage = `Video ma'lumotlarini olib bo'lmadi (yt-dlp). YouTube kirish xatosi (maxfiy/mavjud emas/yosh tekshiruvi/cookie yaroqsiz?). (${err.message})`
				}
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

		// Rest of the job logic remains the same as the previous version...
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
				let ffmpegCommand: ffmpeg.FfmpegCommand | null = null

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
					// Pass potentially empty/null cookie string
					const audioStream = await streamAudioWithYtDlp(
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
						if (!ffmpegCommand) {
							return reject(
								new Error('FFmpeg command was not initialized.')
							)
						}
						const ffmpegOutputStream = ffmpegCommand.pipe()
						let promiseRejected = false

						audioStream.on('error', inputError => {
							if (promiseRejected) return
							promiseRejected = true
							segmentLogger.error(
								{ error: inputError.message },
								'Error emitted on yt-dlp input stream for ffmpeg'
							)
							try {
								if (ffmpegCommand) {
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

						ffmpegCommand.on('error', err => {
							if (promiseRejected) return
							promiseRejected = true
							reject(
								new Error(
									`FFmpeg command failed directly: ${err.message}`
								)
							)
						})

						ffmpegOutputStream.on('error', outputError => {
							if (promiseRejected) return
							promiseRejected = true
							segmentLogger.error(
								{ error: outputError.message },
								'Error emitted on ffmpeg output stream during upload pipe.'
							)
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
									gcsUploadSucceeded = false
								}
							})
							.catch(uploadErr => {
								if (promiseRejected) return
								promiseRejected = true
								segmentLogger.error(
									{ error: uploadErr.message },
									'GCS upload failed.'
								)
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

					const googleInput = transcriptGoogle || ''
					const elevenLabsInput =
						transcriptElevenLabs || (transcriptGoogle ? '' : null)

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

					// Fatal error checks remain the same...
					if (
						segmentErr.message?.includes('Input stream error:') ||
						segmentErr.message?.includes(
							'yt-dlp stream process exited'
						) ||
						segmentErr.message?.includes(
							'yt-dlp download failed'
						) || // Catches the specific auth/network errors now
						// segmentErr.message?.includes('Authentication/Authorization Error') || // Covered by above
						// segmentErr.message?.includes('Network/Socket/Timeout error') || // Covered by above
						segmentErr.message?.includes('ModuleNotFoundError') ||
						segmentErr.message?.includes(
							'cookie file creation failed'
						)
					) {
						segmentLogger.error(
							'Fatal yt-dlp/stream related error occurred. Aborting job.'
						)
						// User message generation depends on the specific error captured by the helper
						let userMsg = `YouTube yuklashda/kirishda xatolik (yt-dlp ${segmentNumber}/${numSegments}). Cookie/URL/Video holatini tekshiring. Jarayon to'xtatildi. (${segmentErr.message})`
						// Refine message based on helper's specific error (already includes bot check etc.)
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
						} else if (
							segmentErr.message?.includes('ModuleNotFoundError')
						) {
							userMsg = `Server xatosi: yt-dlp ishga tushmadi (${segmentNumber}/${numSegments}). Jarayon to'xtatildi. (${segmentErr.message})`
						} else if (
							segmentErr.message?.includes(
								'cookie file creation failed'
							)
						) {
							userMsg = `Server xatosi: Cookie faylini sozlab bo'lmadi (${segmentNumber}/${numSegments}). Jarayon to'xtatildi. (${segmentErr.message})`
						}
						await pushTranscriptionEvent(
							jobId,
							userMsg,
							true,
							broadcast
						)
						throw new Error( // Throw to exit the main try block
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
						throw new Error( // Throw to exit the main try block
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
						throw new Error( // Throw to exit the main try block
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
						throw new Error( // Throw to exit the main try block
							`Aborting job due to fatal transcription/editing failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}

					await delay(2000 + attempt * 1000)
				} finally {
					// Finally block logic for ffmpeg kill and GCS delete remains the same
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
		// Remains the same
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
		// Remains the same, using the refined error messages from helpers/segment loop
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
				// Specific message handling remains the same...
				if (
					err.message?.includes(
						'Aborting job due to fatal stream/auth/network failure'
					)
				) {
					// Pick the more specific message generated earlier
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
					clientErrorMessage = `Xatolik: ${err.message}`
				} else if (
					err.message?.includes('yt-dlp info process exited') ||
					err.message?.includes("Video ma'lumotlarini olib bo'lmadi")
				) {
					// Catch the initial info fetch error, including the refined bot check message
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
