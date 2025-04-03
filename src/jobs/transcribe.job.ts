import ffmpeg from 'fluent-ffmpeg'
import path from 'path'
import { performance } from 'perf_hooks'
import { Readable } from 'stream'
import { exec } from 'youtube-dl-exec'

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

const persistentCookieFilePath = path.join(__dirname, 'cookies.txt')

interface VideoInfo {
	title: string
	duration: number
}

async function getVideoInfoWithYtdl(youtubeUrl: string): Promise<VideoInfo> {
	const logPrefix = 'ytdl-info'
	logger.info(
		`${logPrefix}: Fetching info for ${youtubeUrl}. Attempting to use cookies from ${persistentCookieFilePath}`
	)

	const options: any = {
		noWarnings: true,
		noCallHome: true,
		ignoreConfig: true,
		dumpJson: true,
		skipDownload: true,
		forceIpv4: true
	}

	try {
		// Check if the persistent cookie file exists and is not empty
		const stats = await fs.stat(persistentCookieFilePath)
		if (stats.size > 0) {
			options.cookies = persistentCookieFilePath
			logger.info(
				`${logPrefix}: Using persistent cookie file: ${persistentCookieFilePath}`
			)
		} else {
			logger.warn(
				`${logPrefix}: Persistent cookie file exists but is EMPTY: ${persistentCookieFilePath}. Proceeding without --cookies.`
			)
		}
	} catch (statErr: any) {
		if (statErr.code === 'ENOENT') {
			logger.info(
				`${logPrefix}: Persistent cookie file not found at ${persistentCookieFilePath}. Proceeding without --cookies.`
			)
		} else {
			logger.error(
				{ error: statErr, file: persistentCookieFilePath },
				`${logPrefix}: Failed to stat persistent cookie file. Proceeding without --cookies.`
			)
		}
	}

	try {
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
			specificError = `YouTube access error (yt-dlp info): Video might be private/unavailable/premiere, require login/age/bot confirmation, or cookie file (${persistentCookieFilePath}) is invalid/expired/rejected. Code ${exitCode}.`
		} else if (stderr.includes('unable to download video data')) {
			specificError = `yt-dlp info failed: Unable to download video data. Check URL/Network/Cookie File (${persistentCookieFilePath}). Code ${exitCode}.`
		} else if (
			stderr.includes('cookies') &&
			stderr.includes('No such file or directory')
		) {
			specificError = `yt-dlp info failed: Cookie file specified but not found at ${persistentCookieFilePath}. Code ${exitCode}.`
		}

		throw new Error(`${specificError} Stderr: ${stderr.substring(0, 500)}`)
	}
	// No finally block needed for cookie cleanup anymore
}

async function streamAudioWithYtdl(
	youtubeUrl: string,
	startTime: number,
	duration: number
): Promise<Readable> {
	const logPrefix = 'ytdl-stream'
	let ytdlProcess: ReturnType<typeof exec> | null = null

	logger.info(
		`${logPrefix}: Streaming audio for ${youtubeUrl}. Attempting to use cookies from ${persistentCookieFilePath}`
	)

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

	try {
		// Check if the persistent cookie file exists and is not empty
		const stats = await fs.stat(persistentCookieFilePath)
		if (stats.size > 0) {
			options.cookies = persistentCookieFilePath
			logger.info(
				`${logPrefix}: Using persistent cookie file: ${persistentCookieFilePath}`
			)
		} else {
			logger.warn(
				`${logPrefix}: Persistent cookie file exists but is EMPTY: ${persistentCookieFilePath}. Proceeding without --cookies.`
			)
		}
	} catch (statErr: any) {
		if (statErr.code === 'ENOENT') {
			logger.info(
				`${logPrefix}: Persistent cookie file not found at ${persistentCookieFilePath}. Proceeding without --cookies.`
			)
		} else {
			logger.error(
				{ error: statErr, file: persistentCookieFilePath },
				`${logPrefix}: Failed to stat persistent cookie file. Proceeding without --cookies.`
			)
		}
	}

	try {
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
			// logger.debug(`${logPrefix} stderr chunk: ${chunk.trim()}`); // Uncomment for debugging stderr
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
			// No cookie cleanup needed here anymore

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
					specificError = `yt-dlp download failed (Authentication/Authorization Error - 403/401/Login/Bot/Consent?). Check cookie file (${persistentCookieFilePath}) validity/freshness. Code ${code}.`
				} else if (
					finalStderr.includes('Socket error') ||
					finalStderr.includes('timed out')
				) {
					specificError = `yt-dlp download failed (Network/Socket/Timeout error). Check connection. Code ${code}.`
				} else if (
					finalStderr.includes('cookies') &&
					finalStderr.includes('No such file or directory')
				) {
					specificError = `yt-dlp download failed: Cookie file specified but not found at ${persistentCookieFilePath}. Code ${code}.`
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
			// No cookie cleanup needed here
		})

		outputAudioStream.on('end', () => {
			logger.info(`${logPrefix}: Output stream ended.`)
		})

		return outputAudioStream
	} catch (error: any) {
		logger.error(
			{ error: error.message },
			`${logPrefix}: Error setting up stream (e.g., process start).`
		)
		// No cookie cleanup needed here

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

	// Check if the persistent cookie file exists (optional logging)
	try {
		await fs.access(persistentCookieFilePath, fs.constants.R_OK) // Check read access
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
		jobLogger.info(`Fetching video info via youtube-dl-exec...`)

		try {
			// Pass url only, cookie file path is handled internally now
			videoInfo = await getVideoInfoWithYtdl(url)
			jobLogger.info(
				`Successfully fetched video info for title: ${videoInfo.title}`
			)
		} catch (err: any) {
			jobLogger.error(
				{ error: err.message, stack: err.stack },
				'Failed to get video info from youtube-dl-exec.'
			)
			// Keep user-friendly error messages, reference cookie file path
			let errorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). URL, server yoki cookie faylini (${persistentCookieFilePath}) tekshiring. (${err.message || 'Unknown yt-dlp info error'})`
			if (err.message?.includes('YouTube access error')) {
				if (err.message?.includes('bot confirmation')) {
					errorMessage = `Video ma'lumotlarini olib bo'lmadi (yt-dlp). YouTube bot tekshiruvini talab qilmoqda. Cookie faylini (${persistentCookieFilePath}) yangilang/tekshiring. (${err.message})`
				} else {
					errorMessage = `Video ma'lumotlarini olib bo'lmadi (yt-dlp). YouTube kirish xatosi (maxfiy/mavjud emas/yosh tekshiruvi/cookie yaroqsiz?). Cookie faylini (${persistentCookieFilePath}) tekshiring. (${err.message})`
				}
			} else if (
				err.message?.includes('cookie file specified but not found')
			) {
				errorMessage = `Server xatosi: Cookie fayli topilmadi (${persistentCookieFilePath}). (${err.message})`
			} else if (err.message?.includes('Unable to download video data')) {
				errorMessage = `Video ma'lumotlarini olib bo'lmadi (yt-dlp): Video data yuklanmadi. URL/Tarmoq/Cookie faylini (${persistentCookieFilePath}) tekshiring. (${err.message})`
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
					// Pass url, start time, duration only. Cookie handled internally.
					audioStream = await streamAudioWithYtdl(
						url,
						segmentStartTime,
						safeActualDuration
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
					// Updated error messages to refer to the cookie file
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
							) ||
							segmentErr.message?.includes(
								'Cookie file specified but not found' // Specific cookie file error
							))
					) {
						segmentLogger.error(
							'Fatal youtube-dl-exec stream related error occurred. Aborting job.'
						)
						let userMsg = `YouTube yuklashda/kirishda xatolik (yt-dlp ${segmentNumber}/${numSegments}). Cookie faylini (${persistentCookieFilePath})/URL/Video holatini tekshiring. Jarayon to'xtatildi. (${segmentErr.message})`
						if (
							segmentErr.message?.includes(
								'Authentication/Authorization Error'
							)
						) {
							userMsg = `YouTube kirish xatosi (${segmentNumber}/${numSegments}): Cookie fayli (${persistentCookieFilePath}) yaroqsiz/eskirgan yoki video maxfiy/yosh/bot tekshiruvi? (${segmentErr.message})`
						} else if (
							segmentErr.message?.includes(
								'Network/Socket/Timeout error'
							)
						) {
							userMsg = `Tarmoq xatosi (${segmentNumber}/${numSegments}): YouTube'ga ulanib bo'lmadi (yt-dlp timeout/socket error). (${segmentErr.message})`
						} else if (
							segmentErr.message?.includes(
								'Cookie file specified but not found'
							)
						) {
							userMsg = `Server xatosi (${segmentNumber}/${numSegments}): Cookie fayli topilmadi (${persistentCookieFilePath}). Jarayon to'xtatildi. (${segmentErr.message})`
						}

						await pushTranscriptionEvent(
							jobId,
							userMsg,
							true,
							broadcast
						)
						throw new Error( // Re-throw to exit the main try block
							`Aborting job due to fatal stream/auth/network/cookie failure on segment ${segmentNumber}: ${segmentErr.message}`
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
					}
					// Non-fatal errors will allow retry loop to continue

					await delay(2000 + attempt * 1000) // Backoff before retry
				} finally {
					// Ensure resources are cleaned up after each attempt if needed
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

		if (broadcast) {
			try {
				let clientErrorMessage = `Serverda kutilmagan xatolik yuz berdi. (${err.message?.substring(0, 100) || 'No details'}...)`

				if (
					err.message?.includes(
						'Aborting job due to fatal stream/auth/network/cookie failure'
					)
				) {
					if (err.message?.includes('bot confirmation')) {
						clientErrorMessage = `Xatolik: YouTube bot tekshiruvini talab qilmoqda. Cookie faylini (${persistentCookieFilePath}) yangilang/tekshiring. Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
					} else if (
						err.message?.includes(
							'Authentication/Authorization Error'
						)
					) {
						clientErrorMessage = `Xatolik: YouTube kirish xatosi (cookie fayli (${persistentCookieFilePath}) yaroqsiz/video maxfiy?). Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
					} else if (
						err.message?.includes('Network/Socket/Timeout error')
					) {
						clientErrorMessage = `Xatolik: Tarmoq xatosi (YouTube'ga ulanib bo'lmadi?). Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
					} else if (
						err.message?.includes(
							'Cookie file specified but not found'
						)
					) {
						clientErrorMessage = `Xatolik: Serverda cookie fayli topilmadi (${persistentCookieFilePath}). Jarayon to'xtatildi. (${err.message?.substring(0, 100)}...)`
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
					err.message?.includes('yt-dlp info process failed') ||
					err.message?.includes("Video ma'lumotlarini olib bo'lmadi") // Catch initial info fetch errors
				) {
					if (err.message?.includes('bot confirmation')) {
						clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). YouTube bot tekshiruvini talab qilmoqda. Cookie faylini (${persistentCookieFilePath}) yangilang/tekshiring. (${err.message?.substring(0, 100)}...)`
					} else if (
						err.message?.includes(
							'cookie file specified but not found'
						)
					) {
						clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi. Serverda cookie fayli topilmadi (${persistentCookieFilePath}). (${err.message?.substring(0, 100)}...)`
					} else {
						clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). URL/Cookie faylini (${persistentCookieFilePath})/Video holatini/Tarmoqni tekshiring. (${err.message?.substring(0, 100)}...)`
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
