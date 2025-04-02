import { spawn } from 'child_process'
import ffmpeg from 'fluent-ffmpeg'
import { performance } from 'perf_hooks'
import { Readable } from 'stream'

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

const delay = (ms: number) => new Promise(res => setTimeout(res, ms))

// --- Helper Functions for yt-dlp (Keep as before) ---
interface VideoInfo {
	title: string
	duration: number // in seconds
}
async function getVideoInfoWithYtDlp(
	youtubeUrl: string,
	cookie?: string
): Promise<VideoInfo> {
	return new Promise((resolve, reject) => {
		const args = [
			'--no-warnings',
			'--no-call-home',
			'--ignore-config',
			'--dump-json',
			'--skip-download',
			youtubeUrl
		]
		if (cookie) {
			args.unshift('--add-header', `Cookie:${cookie}`)
			logger.info('Using provided cookie with yt-dlp info command.')
		} else {
			logger.warn('No YouTube cookie provided for yt-dlp info command.')
		}
		logger.info(
			`Spawning yt-dlp to get video info: yt-dlp ${args.join(' ')}`
		)
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
				logger.warn(`yt-dlp info stderr: ${errLine.trim()}`)
			}
		})
		ytDlpProcess.on('error', err => {
			logger.error(
				{ error: err },
				'Failed to spawn yt-dlp process for info.'
			)
			reject(new Error(`Failed to start yt-dlp for info: ${err.message}`))
		})
		ytDlpProcess.on('close', code => {
			if (code !== 0) {
				logger.error(
					`yt-dlp info process exited with code ${code}. Stderr: ${errorData}`
				)
				if (
					errorData.includes('Private video') ||
					errorData.includes('login required') ||
					errorData.includes('confirm your age') ||
					errorData.includes('unavailable') ||
					errorData.includes('Sign in') ||
					errorData.includes('403') ||
					errorData.includes('Premiere')
				) {
					reject(
						new Error(
							`YouTube access error (yt-dlp info): Video might be private/unavailable/premiere, require login, or cookie invalid. Code ${code}. Stderr: ${errorData.substring(0, 500)}`
						)
					)
				} else if (errorData.includes('ModuleNotFoundError')) {
					reject(
						new Error(
							`yt-dlp execution failed (ModuleNotFoundError). Ensure Python environment and yt-dlp installation are correct in the container. Code ${code}. Stderr: ${errorData.substring(0, 500)}`
						)
					)
				} else {
					reject(
						new Error(
							`yt-dlp info process exited with code ${code}. Stderr: ${errorData.substring(0, 500)}`
						)
					)
				}
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
						resolve({ title: info.title, duration: info.duration })
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
}
async function streamAudioWithYtDlp(
	youtubeUrl: string,
	startTime: number,
	duration: number,
	cookie?: string
): Promise<Readable> {
	const args = [
		'--no-warnings',
		'--no-call-home',
		'--ignore-config',
		'-f',
		'bestaudio/best',
		'--output',
		'-',
		'--postprocessor-args',
		`ffmpeg:-ss ${startTime} -t ${duration}`, // No -c:a copy
		youtubeUrl
	]
	if (cookie) {
		args.unshift('--add-header', `Cookie:${cookie}`)
		logger.info('Using provided cookie with yt-dlp stream command.')
	} else {
		logger.warn('No YouTube cookie provided for yt-dlp stream command.')
	}
	logger.info(`Spawning yt-dlp for audio segment: yt-dlp ${args.join(' ')}`)
	const ytDlpProcess = spawn('yt-dlp', args, {
		stdio: ['ignore', 'pipe', 'pipe']
	})
	const outputAudioStream = ytDlpProcess.stdout
	let stderrData = ''
	ytDlpProcess.stderr.on('data', data => {
		const errLine = data.toString()
		stderrData += errLine
		if (
			!errLine.includes('WARNING:') &&
			!errLine.includes('Output stream #') &&
			!errLine.includes('[download]')
		) {
			// Filter download progress
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
			new Error(`Failed to start yt-dlp stream process: ${err.message}`)
		)
	})
	ytDlpProcess.on('close', code => {
		if (code !== 0) {
			const detailedErrorMessage = `yt-dlp stream process exited with error code ${code}. Stderr: ${stderrData.substring(0, 1000)}`
			logger.error(detailedErrorMessage)
			if (stderrData.includes('ModuleNotFoundError')) {
				outputAudioStream.emit(
					'error',
					new Error(
						`yt-dlp execution failed (ModuleNotFoundError). Check container setup. Code ${code}.`
					)
				)
			} else if (stderrData.includes('403: Forbidden')) {
				outputAudioStream.emit(
					'error',
					new Error(
						`yt-dlp download failed (403 Forbidden). Check cookie validity and freshness. Code ${code}. Stderr: ${stderrData.substring(0, 500)}`
					)
				)
			} else {
				outputAudioStream.emit('error', new Error(detailedErrorMessage))
			}
		} else {
			logger.info('yt-dlp stream process finished successfully.')
		}
	})
	return outputAudioStream
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

	try {
		await transcriptService.running(jobId)
		jobStatusUpdated = true
		await delay(1000)

		// --- Get Video Info (unchanged block) ---
		let videoInfo: VideoInfo
		const youtubeCookie = process.env.YOUTUBE_COOKIE
		jobLogger.info(
			{
				hasCookie: !!youtubeCookie,
				cookieLength: youtubeCookie?.length ?? 0
			},
			'Checking YouTube cookie presence before yt-dlp info call'
		)
		try {
			jobLogger.info(`Fetching video info via yt-dlp for URL: ${url}`)
			videoInfo = await getVideoInfoWithYtDlp(url, youtubeCookie)
			jobLogger.info(
				`Successfully fetched video info via yt-dlp for title: ${videoInfo.title}`
			)
		} catch (err: any) {
			jobLogger.error(
				{ error: err.message, stack: err.stack, url: url },
				'Failed to get video info from yt-dlp.'
			)
			let errorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). URL yoki serverni tekshiring. (${err.message || 'Unknown yt-dlp info error'})`
			if (err.message?.includes('YouTube access error')) {
				errorMessage = `Video ma'lumotlarini olib bo'lmadi (yt-dlp). YouTube kirish xatosi (maxfiy/yosh cheklangan/premyera/cookie?) bo'lishi mumkin. (${err.message})`
			} else if (err.message?.includes('ModuleNotFoundError')) {
				errorMessage = `Server xatosi: yt-dlp ishga tushmadi (ModuleNotFoundError). (${err.message})`
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
			/* ... error handling unchanged ... */
		}
		try {
			await transcriptService.updateTitle(jobId, title)
		} catch (updateErr: any) {
			jobLogger.warn(
				{ title, error: updateErr.message },
				'Failed to update job title in database.'
			)
		}

		// --- UI messages unchanged ---
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
			/* ... error handling unchanged ... */
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

			// FIX: Define destFileName consistently within the loop's scope for each segment
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
				let gcsUploadSucceeded = false // Reset for each attempt

				if (attempt > 1) {
					/* ... retry message unchanged ... */
				}

				try {
					await pushTranscriptionEvent(
						jobId,
						`Bo'lak ${segmentNumber}/${numSegments} yuklanmoqda (yt-dlp)...`,
						false,
						broadcast
					)

					// --- Stream Audio Segment (unchanged call) ---
					segmentLogger.info(
						`Attempting segment download via yt-dlp (start: ${segmentStartTime}s, duration: ${safeActualDuration}s)...`
					)
					const audioStream = await streamAudioWithYtDlp(
						url,
						segmentStartTime,
						safeActualDuration,
						youtubeCookie
					)

					// --- Create ffmpeg command ---
					segmentLogger.info(`Starting FFmpeg encoding...`)
					const ffmpegCommand = ffmpeg(audioStream)
						.format('mp3')
						.audioCodec('libmp3lame')
						.audioBitrate('96k') // Set bitrate
						// .audioQuality(undefined) // <<< FIX: REMOVED this line >>>
						.on('start', cmd =>
							segmentLogger.info(`FFmpeg started: ${cmd}`)
						)
						// Error handler below is mainly for logging now, promise rejection handles control flow
						.on('error', (err, stdout, stderr) => {
							segmentLogger.error(
								{ message: err.message, stdout, stderr },
								`FFmpeg error event processing segment (command level)` // Added context
							)
						})
						.on('end', () => {
							segmentLogger.info(
								`FFmpeg processing seemingly finished.`
							)
						})

					// --- Wrap ffmpeg processing and upload in a Promise ---
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
								ffmpegCommand.kill('SIGKILL')
							} catch (killErr) {
								/* ignore */
							}
							reject(
								new Error(
									`Input stream error: ${inputError.message}`
								)
							)
						})

						// Handle ffmpeg's own errors more directly if possible
						ffmpegCommand.on('error', err => {
							// Simpler listener just for rejection
							if (promiseRejected) return
							promiseRejected = true
							// Logged above already by the more detailed listener
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
								// Only resolve if no error occurred during ffmpeg processing/piping
								if (!promiseRejected) {
									gcsUploadSucceeded = true // Mark GCS upload as successful
									segmentLogger.info(
										`Segment successfully encoded and uploaded to ${gcsUri}`
									)
									resolve() // Success
								} else {
									segmentLogger.warn(
										'GCS upload technically finished, but an error occurred earlier in the ffmpeg/stream process.'
									)
									// Don't resolve, let the rejection stand.
								}
							})
							.catch(uploadErr => {
								if (promiseRejected) return
								promiseRejected = true // Ensure rejection happens
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

					// --- Transcriptions & Editing (unchanged logic) ---
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
							`${segmentNumber}/${numSegments}-chi Google matnida xatolik (bo'sh natija).`,
							false,
							broadcast
						)
						continue
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
							`${segmentNumber}/${numSegments}-chi ElevenLabs matnida xatolik (${elevenLabsError.message}).`,
							false,
							broadcast
						)
						continue
					}
					if (!transcriptElevenLabs) {
						segmentLogger.error(
							`ElevenLabs transcription returned empty/null`
						)
						await pushTranscriptionEvent(
							jobId,
							`${segmentNumber}/${numSegments}-chi ElevenLabs matnida xatolik (bo'sh natija).`,
							false,
							broadcast
						)
						continue
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
							`${segmentNumber}/${numSegments}-chi Gemini tahririda xatolik (bo'sh natija).`,
							false,
							broadcast
						)
						continue
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
					// Log segment error (unchanged)
					segmentLogger.error(
						{ error: segmentErr.message, stack: segmentErr.stack },
						`Error processing segment on attempt ${attempt}`
					)
					await pushTranscriptionEvent(
						jobId,
						`Xatolik (${segmentNumber}/${numSegments}, urinish ${attempt}): ${segmentErr.message.substring(0, 150)}...`,
						false,
						broadcast
					)

					// Check for fatal yt-dlp errors (unchanged)
					if (
						segmentErr.message?.includes('yt-dlp') ||
						segmentErr.message?.includes('Input stream error:') ||
						segmentErr.message?.includes('403 Forbidden') ||
						segmentErr.message?.includes('ModuleNotFoundError')
					) {
						segmentLogger.error(
							'Fatal yt-dlp/stream related error occurred. Aborting job.'
						)
						let userMsg = `YouTube yuklashda/kirishda xatolik (yt-dlp ${segmentNumber}/${numSegments}). Cookie/URL/Video holatini tekshiring. Jarayon to'xtatildi.`
						if (
							segmentErr.message?.includes('ModuleNotFoundError')
						) {
							userMsg = `Server xatosi: yt-dlp ishga tushmadi (${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`
						}
						await pushTranscriptionEvent(
							jobId,
							userMsg,
							true,
							broadcast
						)
						throw new Error(
							`Aborting job due to fatal stream failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}
					// Check for fatal ffmpeg errors (added specific check)
					if (segmentErr.message?.includes('FFmpeg command failed')) {
						segmentLogger.error(
							'Fatal FFmpeg error occurred during processing. Aborting job.'
						)
						await pushTranscriptionEvent(
							jobId,
							`Audio kodlashda xatolik (FFmpeg ${segmentNumber}/${numSegments}). Jarayon to'xtatildi.`,
							true,
							broadcast
						)
						throw new Error(
							`Aborting job due to fatal FFmpeg failure on segment ${segmentNumber}: ${segmentErr.message}`
						)
					}
					await delay(1500 + attempt * 1000)
				} finally {
					// --- Cleanup GCS File ---
					// Check gcsUploadSucceeded *before* attempting delete
					if (gcsUploadSucceeded) {
						try {
							segmentLogger.info(
								`Attempting to delete GCS file: ${destFileName}`
							)
							// FIX: Add safeguard check, but real fix is likely in deleteGCSFile helper
							if (destFileName) {
								await deleteGCSFile(destFileName)
								segmentLogger.info(
									`Successfully deleted GCS file: ${destFileName}`
								)
							} else {
								segmentLogger.error(
									'destFileName was unexpectedly empty or undefined before delete attempt!'
								)
							}
						} catch (deleteErr: any) {
							// Log specific GCS delete error
							// Check for the specific error message seen before
							if (
								deleteErr.message?.includes(
									'file name must be specified'
								)
							) {
								segmentLogger.error(
									{
										error: deleteErr.message,
										file: destFileName
									},
									`Potential bug in deleteGCSFile helper or GCS interaction: ${deleteErr.message}`
								)
							} else {
								segmentLogger.error(
									{
										error: deleteErr.message,
										file: destFileName
									},
									`Failed to delete GCS segment file: ${destFileName}`
								)
							}
							// Don't push event for cleanup errors, too noisy
						}
					} else {
						segmentLogger.info(
							`Skipping GCS delete for ${destFileName} because upload did not succeed.`
						)
					}
					await delay(500)
				}
			} // End retry loop

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
				throw new Error(
					`Failed to process segment ${segmentNumber} after ${maxAttempts} attempts.`
				)
			}

			i++ // Move to the next segment
		} // End segment loop

		// --- Combine and Finalize (unchanged) ---
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
		// --- Final Error Handling (minor adjustments for clarity) ---
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
						'Aborting job due to fatal stream failure'
					)
				) {
					clientErrorMessage = `Xatolik: YouTube'dan yuklab bo'lmadi yoki kirishda muammo (maxfiy/yosh/cookie?/server xato?). (${err.message?.substring(0, 100)}...)`
				} else if (
					err.message?.includes(
						'Aborting job due to fatal FFmpeg failure'
					)
				) {
					clientErrorMessage = `Xatolik: Audio faylni kodlashda muammo (FFmpeg). Serverni tekshiring. (${err.message?.substring(0, 100)}...)`
				} else if (err.message?.includes('Failed to process segment')) {
					clientErrorMessage = `Xatolik: ${err.message}`
				} else if (
					err.message?.includes('yt-dlp info process exited')
				) {
					clientErrorMessage = `Xatolik: Video ma'lumotlarini olib bo'lmadi (yt-dlp). URL/Cookie/Video holatini tekshiring.`
				} else if (err.message?.includes('GOOGLE_CLOUD_BUCKET_NAME')) {
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
					'Failed to send final error SSE event'
				)
			}
		}
	} finally {
		jobLogger.info('Transcription job finished execution.')
	}
}
