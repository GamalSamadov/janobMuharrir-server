import ffmpeg from 'fluent-ffmpeg'
import fetch from 'node-fetch'
import { performance } from 'perf_hooks'
import { PassThrough, Readable } from 'stream'

import {
	convertToUzbekLatin,
	delay,
	deleteGCSFile,
	editTranscribed,
	formatDuration,
	getAudioDuration,
	getGCSFileStream,
	transcribeAudioElevenLabs,
	transcribeWithGoogle,
	uploadStreamToGCS
} from '@/jobs/helpers'
import { logger } from '@/lib/logger'
import { userSession } from '@/services/session/session.service'
import { transcriptService } from '@/services/transcript/transcript.service'

import { extractVideoId, pushTranscriptionEvent } from './transcribe'

const RAPIDAPI_KEY = process.env.RAPIDAPI_KEY
const RAPIDAPI_HOST = 'youtube-mp3-api.p.rapidapi.com'
const RAPIDAPI_BASE_URL = `https://${RAPIDAPI_HOST}/get.php`

if (!RAPIDAPI_KEY) {
	logger.error('RapidAPI Key is not configured in environment variables.')
}

export async function runTranscriptionJob(
	jobId: string,
	sessionId: string,
	url: string,
	broadcast?: (content: string, completed: boolean) => void
) {
	const startTime = performance.now()
	let fullAudioGcsUri: string | null = null
	let fullAudioFileName: string | null = null

	try {
		await transcriptService.running(jobId)

		if (!RAPIDAPI_KEY) {
			throw new Error('RapidAPI Key is not configured.')
		}

		logger.info(`Extracting video ID for: ${url}`)
		const videoId = extractVideoId(url)

		if (!videoId) {
			logger.error(`Could not extract Video ID from URL: ${url}`)
			await transcriptService.error(jobId)
			await pushTranscriptionEvent(
				jobId,
				"Videoning ID sini ajratib olib bo'lmadi",
				true,
				broadcast
			)
			return
		}
		logger.info(`Video ID: ${videoId}`)

		await pushTranscriptionEvent(
			jobId,
			'API orqali ma`lumot olinmoqda...',
			false,
			broadcast
		)
		await delay(200)

		const apiUrl = `${RAPIDAPI_BASE_URL}?id=${videoId}`
		const options = {
			method: 'GET',
			headers: {
				'x-rapidapi-key': RAPIDAPI_KEY,
				'x-rapidapi-host': RAPIDAPI_HOST
			}
		}

		let title = 'Untitled Video'
		let downloadUrl: string
		let audioStream: Readable | null = null

		while (!audioStream) {
			logger.info(`Attempting to fetch audio from RapidAPI...`)
			try {
				logger.info(`Calling RapidAPI: ${apiUrl}`)
				const response = await fetch(apiUrl, options)
				const resultText = await response.text() // Get raw text first for logging
				logger.debug(`RapidAPI Raw Response: ${resultText}`)

				if (!response.ok) {
					throw new Error(
						`RapidAPI request failed with status ${response.status}: ${resultText}`
					)
				}

				const result = JSON.parse(resultText) // Now parse JSON

				if (result.status !== 'success' || !result.download) {
					logger.error(
						'RapidAPI did not return success status or download URL:',
						result
					)
					throw new Error(
						`API dan xatolik: ${result.message || 'Noma`lum xato'}`
					)
				}

				title = result.title || title
				downloadUrl = result.download
				logger.info(
					`RapidAPI Success: Title - ${title}, Download URL obtained.`
				)
				await transcriptService.updateTitle(jobId, title)
			} catch (apiError: any) {
				logger.error(
					`Failed to get data from RapidAPI for ${videoId}:`,
					apiError
				)
				await transcriptService.error(jobId)
				await pushTranscriptionEvent(
					jobId,
					`API dan ma'lumot olishda xatolik: ${apiError.message}`,
					false,
					broadcast
				)
			}

			await pushTranscriptionEvent(
				jobId,
				'Ovoz yuklanmoqda...',
				false,
				broadcast
			)
			await delay(500)

			// --- Download the actual audio file and upload to GCS ---
			fullAudioFileName = `full_audio_${jobId}_${Date.now()}.mp3`

			try {
				logger.info(`Fetching audio from download URL: ${downloadUrl}`)
				const audioResponse = await fetch(downloadUrl)
				if (!audioResponse.ok) {
					throw new Error(
						`Failed to fetch audio file: Status ${audioResponse.status}`
					)
				}
				if (!audioResponse.body) {
					throw new Error('Audio response body is null')
				}

				// Ensure audioResponse.body is a Readable stream
				// node-fetch body is already a Readable stream type compatible with Node.js streams
				audioStream = audioResponse.body as unknown as Readable

				logger.info(
					`Uploading full audio to GCS as: ${fullAudioFileName}`
				)

				fullAudioGcsUri = await uploadStreamToGCS(
					audioStream,
					fullAudioFileName
				)
				logger.info(`Full audio uploaded to GCS: ${fullAudioGcsUri}`)
			} catch (downloadError: any) {
				logger.error(
					`Failed to download or upload audio from ${downloadUrl}:`,
					downloadError
				)
				await transcriptService.error(jobId)
				await pushTranscriptionEvent(
					jobId,
					`Audio faylni yuklashda xatolik: ${downloadError.message}`,
					false,
					broadcast
				)
			}
		}

		// --- Get Duration ---
		await pushTranscriptionEvent(
			jobId,
			'Ovoz davomiyligi aniqlanmoqda...',
			false,
			broadcast
		)
		await delay(200)

		let totalDuration: number
		try {
			totalDuration = await getAudioDuration(fullAudioGcsUri)
			if (totalDuration <= 0) {
				throw new Error('Determined audio duration is not positive.')
			}
			logger.info(`Video Duration: ${totalDuration}s`)
		} catch (durationError: any) {
			logger.error(
				`Could not determine audio duration for ${fullAudioGcsUri}:`,
				durationError
			)
			await transcriptService.error(jobId)
			await pushTranscriptionEvent(
				jobId,
				`Audio davomiyligini aniqlab bo'lmadi: ${durationError.message}`,
				true,
				broadcast
			)
			// Don't return yet, proceed to finally block for cleanup
			throw durationError // Throw to trigger the main catch block after cleanup
		}

		// --- Segmentation & Transcription ---
		const segmentDuration = 150 // 2.5 minutes
		const numSegments = Math.ceil(totalDuration / segmentDuration)
		await pushTranscriptionEvent(
			jobId,
			`Ovoz ${numSegments} qismga bo'linmoqda`,
			false,
			broadcast
		)
		await delay(500)

		await pushTranscriptionEvent(
			jobId,
			`Matnga o'girish boshlandi`,
			false,
			broadcast
		)

		const editedTexts: string[] = []
		let i = 0

		while (i < numSegments) {
			const segmentNumber = i + 1
			const segmentStartTime = i * segmentDuration
			const actualDuration = Math.min(
				segmentDuration,
				totalDuration - segmentStartTime
			)

			if (actualDuration <= 0) {
				logger.warn(
					`Skipping segment ${segmentNumber} due to zero or negative duration.`
				)
				i++
				continue
			}

			logger.info(
				`Processing segment ${segmentNumber}/${numSegments}, Start: ${segmentStartTime}s, Duration: ${actualDuration}s`
			)

			const destSegmentFileName = `segment_${jobId}_${i}.mp3`
			let segmentGcsUri: string | null = null

			// try {
			// --- Create Segment using ffmpeg from the *full* GCS audio ---
			logger.info(
				`Creating segment ${segmentNumber} using ffmpeg from ${fullAudioGcsUri}`
			)

			const fullAudioStream = await getGCSFileStream(fullAudioGcsUri)

			// Create a PassThrough stream to pipe ffmpeg output TO, and be read FROM by GCS upload
			const ffmpegOutputPassThrough = new PassThrough() // <--- CORRECT: Use PassThrough

			// Start the GCS upload immediately, it will read from the PassThrough stream
			const uploadPromise = uploadStreamToGCS(
				ffmpegOutputPassThrough, // Pass the PassThrough stream here
				destSegmentFileName
			)

			// Configure ffmpeg and pipe its output INTO the PassThrough stream
			ffmpeg(fullAudioStream) // Input from GCS stream
				.inputOption(`-ss ${segmentStartTime}`) // Start time
				.inputOption('-nostdin')
				.duration(actualDuration) // Duration of the segment
				.format('mp3')
				.audioCodec('libmp3lame')
				.audioQuality(2)
				.on('start', cmd =>
					logger.info(
						`FFmpeg command segment ${segmentNumber}: ${cmd}`
					)
				)
				.on('error', (err, stdout, stderr) => {
					logger.error(
						`FFmpeg error processing segment ${segmentNumber}:`
					)
					logger.error(
						{
							message: err.message,
							stack: err.stack,
							stdout: stdout,
							stderr: stderr,
							segmentStartTime,
							actualDuration
						},
						'FFmpeg Error Details:'
					)
					// Signal error TO the passthrough stream if ffmpeg fails
					// This will propagate the error to the uploadStreamToGCS reader
					ffmpegOutputPassThrough.destroy(err)
				})
				.on('end', () => {
					logger.info(
						`FFmpeg finished processing segment ${segmentNumber}`
					)
					// Signal the end of the stream TO the uploader via the PassThrough
					// When ffmpeg ends, we signal that no more data will be written
					// to the PassThrough stream. The GCS uploader reading from it
					// will then know the stream has ended.
					// Using .end() is often cleaner for PassThrough than .push(null)
					// ffmpegOutputPassThrough.end();
					// Although push(null) also works:
					ffmpegOutputPassThrough.push(null)
				})
				.pipe(ffmpegOutputPassThrough, { end: false }) // Pipe ffmpeg output INTO the PassThrough stream.
			// {end: false} prevents ffmpeg's pipe from automatically calling .end()
			// on the PassThrough, allowing our 'end' handler to do it explicitly.

			// Wait for the upload to complete
			segmentGcsUri = await uploadPromise
			logger.info(
				`Segment ${segmentNumber} uploaded to GCS: ${segmentGcsUri}`
			)

			// --- Transcription (Google) ---
			await pushTranscriptionEvent(
				jobId,
				`Google matnni o'girmoqda ${segmentNumber}/${numSegments}`,
				false,
				broadcast
			)

			let transcriptGoogle: string | null = null

			try {
				transcriptGoogle = await transcribeWithGoogle(segmentGcsUri)
			} catch (error) {
				logger.error(
					`Error during Google transcription for segment ${segmentNumber}:`,
					error
				)
				await pushTranscriptionEvent(
					jobId,
					`Google matnni o'girishda xatolik (${segmentNumber}/${numSegments})! Qayta urinilmoqda...`,
					false,
					broadcast
				)

				continue // Retry this segment
			}

			// --- Transcription (ElevenLabs) ---
			await pushTranscriptionEvent(
				jobId,
				`Elevenlabs matnni o'girmoqda ${segmentNumber}/${numSegments}`,
				false,
				broadcast
			)

			let transcriptElevenLabs: string | null = null

			try {
				const segmentStreamForElevenLabs =
					await getGCSFileStream(segmentGcsUri)
				transcriptElevenLabs = await transcribeAudioElevenLabs(
					segmentStreamForElevenLabs
				)
				if (!transcriptElevenLabs) {
					logger.warn(
						`ElevenLabs transcription returned empty for segment ${segmentNumber}. Retrying...`
					)
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi elevenlabs matnida xatolik yoki bo'sh javob! Qayta urinilmoqda...`,
						false,
						broadcast
					)
					await delay(1500)
					continue // Retry this segment
				}
			} catch (error) {
				logger.error(
					`Error during ElevenLabs transcription for segment ${segmentNumber}:`,
					error
				)
				await pushTranscriptionEvent(
					jobId,
					`Elevenlabs matnni o'girishda xatolik (${segmentNumber}/${numSegments})! Qayta urinilmoqda...`,
					false,
					broadcast
				)
				continue // Retry this segment
			}

			// --- Editing (Gemini) ---
			await pushTranscriptionEvent(
				jobId,
				`Matnni Gemini tahrirlamoqda ${segmentNumber}/${numSegments}...`,
				false,
				broadcast
			)

			try {
				const finalText = await editTranscribed(
					transcriptGoogle,
					transcriptElevenLabs
				)

				if (finalText) {
					editedTexts.push(finalText)
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi matn tayyor! Ovoz o'chirilmoqda...`,
						false,
						broadcast
					)
					await delay(500)
				} else {
					logger.warn(
						`Gemini editing returned empty for segment ${segmentNumber}. Retrying...`
					)
					await pushTranscriptionEvent(
						jobId,
						`Gemini tahririda xatolik (${segmentNumber}/${numSegments})! Qayta urinilmoqda...`,
						false,
						broadcast
					)
					await delay(1500)
					continue // Retry this segment
				}
			} catch (error) {
				logger.error(
					`Error during Gemini editing for segment ${segmentNumber}:`,
					error
				)

				await pushTranscriptionEvent(
					jobId,
					`Gemini tahririda xatolik (${segmentNumber}/${numSegments})! Qayta urinilmoqda...`,
					false,
					broadcast
				)
				continue // Retry this segment
			}

			// } catch (segmentErr) {
			// 	logger.error(
			// 		`Error processing segment ${segmentNumber}:`,
			// 		segmentErr
			// 	)
			// 	await pushTranscriptionEvent(
			// 		jobId,
			// 		`Segment ${segmentNumber}/${numSegments} da xatolik yuz berdi. Keyingisiga o'tilmoqda.`,
			// 		false,
			// 		broadcast
			// 	)
			// 	await delay(1000)
			// 	// Don't retry indefinitely, just log and continue to the next segment
			// 	// If we want retry logic for segment processing errors, it should be added here
			// } finally {
			// 	// --- Delete Segment File ---
			// 	if (segmentGcsUri) {
			// 		try {
			// 			logger.info(
			// 				`Deleting GCS file for segment ${segmentNumber}: ${segmentGcsUri}`
			// 			)
			// 			await deleteGCSFile(segmentGcsUri) // Use the URI directly
			// 		} catch (deleteErr) {
			// 			logger.error(
			// 				`Failed to delete segment ${segmentNumber} from GCS (${segmentGcsUri}):`,
			// 				deleteErr
			// 			)
			// 			// Log error but continue
			// 		}
			// 	}
			// }

			i++ // Move to the next segment
			await delay(200) // Small delay between segments
		} // End of while loop for segments

		// --- Combine final results ---
		if (editedTexts.length === 0 && numSegments > 0) {
			logger.error(
				`Job ${jobId} finished, but no segments were successfully transcribed.`
			)
			await transcriptService.error(jobId)
			await pushTranscriptionEvent(
				jobId,
				"Matn qismlarini o'girib bo'lmadi.",
				true,
				broadcast
			)
			// Don't return yet, proceed to finally block for cleanup
			throw new Error('No segments transcribed successfully.')
		}

		try {
			await userSession.completed(sessionId)
		} catch (err) {
			logger.warn(
				`Could not mark session as completed for sessionId=${sessionId}`,
				err
			)
		}

		await pushTranscriptionEvent(
			jobId,
			'Matn tayyorlanmoqda...',
			false,
			broadcast
		)
		await delay(500)

		const combinedResult = editedTexts
			.join('\n\n')
			.replace(/\(\(\((.*?)\)\)\)/g, '$1') // Keep the regex for now
		const duration = performance.now() - startTime

		await pushTranscriptionEvent(jobId, `Text jamlandi!`, false, broadcast)
		await delay(500)

		const finalTranscript = `<i style="display: block; font-style: italic; text-align: center;">🕒 Arginalni yozib chiqish uchun: ${formatDuration(duration)} vaqt ketdi!</i><h1 style="font-weight: 700; font-size: 1.8rem; margin: 1rem 0; text-align: center; line-height: 1;">${title}</h1>\n\n<p style="text-indent: 30px;">${convertToUzbekLatin(combinedResult)}</p>`

		await transcriptService.saveFinalTranscript(jobId, finalTranscript)
		logger.info(
			`Transcription job ${jobId} completed successfully in ${formatDuration(duration)}.`
		)

		// Send final SSE event
		await pushTranscriptionEvent(jobId, finalTranscript, true, broadcast)
	} catch (err: any) {
		// Catch errors from initial setup or unexpected loop errors
		logger.error(`runTranscriptionJob error for job ${jobId}:`, err)
		await transcriptService.error(jobId)
		await pushTranscriptionEvent(
			jobId,
			`Umumiy xatolik yuz berdi: ${err.message || String(err)}`,
			true,
			broadcast
		) // Mark as completed with error
	} finally {
		if (fullAudioGcsUri) {
			try {
				logger.info(`Deleting full audio GCS file: ${fullAudioGcsUri}`)
				await deleteGCSFile(fullAudioGcsUri) // Use URI for deletion
			} catch (deleteErr) {
				logger.error(
					`Failed to delete full audio file ${fullAudioGcsUri} from GCS:`,
					deleteErr
				)
			}
		} else if (fullAudioFileName) {
			logger.warn(
				`Full audio GCS URI was not set. Cannot delete file named: ${fullAudioFileName}. Manual cleanup might be required.`
			)
		}
	}
}
