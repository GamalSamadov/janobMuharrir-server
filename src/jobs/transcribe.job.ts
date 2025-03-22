import fs from 'fs'
import path from 'path'
import { performance } from 'perf_hooks'

import { logger } from '@/lib/logger'
import { userSession } from '@/services/session/session.service'
import {
	transcriptEventService,
	transcriptService
} from '@/services/transcript/transcript.service'
import { convertToUzbekLatin } from '@/utils/cryllic-to-latin.util'
import { downloadYoutubeAudio } from '@/utils/download-audio.util'
import { editTranscribed } from '@/utils/edit-transcribed .util'
import { formatDuration } from '@/utils/format-duration.util'
import { transcribeWithGoogle, uploadAudioToGCS } from '@/utils/google-stt.util'
import { splitMp3IntoSegments } from '@/utils/split-audio.util'
import { transcribeAudioElevenLabs } from '@/utils/transcribe-elevenlabs.util'

const delay = (ms: number) => new Promise(res => setTimeout(res, ms))

const audioFilesPath = path.resolve(__dirname, 'audios')

export async function pushTranscriptionEvent(
	jobId: string,
	content: string,
	completed = false,
	broadcast?: (content: string, completed: boolean) => void
) {
	await transcriptEventService.create(jobId, content, completed)

	if (broadcast) {
		broadcast(content, completed)
	}
}

export async function runTranscriptionJob(
	jobId: string,
	sessionId: string,
	url: string,
	broadcast?: (content: string, completed: boolean) => void
) {
	try {
		const startTime = performance.now()

		// Mark the job as RUNNING

		await transcriptService.running(jobId)

		// 1. DOWNLOAD AUDIO

		await pushTranscriptionEvent(
			jobId,
			'Ovoz yuklanmoqda',
			false,
			broadcast
		)

		const { outputPath, title: downloadedVideoTitle } =
			await downloadYoutubeAudio(url, audioFilesPath)

		await transcriptService.updateTitle(jobId, downloadedVideoTitle)

		await pushTranscriptionEvent(jobId, 'Ovoz yuklandi!', false, broadcast)

		await delay(500)

		// 2. SPLIT AUDIO

		await pushTranscriptionEvent(
			jobId,
			'Ovoz taqsimlanmoqda',
			false,
			broadcast
		)
		const { segments, outputDir } = await splitMp3IntoSegments(
			outputPath,
			audioFilesPath
		)

		await pushTranscriptionEvent(
			jobId,
			`Ovoz ${segments.length}ga taqsimlandi!`,
			false,
			broadcast
		)
		await delay(500)

		// 3. DELETE ORIGINAL

		fs.unlink(outputPath, () => {})

		await pushTranscriptionEvent(
			jobId,
			`Asl ovoz o'chirildi!`,
			false,
			broadcast
		)
		await delay(500)

		// 4. TRANSCRIPTION

		await pushTranscriptionEvent(
			jobId,
			`Matnga o'g'rilmoqda`,
			false,
			broadcast
		)
		const editedTexts: string[] = []
		let i = 0

		while (i < segments.length) {
			const segmentName = segments[i]
			const segmentPath = path.resolve(outputDir, `${segmentName}.mp3`)

			const segmentCount = segments.length
			const segmentNumber = i + 1

			await delay(1000)
			try {
				await pushTranscriptionEvent(
					jobId,
					`Matn ${segmentNumber}/${segmentCount}`,
					false,
					broadcast
				)

				// google STT
				const gcsUri = await uploadAudioToGCS(segmentPath)
				const transcriptGoogle = await transcribeWithGoogle(gcsUri)

				if (!transcriptGoogle) {
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${segmentCount}da xato!`,
						false,
						broadcast
					)

					await delay(500)

					await pushTranscriptionEvent(
						jobId,
						`Matn ${segmentNumber}/${segmentCount}`,
						false,
						broadcast
					)
					continue
				}

				await pushTranscriptionEvent(
					jobId,
					`Matn ${segmentNumber}/${segmentCount}`,
					false,
					broadcast
				)

				// elevenlabs STT

				const transcriptElevenLabs =
					await transcribeAudioElevenLabs(segmentPath)

				if (!transcriptElevenLabs) {
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${segmentCount}da xato!`,
						false,
						broadcast
					)

					await delay(500)

					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${segmentCount}da takror!`,
						false,
						broadcast
					)
					continue
				}

				await pushTranscriptionEvent(
					jobId,
					`Matn tahriri ${segmentNumber}/${segmentCount}`,
					false,
					broadcast
				)

				const finalText = await editTranscribed(
					transcriptGoogle,
					transcriptElevenLabs
				)
				if (finalText) {
					editedTexts.push(finalText)

					fs.unlink(segmentPath, () => {})

					await pushTranscriptionEvent(
						jobId,
						`Ovozni o'chirish ${segmentNumber}/${segmentCount}`,
						false,
						broadcast
					)

					await delay(500)

					await pushTranscriptionEvent(
						jobId,
						`Tahrir ${segmentNumber}/${segmentCount}`,
						false,
						broadcast
					)
				} else {
					await pushTranscriptionEvent(
						jobId,
						`Qayta tahrir ${segmentNumber}/${segmentCount}!`,
						false,
						broadcast
					)

					await delay(500)

					continue
				}
			} catch (err) {
				logger.error('Xatolik:', err)
				await transcriptService.error(jobId)

				continue
			}

			await pushTranscriptionEvent(
				jobId,
				`Matn tayyor ${segmentNumber}/${segmentCount}!`,
				false,
				broadcast
			)
			await delay(500)
			i++
		}

		// Mark session complete (assuming each session has a single job to do)
		try {
			await userSession.completed(sessionId)
		} catch (err) {
			logger.warn(
				`Could not mark session as completed for sessionId=${sessionId}`,
				err
			)
		}

		await pushTranscriptionEvent(jobId, 'Matn tayyor!', false, broadcast)

		await delay(500)

		// Combine final

		const combinedResult = editedTexts
			.join('\n\n')
			.replace(/\(\(\((.*?)\)\)\)/g, '$1')
		const duration = performance.now() - startTime

		await pushTranscriptionEvent(jobId, `Text jamlandi!`, false, broadcast)

		await delay(500)

		const finalTranscript = `<i style="display: block; font-style: italic; text-align: center;">🕒Arginalni yozib chiqish uchun: ${formatDuration(duration)} vaqt ketdi!</i><h1 style="font-weight: 700; font-size: 1.8rem; margin: 1rem 0; text-align: center; line-height: 1;">${downloadedVideoTitle}</h1>\n\n<p style="text-indent: 30px;">${convertToUzbekLatin(combinedResult)}</p>`

		await transcriptService.saveFinalTranscript(jobId, finalTranscript)

		// Send final SSE event
		await pushTranscriptionEvent(jobId, finalTranscript, true, broadcast)
	} catch (err) {
		logger.error('runTranscriptionJob error:', err)
		await transcriptService.error(jobId)
	}
}
