import ytdl from '@distube/ytdl-core'
import ffmpeg from 'fluent-ffmpeg'
import { performance } from 'perf_hooks'

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

const agent = ytdl.createAgent([
	{
		domain: '.youtube.com',
		expirationDate: 1234567890,
		hostOnly: false,
		httpOnly: true,
		name: 'cookie',
		path: '/',
		sameSite: 'no_restriction',
		secure: true,
		value: process.env.YOUTUBE_COOKIE || ''
	}
])

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
	const startTime = performance.now()

	await transcriptService.running(jobId)

	await delay(1000)

	const info = await ytdl.getInfo(url, { agent })
	const title = info.videoDetails.title
	const totalDuration = parseFloat(info.videoDetails.lengthSeconds)

	await transcriptService.updateTitle(jobId, title)

	await pushTranscriptionEvent(jobId, 'Ovoz yuklanmoqda', false, broadcast)
	await delay(500)

	const segmentDuration = 150 // 2.5 minutes
	const numSegments = Math.ceil(totalDuration / segmentDuration)
	await pushTranscriptionEvent(
		jobId,
		`Ovoz ${numSegments}ga taqsimlanmoqda`,
		false,
		broadcast
	)
	await delay(500)

	// TRANSCRIPTION

	await pushTranscriptionEvent(jobId, `Matnga o'g'rilmoqda`, false, broadcast)

	const editedTexts: string[] = []
	let i = 0

	try {
		while (i < numSegments) {
			const segmentNumber = i + 1
			const startTime = i * segmentDuration
			const actualDuration = Math.min(
				segmentDuration,
				totalDuration - startTime
			)

			// Stream segment from YouTube

			const segmentStream = ytdl(url, {
				agent,
				begin: `${startTime}s`
			})
			const ffmpegStream = ffmpeg(segmentStream)
				.format('mp3')
				.audioCodec('libmp3lame')
				.audioQuality(2)
				.duration(actualDuration)
				.on('start', cmd => logger.info('FFmpeg command:', cmd))
				.on('error', (err, stdout, stderr) => {
					logger.error(
						`FFmpeg error processing segment ${segmentNumber}:`
					)

					logger.error(
						{
							message: err.message,
							stack: err.stack,
							stdout: stdout,
							stderr: stderr
						},
						'FFmpeg Error Details:'
					)
				})
				.pipe()

			const destFileName = `segment_${jobId}_${i}.mp3`
			const gcsUri = await uploadStreamToGCS(ffmpegStream, destFileName)

			await pushTranscriptionEvent(
				jobId,
				`Google matnni o'girmoqda ${segmentNumber}/${numSegments}`,
				false,
				broadcast
			)

			try {
				const transcriptGoogle = await transcribeWithGoogle(gcsUri)

				if (!transcriptGoogle) {
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi google matnida xatolik yuz berdi!`,
						false,
						broadcast
					)

					await delay(500)

					await pushTranscriptionEvent(
						jobId,
						`Qaytadan google matnni o'girmoqda ${segmentNumber}/${numSegments}!`,
						false,
						broadcast
					)
					continue
				}

				// elevenlabs STT

				const segmentStreamForElevenLabs =
					await getGCSFileStream(gcsUri)
				const transcriptElevenLabs = await transcribeAudioElevenLabs(
					segmentStreamForElevenLabs
				)

				await pushTranscriptionEvent(
					jobId,
					`Elevenlabs matnni o'girmoqda ${segmentNumber}/${numSegments}`,
					false,
					broadcast
				)

				if (!transcriptElevenLabs) {
					await pushTranscriptionEvent(
						jobId,
						`${segmentNumber}/${numSegments}-chi elevenlabs matnida xatolik yuz berdi!`,
						false,
						broadcast
					)

					await delay(500)

					await pushTranscriptionEvent(
						jobId,
						`Qaytadan elevenlabs matnni o'girmoqda ${segmentNumber}/${numSegments}!`,
						false,
						broadcast
					)
					continue
				}

				await pushTranscriptionEvent(
					jobId,
					`Matnni Gemini tahrirlamoqda ${segmentNumber}/${numSegments}!`,
					false,
					broadcast
				)

				const finalText = await editTranscribed(
					transcriptGoogle,
					transcriptElevenLabs
				)
				if (finalText) {
					editedTexts.push(finalText)

					await pushTranscriptionEvent(
						jobId,
						`Ovoz o'chirilmoqda ${segmentNumber}/${numSegments}!`,
						false,
						broadcast
					)

					await delay(500)
				} else {
					await pushTranscriptionEvent(
						jobId,
						`Qaytadan Gemini tahrir qilmoqda ${segmentNumber}/${numSegments}!`,
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
			} finally {
				try {
					await deleteGCSFile(gcsUri)
				} catch (deleteErr) {
					logger.error('Failed to delete segment:', deleteErr)
				}
			}

			await pushTranscriptionEvent(
				jobId,
				`${segmentNumber}/${numSegments}-chi matn tayyor!`,
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

		const finalTranscript = `<i style="display: block; font-style: italic; text-align: center;">🕒Arginalni yozib chiqish uchun: ${formatDuration(duration)} vaqt ketdi!</i><h1 style="font-weight: 700; font-size: 1.8rem; margin: 1rem 0; text-align: center; line-height: 1;">${title}</h1>\n\n<p style="text-indent: 30px;">${convertToUzbekLatin(combinedResult)}</p>`

		await transcriptService.saveFinalTranscript(jobId, finalTranscript)

		// Send final SSE event
		await pushTranscriptionEvent(jobId, finalTranscript, true, broadcast)
	} catch (err) {
		logger.error('runTranscriptionJob error:', err)
		await transcriptService.error(jobId)
	}
}
