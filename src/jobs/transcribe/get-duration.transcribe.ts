import { logger } from '@/lib/logger'
import { transcriptService } from '@/services/transcript/transcript.service'

import { getAudioDuration } from '../helpers'

import { pushTranscriptionEvent } from './push-transcription-event.transcribe'

export async function getDuration(
	fullAudioGcsUri: string | null,
	jobId: string,
	broadcast?: (content: string, completed: boolean) => void
) {
	let totalDuration: number
	try {
		totalDuration = await getAudioDuration(fullAudioGcsUri)
		if (totalDuration <= 0) {
			throw new Error('Determined audio duration is not positive.')
		}
		logger.info(`Video Duration: ${totalDuration}s`)

		return totalDuration
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
}
