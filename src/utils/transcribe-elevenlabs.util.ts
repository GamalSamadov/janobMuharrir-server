import dotenv from 'dotenv'
import { ElevenLabsClient } from 'elevenlabs'
import fs from 'fs'

dotenv.config()

export const transcribeAudioElevenLabs = async (filePath: string) => {
	const client = new ElevenLabsClient({
		apiKey: process.env.ELEVENLABS_API_KEY
	})

	let transcription = await client.speechToText.convert({
		file: fs.createReadStream(filePath),
		model_id: 'scribe_v1',
		language_code: 'uzb'
	})

	return transcription.text
}
