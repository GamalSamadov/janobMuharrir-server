import dotenv from 'dotenv'
import { ElevenLabsClient } from 'elevenlabs'
import { Readable } from 'stream'

dotenv.config()

async function streamToBuffer(stream: Readable): Promise<Buffer> {
	const chunks: Buffer[] = []
	for await (const chunk of stream) {
		chunks.push(Buffer.from(chunk))
	}
	return Buffer.concat(chunks)
}

export const transcribeAudioElevenLabs = async (
	audioStream: Readable
): Promise<string> => {
	const client = new ElevenLabsClient({
		apiKey: process.env.ELEVENLABS_API_KEY
	})

	const audioBuffer = await streamToBuffer(audioStream)

	const audioBlob = new Blob([audioBuffer])

	const transcription = await client.speechToText.convert({
		file: audioBlob,
		model_id: 'scribe_v1',
		language_code: 'uzb'
	})

	return transcription.text
}
