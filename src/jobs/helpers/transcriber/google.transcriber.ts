import { v2 as speechV2 } from '@google-cloud/speech'
import dotenv from 'dotenv'

dotenv.config()

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT_ID
const RECOGNIZER = process.env.GOOGLE_CLOUD_RECOGNIZER
const CLIENT_EMAIL = process.env.GOOGLE_CLOUD_CLIENT_EMAIL
let PRIVATE_KEY = process.env.GOOGLE_CLOUD_PRIVATE_KEY

if (PRIVATE_KEY) {
	PRIVATE_KEY = PRIVATE_KEY.replace(/\\n/g, '\n')
}

const client = new speechV2.SpeechClient({
	projectId: PROJECT_ID,
	credentials: {
		client_email: CLIENT_EMAIL,
		private_key: PRIVATE_KEY
	}
})

export async function transcribeWithGoogle(audioUri: string) {
	const request = {
		recognizer: `projects/${PROJECT_ID}/locations/global/recognizers/${RECOGNIZER}`,
		config: {
			autoDecodingConfig: {},
			languageCodes: ['uz-UZ'],
			model: 'long'
		},
		files: [{ uri: audioUri }],
		recognitionOutputConfig: {
			inlineResponseConfig: {}
		}
	}

	const [operation] = await client.batchRecognize(request)

	const [response] = await operation.promise()

	if (
		!response.results ||
		!response.results[audioUri] ||
		!response.results[audioUri].transcript
	) {
		throw new Error('No transcript was returned in the response')
	}

	const transcripts = []

	for (const result of response.results[audioUri].transcript.results || []) {
		const alternatives = result?.alternatives

		for (const speech of alternatives) {
			transcripts.push(speech.transcript)
		}
	}

	const transcript = transcripts.join(' ')

	return transcript
}
