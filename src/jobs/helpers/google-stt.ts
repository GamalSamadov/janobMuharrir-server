import { v2 as speechV2 } from '@google-cloud/speech'
import { Storage } from '@google-cloud/storage'
import 'dotenv/config'
import { PassThrough, Readable, Writable } from 'stream'

import { logger } from '@/lib/logger'

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT_ID
const BUCKET_NAME = process.env.GOOGLE_CLOUD_BUCKET_NAME
const RECOGNIZER = process.env.GOOGLE_CLOUD_RECOGNIZER
const CLIENT_EMAIL = process.env.GOOGLE_CLOUD_CLIENT_EMAIL
let PRIVATE_KEY = process.env.GOOGLE_CLOUD_PRIVATE_KEY

if (PRIVATE_KEY) {
	PRIVATE_KEY = PRIVATE_KEY.replace(/\\n/g, '\n')
}

const storage = new Storage({
	projectId: PROJECT_ID,
	credentials: {
		client_email: CLIENT_EMAIL,
		private_key: PRIVATE_KEY
	}
})

export async function uploadStreamToGCS(
	stream: Writable | PassThrough,
	destFileName: string
): Promise<string> {
	const bucket = storage.bucket(BUCKET_NAME)
	const file = bucket.file(destFileName)
	await new Promise((resolve, reject) => {
		stream
			.pipe(file.createWriteStream())
			.on('finish', resolve)
			.on('error', reject)
	})
	return `gs://${BUCKET_NAME}/${destFileName}`
}

export async function getGCSFileStream(gcsUri: string): Promise<Readable> {
	const [bucketName, fileName] = gcsUri.replace('gs://', '').split('/', 2)
	const bucket = storage.bucket(bucketName)
	const file = bucket.file(fileName)
	return file.createReadStream()
}

export async function deleteGCSFile(fileName: string): Promise<void> {
	if (!BUCKET_NAME) {
		throw new Error(
			'Cannot delete file: GOOGLE_CLOUD_BUCKET_NAME is not configured.'
		)
	}
	if (
		!fileName ||
		typeof fileName !== 'string' ||
		fileName.trim().length === 0
	) {
		logger.error(
			`Invalid file name provided to deleteGCSFile: "${fileName}"`
		)
		throw new Error('A valid file name must be provided to delete.')
	}

	try {
		logger.info(
			`Attempting to delete GCS file "${fileName}" from bucket "${BUCKET_NAME}"`
		)
		const bucket = storage.bucket(BUCKET_NAME)
		const file = bucket.file(fileName)

		await file.delete()
		logger.info(
			`Successfully deleted GCS file "${fileName}" from bucket "${BUCKET_NAME}"`
		)
	} catch (err: any) {
		logger.error(
			`Failed to delete GCS file "${fileName}" from bucket "${BUCKET_NAME}": ${err.message || err}`,
			err
		)
		throw err
	}
}

export async function transcribeWithGoogle(audioUri: string) {
	const client = new speechV2.SpeechClient({
		projectId: PROJECT_ID,
		credentials: {
			client_email: CLIENT_EMAIL,
			private_key: PRIVATE_KEY
		}
	})

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
