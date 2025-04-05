import { Storage } from '@google-cloud/storage'
import dotenv from 'dotenv'
import ffmpeg from 'fluent-ffmpeg'
import { Readable } from 'stream'

import { logger } from '@/lib/logger'

dotenv.config()

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT_ID
const BUCKET_NAME = process.env.GOOGLE_CLOUD_BUCKET_NAME
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
	stream: Readable,
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
	logger.debug(`Creating read stream for GCS URI: ${gcsUri}`)
	const [bucketName, ...filePathParts] = gcsUri
		.replace('gs://', '')
		.split('/')
	const fileName = filePathParts.join('/')

	if (!bucketName || !fileName) {
		throw new Error(
			`Could not parse bucket/file name from GCS URI: "${gcsUri}"`
		)
	}

	const bucket = storage.bucket(bucketName)
	const file = bucket.file(fileName)
	return file.createReadStream()
}

export async function deleteGCSFile(gcsUri: string): Promise<void> {
	if (!gcsUri || !gcsUri.startsWith('gs://')) {
		logger.error(`Invalid GCS URI provided to deleteGCSFile: "${gcsUri}"`)
		throw new Error('A valid GCS URI (gs://bucket/file) must be provided.')
	}

	const [bucketName, ...filePathParts] = gcsUri
		.replace('gs://', '')
		.split('/')
	const fileName = filePathParts.join('/')

	if (!bucketName || !fileName) {
		logger.error(
			`Could not parse bucket name or file name from GCS URI: "${gcsUri}"`
		)
		throw new Error('Invalid GCS URI format.')
	}
	// ... rest of the existing delete logic using bucketName and fileName ...

	try {
		logger.info(
			`Attempting to delete GCS file "${fileName}" from bucket "${bucketName}"`
		)
		const bucket = storage.bucket(bucketName)
		const file = bucket.file(fileName)

		await file.delete()
		logger.info(
			`Successfully deleted GCS file "${fileName}" from bucket "${bucketName}"`
		)
	} catch (err: any) {
		logger.error(
			`Failed to delete GCS file "${fileName}" from bucket "${bucketName}": ${err.message || err}`,
			err
		)
		throw err
	}
}

export async function getAudioDuration(gcsUri: string): Promise<number> {
	logger.info(`Probing audio duration for GCS URI: ${gcsUri}`)

	if (!gcsUri || !gcsUri.startsWith('gs://')) {
		throw new Error(`Invalid GCS URI provided: "${gcsUri}"`)
	}

	try {
		// 1. Parse Bucket and File Name
		const [bucketName, ...filePathParts] = gcsUri
			.replace('gs://', '')
			.split('/')
		const fileName = filePathParts.join('/')

		if (!bucketName || !fileName) {
			throw new Error(
				`Could not parse bucket/file name from GCS URI: "${gcsUri}"`
			)
		}

		// 2. Generate a Signed URL
		logger.debug(`Generating signed URL for gs://${bucketName}/${fileName}`)
		const options = {
			version: 'v4' as const, // Use v4 for better security and features
			action: 'read' as const,
			expires: Date.now() + 15 * 60 * 1000 // 15 minutes expiration - should be enough for ffprobe
		}

		const [signedUrl] = await storage
			.bucket(bucketName)
			.file(fileName)
			.getSignedUrl(options)

		logger.debug(
			`Generated Signed URL (valid for 15 min): ${signedUrl.substring(0, 100)}...`
		) // Log truncated URL

		// 3. Use the Signed URL with ffprobe
		return new Promise((resolve, reject) => {
			ffmpeg.ffprobe(signedUrl, (err, metadata) => {
				// <-- Pass the URL string here
				if (err) {
					// Check if the error might be due to the stream ending unexpectedly (common with signed URLs if ffprobe is slow)
					if (
						err.message.includes('Server returned 4XX') ||
						err.message.includes('Input/output error')
					) {
						logger.warn(
							`ffprobe potentially failed due to signed URL expiry or access issue for ${gcsUri}. Error: ${err.message}`
						)
					} else {
						logger.error(
							`ffprobe error using signed URL for ${gcsUri}:`,
							err
						)
					}
					return reject(
						new Error(
							`Failed to probe audio duration via signed URL: ${err.message}`
						)
					)
				}
				if (metadata?.format?.duration) {
					logger.info(
						`Duration found: ${metadata.format.duration}s for ${gcsUri}`
					)
					resolve(Number(metadata.format.duration))
				} else {
					logger.error(
						`Could not find duration in ffprobe metadata using signed URL for ${gcsUri}:`,
						metadata
					)
					reject(
						new Error(
							'Could not determine audio duration from metadata'
						)
					)
				}
			})
		})
	} catch (error: any) {
		logger.error(`Error generating signed URL or probing ${gcsUri}:`, error)
		// Make error message more specific if possible
		if (error.message.includes('Could not find bucket')) {
			throw new Error(`GCS Bucket not found for URI: ${gcsUri}`)
		}
		if (error.message.includes('No such object')) {
			throw new Error(`GCS Object not found: ${gcsUri}`)
		}
		if (error.message.includes('permission')) {
			logger.error(
				"Permission error likely related to generating Signed URL. Ensure service account has 'roles/storage.objectViewer' or equivalent permissions."
			)
			throw new Error(
				`Permission error accessing GCS object or creating signed URL for ${gcsUri}`
			)
		}
		throw error // Re-throw the original or a more specific error
	}
}
