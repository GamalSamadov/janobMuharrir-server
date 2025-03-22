import ytdl from '@distube/ytdl-core'
import ffmpegInstaller from '@ffmpeg-installer/ffmpeg'
import ffprobeInstaller from '@ffprobe-installer/ffprobe'
import ffmpeg from 'fluent-ffmpeg'
import fs from 'fs'
import path from 'path'
import { v4 as uuidv4 } from 'uuid'

ffmpeg.setFfmpegPath(ffmpegInstaller.path)
ffmpeg.setFfprobePath(ffprobeInstaller.path)

export async function downloadYoutubeAudio(
	url: string,
	baseOutputPath: string
) {
	const randomId = uuidv4()

	try {
		const info = await ytdl.getInfo(url)
		const title = info.videoDetails.title || 'Unknown Title'

		if (!fs.existsSync(baseOutputPath)) {
			fs.mkdirSync(baseOutputPath, { recursive: true })
		}

		const outputPath = path.resolve(baseOutputPath, `video-${randomId}.mp3`)

		await new Promise<void>((resolve, reject) => {
			const audioStream = ytdl(url, {
				filter: 'audioonly',
				quality: 'highestaudio',
				highWaterMark: 1 << 25 // 32 MB buffer
			})

			audioStream.on('error', err => {
				reject(err)
			})

			ffmpeg(audioStream)
				.audioBitrate(128)
				.format('mp3')

				.on('error', err => {
					reject(err)
				})
				.on('end', () => {
					resolve()
				})
				.save(outputPath)
		})

		return { outputPath, title }
	} catch (error: any) {
		throw new Error(`Download failed: ${error.message}`)
	}
}
