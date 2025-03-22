import ffmpegInstaller from '@ffmpeg-installer/ffmpeg'
import ffprobeInstaller from '@ffprobe-installer/ffprobe'
import ffmpeg from 'fluent-ffmpeg'
import fs from 'fs'
import path from 'path'

import { logger } from '@/lib/logger'

ffmpeg.setFfmpegPath(ffmpegInstaller.path)
ffmpeg.setFfprobePath(ffprobeInstaller.path)

interface SplitResult {
	segments: string[]
	outputDir: string
}

export function splitMp3IntoSegments(
	inputFilePath: string,
	outputDirectory: string
): Promise<SplitResult> {
	const { name: fileName } = path.parse(inputFilePath)
	return new Promise((resolve, reject) => {
		try {
			if (!fs.existsSync(inputFilePath)) {
				throw new Error(`Input file not found: ${inputFilePath}`)
			}
			const stats = fs.statSync(inputFilePath)
			if (!stats.isFile()) {
				throw new Error(`Input path is not a file: ${inputFilePath}`)
			}

			if (!fs.existsSync(outputDirectory)) {
				fs.mkdirSync(outputDirectory, { recursive: true })
			}

			const beforeFiles = new Set(fs.readdirSync(outputDirectory))

			const segmentTime = 150 // 2.5 minutes in seconds
			const outputPattern = path.join(
				outputDirectory,
				`${fileName}_%03d.mp3`
			) // e.g., 'audio_%03d.mp3'

			ffmpeg(inputFilePath)
				.outputOptions([
					'-f',
					'segment',
					'-segment_time',
					String(segmentTime),
					'-c',
					'copy'
				])
				.output(outputPattern)
				.on('end', () => {
					try {
						const afterFiles = fs.readdirSync(outputDirectory)
						const newFiles = afterFiles.filter(
							file => !beforeFiles.has(file)
						)
						const segments = newFiles
							.filter(
								file =>
									file.startsWith(fileName + '_') &&
									file.endsWith('.mp3')
							)
							.sort((a, b) =>
								a.localeCompare(b, undefined, { numeric: true })
							)
						const segmentNamesWithoutExt = segments.map(
							file => path.parse(file).name
						)
						resolve({
							segments: segmentNamesWithoutExt,
							outputDir: outputDirectory
						})
					} catch (error) {
						reject(
							new Error(
								`Failed to read output directory: ${error.message}`
							)
						)
					}
				})
				.on('error', error => {
					reject(
						new Error(`FFmpeg processing error: ${error.message}`)
					)
				})
				.run()
		} catch (error) {
			reject(error instanceof Error ? error : new Error(String(error)))
		}
	})
}
