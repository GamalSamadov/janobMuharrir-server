import { PrismaClient, TranscriptionJobStatus } from '@prisma/client'

const prisma = new PrismaClient()

class TranscriptService {
	public async getAll() {
		return await prisma.transcriptionJob.findMany({
			include: {
				session: true
			}
		})
	}

	public async delete(id: string) {
		return await prisma.transcriptionJob.delete({
			where: {
				id
			}
		})
	}

	public async running(jobId: string) {
		return await prisma.transcriptionJob.update({
			where: { id: jobId },
			data: { status: TranscriptionJobStatus.RUNNING }
		})
	}

	public async completed(jobId: string) {
		return await prisma.transcriptionJob.update({
			where: { id: jobId },
			data: { status: TranscriptionJobStatus.COMPLETED }
		})
	}

	public async error(jobId: string) {
		return await prisma.transcriptionJob.update({
			where: { id: jobId },
			data: { status: TranscriptionJobStatus.ERROR }
		})
	}

	public async updateTitle(jobId: string, title: string) {
		return await prisma.transcriptionJob.update({
			where: { id: jobId },
			data: { downloadedTitle: title }
		})
	}

	public async saveFinalTranscript(jobId: string, finalTranscript: string) {
		return await prisma.transcriptionJob.update({
			where: { id: jobId },
			data: {
				status: TranscriptionJobStatus.COMPLETED,
				finalTranscript
			}
		})
	}
}

class TranscriptEventService {
	public async create(jobId: string, content: string, completed: boolean) {
		return await prisma.transcriptionEvent.create({
			data: {
				jobId,
				content,
				completed
			}
		})
	}
}

export const transcriptService = new TranscriptService()
export const transcriptEventService = new TranscriptEventService()
