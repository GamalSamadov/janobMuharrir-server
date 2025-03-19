import { PrismaClient } from '@prisma/client'

const prisma = new PrismaClient()

export class EventService {
	public async findMany(sessionId: string) {
		return await prisma.event.findMany({
			where: {
				sessionId
			},
			orderBy: { createdAt: 'asc' }
		})
	}

	public async eventsCount(sessionId: string) {
		return await prisma.event.count({ where: { sessionId } })
	}

	public async create(content: string, sessionId: string) {
		return await prisma.event.create({
			data: {
				content,
				sessionId
			}
		})
	}
}
