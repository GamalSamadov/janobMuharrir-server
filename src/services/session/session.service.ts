import { PrismaClient } from '@prisma/client'

const prisma = new PrismaClient()

class SessionService {
	public async create(userId: string): Promise<string> {
		const session = await prisma.userSession.create({
			data: {
				user: {
					connect: {
						id: userId
					}
				}
			}
		})

		return session.id
	}

	public async completed(id: string) {
		await prisma.userSession.update({
			where: { id },
			data: { completed: true }
		})

		return true
	}
}

export const userSession = new SessionService()
