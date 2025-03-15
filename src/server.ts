import { PrismaClient } from '@prisma/client'
import cookieParser from 'cookie-parser'
import cors from 'cors'
import * as dotenv from 'dotenv'
import express, { Request, Response } from 'express'

import { logger } from '@/lib/logger'

import { authController, userController } from './controllers'

dotenv.config()

export const prisma = new PrismaClient()

const app = express()

async function main() {
	app.use(express.json())
	app.use(cookieParser())

	app.use(
		cors({
			origin: [process.env.CLIENT_URL || 'http://localhost:3000'],
			credentials: true,
			exposedHeaders: ['Set-Cookie']
		})
	)

	app.use('/api', authController)
	app.use('/api', userController)

	app.all('*', (req: Request, res: Response) => {
		res.status(404).json({ message: `Route ${req.originalUrl} Not Found` })
	})

	const port = process.env.PORT || 4200

	app.listen(port, () => {
		logger.info(`Server is running on: http://localhost:${port}`)
	})
}

main()
	.then(async () => {
		logger.info('Connected to database')
		await prisma.$connect()
	})
	.catch(async e => {
		logger.error(`Failed to connect to database ${e}`)
		console.error(e)
		await prisma.$disconnect()
		process.exit(1)
	})
