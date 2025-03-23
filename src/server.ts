import { PrismaClient } from '@prisma/client'
import cookieParser from 'cookie-parser'
import cors from 'cors'
import * as dotenv from 'dotenv'
import express, { Request, Response } from 'express'
import 'module-alias/register'

import {
	authController,
	eventController,
	sessionController,
	transcriptController,
	userController
} from '@/controllers'
import { logger } from '@/lib/logger'

dotenv.config()

export const prisma = new PrismaClient()

const app = express()

app.use(express.json())
app.use(cookieParser())

app.use(
	cors({
		origin: [process.env.CLIENT_URL, 'http://localhost:3000'],
		credentials: true,
		exposedHeaders: ['Set-Cookie']
	})
)

app.get('/test', (req: Request, res: Response) => {
	res.json({ message: 'Hello World' }).status(200)
})
app.use('/api/auth', authController)
app.use('/api/users', userController)
app.use('/api/sessions', sessionController)
app.use('/api/events', eventController)
app.use('/api/transcripts', transcriptController)

app.all('*', (req: Request, res: Response) => {
	res.status(404).json({ message: `Route ${req.originalUrl} Not Found` })
})

async function run() {
	try {
		await prisma.$connect()
		logger.info('Connected to database')
		const port = process.env.PORT || 4200
		app.listen(port, () => {
			logger.info(`Server is running on: http://localhost:${port}`)
		})
	} catch (e) {
		logger.error(`Failed to connect to database ${e}`)
		process.exit(1)
	}
}

run()

export default app
