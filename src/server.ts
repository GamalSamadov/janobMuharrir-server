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

import { IS_PRODUCTION } from './constants'

dotenv.config()

export const prisma = new PrismaClient()

const app = express()

async function run() {
	app.use(express.json())
	app.use(cookieParser())
	app.use(
		cors({
			origin: [process.env.CLIENT_URL, 'http://localhost:3000'],
			credentials: true
			// exposedHeaders: ['Set-Cookie']
		})
	)

	app.use('/api/auth', authController)
	app.use('/api/users', userController)
	app.use('/api/sessions', sessionController)
	app.use('/api/events', eventController)
	app.use('/api/transcripts', transcriptController)

	app.all('*', (req: Request, res: Response) => {
		res.status(404).json({
			message: `Route ${req.originalUrl} Not Found`
		})
	})
}

run()
	.then(async () => {
		await prisma.$connect()
		logger.info('Connected to database')

		if (IS_PRODUCTION) {
			logger.info(`Server is on production mode`)
		} else {
			logger.info(`Server is on development mode`)
		}

		const port = process.env.PORT || 4200
		app.listen(port, () => {
			logger.info(`Server is running on: http://localhost:${port}`)
		})
	})
	.catch(e => {
		logger.error(`Failed to connect to database ${e}`)
		process.exit(1)
	})
