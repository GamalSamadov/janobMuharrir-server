import pino from 'pino'

import { IS_PRODUCTION } from '@/constants'

export const logger = pino({
	level: process.env.LOG_LEVEL || 'info',
	transport: !IS_PRODUCTION
		? {
				target: 'pino-pretty',
				options: {
					colorize: true,
					translateTime: 'SYS:standard',
					ignore: 'pid,hostname'
				}
			}
		: undefined,
	formatters: {
		level: label => ({ level: label.toUpperCase() })
	},
	timestamp: pino.stdTimeFunctions.isoTime
})
