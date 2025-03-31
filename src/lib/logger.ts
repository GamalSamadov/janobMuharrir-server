import pino from 'pino'

export const logger = pino({
	level: process.env.LOG_LEVEL || 'info',
	transport: {
		target: 'pino-pretty',
		options: {
			colorize: true,
			translateTime: 'SYS:standard',
			ignore: 'pid,hostname'
		}
	},
	formatters: {
		level: label => ({ level: label.toUpperCase() })
	},
	timestamp: pino.stdTimeFunctions.isoTime
})
