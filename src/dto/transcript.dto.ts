import { IsUrl } from 'class-validator'

export class TranscriptDto {
	@IsUrl()
	url: string
}
