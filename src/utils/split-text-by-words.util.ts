export function splitTextBy(text: string, wordsNumber: number) {
	const words = text.split(' ')
	const chunks = []

	for (let i = 0; i < words.length; i += wordsNumber) {
		const chunk = words.slice(i, i + wordsNumber).join(' ')
		chunks.push(chunk)
	}

	return chunks
}
