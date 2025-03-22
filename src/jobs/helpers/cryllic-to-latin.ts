const apostrophe = '\u02BB'

const cyrillicToLatin: { [key: string]: string } = {
	А: 'A',
	а: 'a',
	Б: 'B',
	б: 'b',
	В: 'V',
	в: 'v',
	Г: 'G',
	г: 'g',
	Д: 'D',
	д: 'd',
	Е: 'E',
	е: 'e',
	Ё: 'Yo',
	ё: 'yo',
	Ж: 'J',
	ж: 'j',
	З: 'Z',
	з: 'z',
	И: 'I',
	и: 'i',
	Й: 'Y',
	й: 'y',
	К: 'K',
	к: 'k',
	Л: 'L',
	л: 'l',
	М: 'M',
	м: 'm',
	Н: 'N',
	н: 'n',
	О: 'O',
	о: 'o',
	П: 'P',
	п: 'p',
	Р: 'R',
	р: 'r',
	С: 'S',
	с: 's',
	Т: 'T',
	т: 't',
	У: 'U',
	у: 'u',
	Ф: 'F',
	ф: 'f',
	Х: 'X',
	х: 'x',
	Ц: 'S',
	ц: 's', // Note: 'Ts' might be used in some contexts, but 'S' is common
	Ч: 'Ch',
	ч: 'ch',
	Ш: 'Sh',
	ш: 'sh',
	Щ: 'Sh',
	щ: 'sh', // Often simplified to 'Sh' in Uzbek
	Ъ: "'",
	ъ: "'", // Regular apostrophe for hard sign (context-dependent)
	Ы: 'I',
	ы: 'i',
	Ь: "'",
	ь: "'", // Regular apostrophe for soft sign (context-dependent)
	Э: 'E',
	э: 'e',
	Ю: 'Yu',
	ю: 'yu',
	Я: 'Ya',
	я: 'ya',
	Ғ: `G${apostrophe}`,
	ғ: `g${apostrophe}`, // Correct Gʻ/gʻ
	Қ: 'Q',
	қ: 'q',
	Ҳ: 'H',
	ҳ: 'h',
	Ў: `O${apostrophe}`,
	ў: `o${apostrophe}` // Correct Oʻ/oʻ
}

export function convertToUzbekLatin(text: string): string {
	const result: string[] = []
	for (const char of text) {
		result.push(cyrillicToLatin[char] || char)
	}
	let latinText = result.join('')

	latinText = latinText
		.replace(/O'/g, `O${apostrophe}`) // O' → Oʻ
		.replace(/o'/g, `o${apostrophe}`) // o' → oʻ
		.replace(/G'/g, `G${apostrophe}`) // G' → Gʻ
		.replace(/g'/g, `g${apostrophe}`) // g' → gʻ

	return latinText
}
