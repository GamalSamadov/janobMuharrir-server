import { GoogleGenerativeAI } from '@google/generative-ai'
import dotenv from 'dotenv'

dotenv.config()

const genAI = new GoogleGenerativeAI(process.env.GOOGLE_GEMINI_API_KEY)

const model = genAI.getGenerativeModel({
	model: 'gemini-2.0-flash-thinking-exp'
})

const prompt = `Men senga ikkta matn beraman. Ikkalasi ham bir xil matn. Birinchi matndagi o'zbekcha gaplarda xatolar bor, lekin arabcha so'zlari bor. Ikkinchi matnda esa o'zbekcha so'zlarda xato yo'q, biroq birinchisida bor bo'lgan arabcha gaplar va tinish belgilari yo'q. Sendan talab qiladiganim shuki: shu ikki matnni tekshirib chiqqan holda menga bir matn yozib ber. Sen yozadigan matningda birinchisidagi arabcha gaplar o'z holicha qolishi kerak, ammo o'zbekcha xato yozilgan gaplarni esa ikkinchi matndan olib qo'yib chiqishing kerak. Shunda senga nihoiy bir matn bo'ladi. Manashu nihoiy matnni ham to'g'irlashing kerak ushbu narsalarni rioya qilgan holda:\n- Arabcha so'zlarni o'zbekcha lotin harflar bilan to'g'ri yozishing kerak. Masalan: "Alhamdulillahi rabbil ʻalamin wa sallallahu ʻalā sayyidinā wa nabiyyinā Muhammad wa ʻalā ālihi wa ashabihi ajma'īn", bu xato. To'g'ri yozilishi: "Alhamdulillahi rabbil ʻalamin va sollallohu ala sayyidina va nabiyyina Muhammad va ʻala ā alihi va ashabihi ajma'iyn". Faqat o'zbekcha lotin harflari bilan yozilishi kerak: "ā", yoki "w" kabi harflar o'zbekchada yo'q.\n- O'zbekcha so'zlarni ham to'g'ri yozishing kerak. Masalan: "Ubay etdi", bu ham xato. To'g'ri yozilishi: "Ubay aytdi".\n- Javobing orasi boshqa birorta ham o'zingdan gap yozma, "Avvalo, keling, matnlarni birma-bir ko'rib chiqaylik", yoki: "Agar yana qandaydir savollaringiz yoki tuzatishlar bo'lsa, men doimo yordam berishga tayyorman", deb javob orasida umuman yozma. Shunchaki matnni yozib ber.\n\n`

export async function editTranscribed(
	googleText: string,
	elevenLabsText: string
) {
	const result = await model.generateContent(
		`${prompt}Birinchi matn:\n${elevenLabsText}\n\nIkkinchi matn:${googleText}`
	)

	return result.response.text()
}
