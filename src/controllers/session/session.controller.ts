import { Request, Response, Router } from 'express'

import { authenticate } from '@/middlewares/auth.middleware'
import { userSession } from '@/services/session/session.service'

const router = Router()

const TRANSCRIBE_EDIT_PROMPT = `Men senga matn beraman. Matnni to'g'irlashing kerak ushbu narsalarni rioya qilgan holda:\n- Matndagi tinish belgilari to’g’irlanishi kerak. Masalan: “mana bu qur'on bizga ana shu sun'atlarni o'rgatadi qoidalarni o'rgatadi hayot nima ekanligini bildiradi”, quydagi tarzda to’g’irlanishi kerak: “mana bu qur'on bizga ana shu sunnatlarni o'rgatadi. Qoidalarni o'rgatadi. Hayot nima ekanligini bildiradi.”\n- O'zbekcha so'zlar to'g'ri yozishing kerak. Masalan: "Ubay etdi", bu xato. To'g'ri yozilishi: "Ubay aytdi".\n- Javobing orasi boshqa birorta ham o'zingdan gap yozma, "Avvalo, keling, matnlarni birma-bir ko'rib chiqaylik", yoki: "Agar yana qandaydir savollaringiz yoki tuzatishlar bo'lsa, men doimo yordam berishga tayyorman", deb javob orasida umuman yozma. Shunchaki matnni yozib ber.\n\nMatn:\n`

const EDIT_PROMPT = `Ushbu matnni o‘zbek tili grammatikasi: sintaksis va morfologiyasiga mos ravishda, qo‘yidagilarga rioya qilgan holda tahrir qilib bering:\n– Matnda arab tilida yozilgan iboralarni lotin harflari asosidagi o‘zbek alifbosida bexato yozing. Eslatma: o‘zbek alifbosida «w» harfi mavjud emas.\n– Matndagi ko‘chirma gaplarni qo‘shtirnoqlar bilan ajratib yozing.\n– Matnda keltirilgan voqea va hikoyalarni tushirib qoldirmasdan to‘liq yozing.\n– Matndagi imloviy xatolarni tuzating.\n– Jumlalar o‘rtasidagi mantiqiy ketmaketlikka rioya qiling.\n-Nihoiy matndagi har bir abzatzlarni ushbu HTML <p></p> tagi orasiga qo'yib yozib ber. <p>mana bu shaklda</p>\n\nMatn:\n`

router.post(
	'/start-transcribe',
	authenticate,
	async (req: Request, res: Response) => {
		const { url } = req.body as { url: string }

		if (!url) {
			res.status(400).json({ message: 'URL is required!' })
			return
		}

		const userId = req.user.id

		const sessionId = await userSession.create(userId)
		await userSession.updateURL(sessionId, url)
		await userSession.updatePrompt(sessionId, TRANSCRIBE_EDIT_PROMPT)

		res.json({ sessionId: sessionId })
	}
)

router.post(
	'/start-edit',
	authenticate,
	async (req: Request, res: Response) => {
		const { text, title } = req.body as {
			text: string
			title: string
		}

		if (!text) {
			res.status(400).json({ message: 'Text is required!' })
			return
		}

		if (!title) {
			res.status(400).json({ message: 'Title is required!' })
			return
		}

		const userId = req.user.id

		const sessionId = await userSession.create(userId)

		await userSession.updateText(sessionId, text)
		await userSession.updateTitle(sessionId, title)
		await userSession.updatePrompt(sessionId, EDIT_PROMPT)

		res.json({ sessionId: sessionId })
	}
)

export default router
