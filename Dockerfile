FROM node:20 AS build

RUN apt-get update && \
		apt-get install -y --no-install-recommends \
		ffmpeg \
		python3 \
		yt-dlp \
		&& rm -rf /var/lib/apt/lists/*
	
WORKDIR /app

COPY package.json yarn.lock ./

RUN yarn install

COPY . .

RUN yarn prisma generate

RUN yarn build

FROM node:20 AS production

ENV NODE_ENV="production"

ENV DATABASE_URL="postgresql://neondb_owner:npg_pFRLe2QmPsI8@ep-sweet-night-a5kqw2vo-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require"

ENV JWT_SECRET="7345(*&E"

ENV GOOGLE_GEMINI_API_KEY="AIzaSyAmQj1LqOYpEarV03zq5k2xH6xCq5KRh2o"

ENV GOOGLE_CLOUD_PROJECT_ID="janob-453400"
ENV GOOGLE_CLOUD_BUCKET_NAME="muharrir"
ENV GOOGLE_CLOUD_CLIENT_EMAIL="muharrir@janob-453400.iam.gserviceaccount.com"
ENV GOOGLE_CLOUD_RECOGNIZER="uzbek-recognizer"

ENV GOOGLE_CLOUD_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCu38xGpTUtrSrI\nVlf4+8dccmSfSy2P2vLb6aqI5mGxZ6stdV7yVts18KnYM5raVd9IwMk4Kt5AsvBA\n0by755zs0jwMdP8ebJLsqDTlP+EbT42vPtW3I0SEnSQbzkxwgmJIUXexxemHm9gB\n/PiU6T+XGAX8QnUHzwgI56rufVPT7yU8yj4MAptf8xrxhu8H8BpFS2ybVM+uhB/E\n8opacGAyoL83PVlSatxcsN/3i+bYNGlSzNBaDVbfe7FcnBr6R7cjvdXSfYAVe4xQ\nzmL7csqIDGgsgpFQR31tLnLBlW4xtkX5USCppobDoC3PGPGlak/GJfDMidR9Pc7H\nswsE2UNJAgMBAAECggEACh6gUxw/Wg/R0HsSpzFmxYZHJWKz1cXNVQWOHVL7p0kB\nAmXu4yY8lEADjCPcW7MpcyvY2Ru99c+FELMycUtDbP7zg6MHmmqdC/3l5bhPHgX6\nBICl3G25itUPIF7NVJtu9ZFlVE/IJlVR6DeL6Q4gZG169zCfVh7Yb698WEW/Yp20\n8SmZ2W0F7Avrp09+5EZcnrWRFOj20Kc+oaTNQYUS8RM9aL91zyXLO4nWIPRMQDs/\nbLHwCp+UlWVhdi0WgOTO2uEemqSFb2ZbCvkaSvEOA8MBGo/Mbr8CIKUqbFWgwILI\n322UWJB+E46HXksxW9fL+R0Za12F49fUBJ5VLFsLgwKBgQD077tYWeUBbC16dgA1\nln2qzFEde9BsOqPTF1zdB0J1hNEEsCo7dXnJn51oWcVGkVCsPH/e1r3Nj++7Xtwg\nmFk8iAWRuhfXO1LQ50Ztwt/cSW8feNEDIB8ppduNMtXWlMXBGAi14fxyFFCGivZr\n0vp4zhN8/Nb0d4eis6jEUY7+EwKBgQC2xeq/01XF5B9KEJf1GKpM1uTKSAx9iLrr\nC2H6QU2LOLey9+D1hga/0vnFhv/Am7ScqAvBLXlfmKdGYcARMwfXv3awc5VYICOB\nK3y8E+5MDO6Qp3t8qx2jA+MCE7kj1p4Bky/LVNI8GjoGL3moNjGdmULZTWyn+KKj\ny8op5e90swKBgQCs75aXCuFl37s8e5QqQdb4gMnEx2ahA8YnhJdMd5O05dGuPbOT\nx531fXzl8HwZFwCemxitfXEFclsRMCi72tUmp+NGwN2UWY5fcOuJE856+l47GlJ4\nonVeMnyRdRMGLoRxkUaVJLnY0f8I/5R9DLlMEjQwS1R5xus9NRwzgGI1gQKBgAux\nk/hb7BPyKXJSp3Y98r9hzFeIzovFkLfzDXy2auCqa05e/5yVy57AP8y8e2Z2T9Lq\nCX59i1Nrb+BX5rLzAeBy1oQpkPeHbt4Xyy45VDi3ZxgILyVlyUqhuAds/Z5mX8uY\nACnRLnY781Rr+yug9AGySY2fn4ELrlQueKu/k1dzAoGACXMv4aE7pi94Fh6+lfe7\nTJKx+GgyluZPi1j112b/WEg3v0Fxg20EwHAcQ7IRVuAMwwaScztRkE22FpxXTQUU\nMdQkPqvDaeJrLAjXmLZYo4OxNLADXKs7akC1jsWnKPg9oSBXfqfOq0YhTBNl8txf\ns6WPnUnDgKzbht1d7zJPg58=\n-----END PRIVATE KEY-----\n"

ENV ELEVENLABS_API_KEY="sk_22020008797fbc15932ed3e194ab4a39669dff2461846813"

RUN apt-get update && \
		apt-get install -y --no-install-recommends \
		ffmpeg \
		python3 \
		yt-dlp \
		&& rm -rf /var/lib/apt/lists/*


WORKDIR /app

COPY --from=build /app/package.json ./package.json
COPY --from=build /app/yarn.lock ./yarn.lock
COPY --from=build /app/dist ./dist
COPY --from=build /app/prisma ./prisma

RUN yarn install --production --frozen-lockfile

EXPOSE 4200

CMD ["node", "dist/server.js"]