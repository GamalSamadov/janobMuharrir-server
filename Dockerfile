FROM node:20.12.2-slim AS build

WORKDIR /app

COPY package.json yarn.lock ./

ENV YOUTUBE_DL_SKIP_PYTHON_CHECK=1

RUN yarn install --frozen-lockfile

COPY . .

RUN yarn prisma generate

RUN yarn build

FROM node:20.12.2-slim AS production

ENV NODE_ENV="production"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /app/package.json ./package.json
COPY --from=build /app/yarn.lock ./yarn.lock
COPY --from=build /app/dist ./dist
COPY --from=build /app/prisma ./prisma
COPY --from=build /app/node_modules ./node_modules

EXPOSE 4200

CMD ["node", "dist/server.js"]