FROM node:20.12.2-slim AS build

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json yarn.lock ./

RUN yarn install --frozen-lockfile

COPY . .

RUN yarn prisma generate

RUN yarn build

FROM node:20.12.2-slim AS production

ENV NODE_ENV="production"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg \
    python3 \
    python3-pip \
    && python3 -m pip install --no-cache-dir --upgrade pip \
    && python3 -m pip install --no-cache-dir --upgrade yt-dlp --break-system-packages \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /app

COPY --from=build /app/package.json ./package.json
COPY --from=build /app/yarn.lock ./yarn.lock
COPY --from=build /app/dist ./dist
COPY --from=build /app/prisma ./prisma

RUN yarn install --production --frozen-lockfile

EXPOSE 4200

CMD ["node", "dist/server.js"]