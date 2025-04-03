FROM node:20-slim AS build

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json yarn.lock ./

RUN yarn install --frozen-lockfileg

COPY . .

RUN yarn prisma generate

RUN yarn build

FROM node:20-slim AS production

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
COPY --from=build /app/node_modules/@prisma ./node_modules/@prisma
COPY --from=build /app/node_modules/prisma ./node_modules/prisma 
COPY --from=build /app/node_modules/.prisma ./node_modules/.prisma

RUN yarn install --production --frozen-lockfile

EXPOSE 4200

CMD ["node", "dist/server.js"]