FROM node:20 AS build

RUN apt-get update && apt-get install -y ffmpeg

WORKDIR /app

COPY package.json yarn.lock ./

RUN yarn install

COPY . .

RUN yarn prisma generate

RUN yarn build

FROM node:20 AS production

ENV NODE_ENV="production"

RUN apt-get update && apt-get install -y ffmpeg

WORKDIR /app

COPY --from=build /app/package.json .
COPY --from=build /app/yarn.lock .
COPY --from=build /app/dist ./dist
COPY --from=build /app/prisma ./prisma

RUN yarn install --production

EXPOSE 4200

CMD ["node", "dist/server.js"]
