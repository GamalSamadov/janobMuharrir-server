# ---- Build Stage ----
	FROM node:20 AS build

	# Install build dependencies: ffmpeg for potential native builds, python/pip for yt-dlp
	RUN apt-get update && \
			apt-get install -y --no-install-recommends \
			ffmpeg \
			python3 \
			python3-pip \
			&& rm -rf /var/lib/apt/lists/*
	
	# Install yt-dlp using pip
	RUN pip3 install --no-cache-dir yt-dlp
	
	WORKDIR /app
	
	COPY package.json yarn.lock ./
	
	# Install node modules (including devDependencies needed for build)
	RUN yarn install
	
	COPY . .
	
	# Run prisma generate if needed during build
	RUN yarn prisma generate
	
	# Build the application
	RUN yarn build
	
	
	# ---- Production Stage ----
	FROM node:20 AS production
	
	ENV NODE_ENV="production"
	
	# Install runtime dependencies: ffmpeg for transcoding, python for yt-dlp
	RUN apt-get update && \
			apt-get install -y --no-install-recommends \
			ffmpeg \
			python3 \
			python3-pip \
			&& rm -rf /var/lib/apt/lists/*
	
	# Install yt-dlp using pip
	RUN pip3 install --no-cache-dir yt-dlp
	
	WORKDIR /app
	
	# Copy necessary files from the build stage
	COPY --from=build /app/package.json ./package.json
	COPY --from=build /app/yarn.lock ./yarn.lock
	COPY --from=build /app/dist ./dist
	COPY --from=build /app/prisma ./prisma
	# Copy node_modules from build stage IF they are needed and `yarn install --production` is insufficient
	# COPY --from=build /app/node_modules ./node_modules
	
	# Install production node modules
	# Ensure this installs fluent-ffmpeg and other runtime deps correctly
	RUN yarn install --production --frozen-lockfile
	
	# Application Port
	EXPOSE 4200
	
	# Command to run the application
	CMD ["node", "dist/server.js"]