# --- Build Stage ---
    FROM node:20.12.2-slim AS build

    # Install git only needed if you have git dependencies in package.json
    # RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
    
    WORKDIR /app
    
    COPY package.json yarn.lock ./
    
    # Use --frozen-lockfile for consistency and speed in CI/CD
    RUN yarn install --frozen-lockfile
    
    COPY . .
    
    # Generate Prisma client based on your schema
    RUN yarn prisma generate
    
    # Build the application
    RUN yarn build
    
    # --- Production Stage ---
    FROM node:20.12.2-slim AS production
    
    # Set environment to production
    ENV NODE_ENV="production"
    
    # Install only ffmpeg - youtube-dl-exec handles yt-dlp binary download
    RUN apt-get update && \
        apt-get install -y --no-install-recommends \
        ffmpeg \
        python3 \
        python3-pip \
        # Add any other essential runtime OS packages here (e.g., ca-certificates, dumb-init)
        ca-certificates \
        && rm -rf /var/lib/apt/lists/*
    
    # Set working directory
    WORKDIR /app
    
    # Copy necessary files from the build stage
    COPY --from=build /app/package.json ./package.json
    COPY --from=build /app/yarn.lock ./yarn.lock
    COPY --from=build /app/dist ./dist
    COPY --from=build /app/prisma ./prisma
    # Copy node_modules from build stage for production dependencies
    # This avoids reinstalling everything and leverages the build cache
    COPY --from=build /app/node_modules ./node_modules
    
    # Optional: Add a non-root user for security
    # RUN addgroup --system --gid 1001 nodejs
    # RUN adduser --system --uid 1001 nodejs
    # USER nodejs
    
    # Expose application port
    EXPOSE 4200
    
    # Command to run the application
    CMD ["node", "dist/server.js"]