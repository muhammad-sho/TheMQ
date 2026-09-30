# ---- Build stage ----
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json tsconfig.build.json ./
COPY src ./src
# Runtime needs plain JS only: skip declarations and sourcemaps.
RUN npm run build:docker

# ---- Runtime stage ----
FROM node:22-alpine AS runtime
ENV NODE_ENV=production
# Docker-aware default: Compose service name on the app network.
# Override with REDIS_URL for external or non-Compose deployments.
ENV REDIS_URL=redis://redis:6379
WORKDIR /app
COPY package.json package-lock.json ./
# Prod dependencies only; the lockfile is build input, not runtime cargo.
RUN npm ci --omit=dev && npm cache clean --force && rm package-lock.json
COPY --from=build /app/dist ./dist
EXPOSE 3000
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.API_PORT||'3000')+'/health/live').then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"
# Bounded heap for small boxes (tune with the Redis maxmemory share).
CMD ["node", "--max-old-space-size=256", "dist/index.js"]
