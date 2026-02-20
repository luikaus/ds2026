ARG NODE_VERSION=24.13.1

FROM node:${NODE_VERSION}-alpine as build

ENV NODE_ENV=production

WORKDIR /usr/src/app

RUN --mount=type=bind,source=package.json,target=package.json \
    --mount=type=bind,source=package-lock.json,target=package-lock.json \
    --mount=type=cache,target=/root/.npm \
    npm ci

COPY . .

RUN npm run build

# Runner

FROM nginx:1.29.5-alpine as runner

COPY --from=build /usr/src/app/build /usr/share/nginx/html

