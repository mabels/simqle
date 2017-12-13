FROM node:boron

COPY tsconfig.json tslint.json package.json /app/
COPY test /app/test
COPY lib /app/lib
RUN cd app && ls -l && npm run lint && npm test && npm install && npm run build

CMD ["node", "/app/dist/lib/server.js"]

