FROM node:20.2

WORKDIR /app

COPY . .

RUN npm install

EXPOSE 8080/tcp

CMD [ "node", "server.js" ]
