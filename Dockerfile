FROM node:11

WORKDIR /usr/app

COPY package.json .
RUN npm install --quiet

COPY . .
RUN chmod +x src/bin/processor

ENTRYPOINT [ "src/bin/processor" ]