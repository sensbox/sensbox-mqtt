FROM node:lts

WORKDIR /srv/node-scripts/
# Bundle APP files
COPY package.json server.js utils.js ./

# Install app dependencies
ENV NPM_CONFIG_LOGLEVEL warn

RUN npm install --production

# Expose the listening port of your app
EXPOSE 1883

CMD [ "npm", "run", "start" ]
