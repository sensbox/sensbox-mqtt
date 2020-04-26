FROM node:lts

WORKDIR /srv/node-scripts/


# Install app dependencies
COPY package.json package.json 
COPY package-lock.json package-lock.json 
ENV NPM_CONFIG_LOGLEVEL warn

RUN npm ci

# Bundle APP files
COPY server.js utils.js ./



# Expose the listening port of your app
EXPOSE 1883

CMD [ "npm", "run", "start" ]
