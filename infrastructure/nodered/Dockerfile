FROM nodered/node-red:latest

USER root

# Copy package.json with dependencies
COPY data/package.json /data/package.json

# Install dependencies
RUN cd /data && npm install

USER node-red

# Start Node-RED
CMD ["node-red"]
