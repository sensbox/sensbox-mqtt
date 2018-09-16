module.exports = {
  apps : [{
    name: "MQTT server",
    script: "./server.js",
    // docker env config
    env: {
      NODE_ENV:      "development",
      DEBUG:         "sensbox:*"
    },
    // Set Env Vars if you are running on production
    env_production: {
      NODE_ENV:      "production"
    }
  }]
}
