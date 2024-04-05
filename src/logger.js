// logger.js

const { Kafka } = require('kafkajs');
const config = require('./config.js');

class Logger {
  constructor() {
    const logLevel = config.loggerLevel;
    this.kafka = new Kafka({
      logLevel,
    });
    this.logger = this.kafka.logger();
  }

  debug(message) {
    this.logger.debug(message);
  }

  info(message) {
    this.logger.info(message);
  }

  error(message) {
    this.logger.error(message);
  }
}

module.exports = new Logger();
