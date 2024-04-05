const { Kafka } = require('kafkajs');
const config = require('./config');

class Client {

  constructor() {
    const clientConfig = {
      clientId: 'my-app',
      brokers: ['localhost:9092'],
      logLevel: config.loggerLevel
    }
    this.kafkaClient = new Kafka(clientConfig);
  }

}

// Singleton client instance
module.exports = (new Client()).kafkaClient;