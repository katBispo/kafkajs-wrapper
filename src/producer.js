const kafkaClient = require('./client');
const logger = require('./logger');
const config = require('./config');

class Producer {

  constructor() {
    this.ready = false;
    this.kafkaProducer = null;
  }

  async start() {
    try {
      if (this.ready) return;

      this.kafkaProducer = kafkaClient.producer();
      await this.kafkaProducer.connect();
      logger.info('Kafka Producer is ready');
      this.ready = true;
    }
    catch (error) {
      logger.error(`Error starting Kafka Producer: ${error}`);
      throw error;
    }
  }

  async produce({ topic, messages }) {
    try {
      if (!this.ready) {
        throw new Error('Producer not ready');
      }

      if (!Array.isArray(messages)) {
        messages = [messages];
      }

      if (messages.length === 0) {
        return;
      }

      messages = messages.map(({ value, headers, key }) => ({ value, headers, key }));

      await this.kafkaProducer.send({
        acks: config.acks,
        topic,
        messages
      });

      logger.debug(`Producer sent message to topic: ${topic}`);
    }
    catch (error) {
      logger.error(`Error sending message: ${error}`);
      throw error;
    }
  }

  async produceBulkMsg({ topic, messages }) {
    return this.produce({ topic, messages });
  }

}

module.exports = new Producer();
