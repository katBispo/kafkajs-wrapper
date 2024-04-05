const async = require('async');
const kafkaClient = require('./client');
const config = require('./config');
const kafkaProducer = require('./producer');
const commitManager = require('./commitManager');

class Consumer {
  constructor() {
    this.ready = false;
    this.paused = false;
    this.kafkaConsumer = null;
    this.msgQueue = null;
  }

  async start({ groupId, topicsList, onData, onError, autoCommit }) {
    try {
      if (this.ready) return;

      if (!topicsList) throw new Error('Cannot start without a topic list');

      this.topicsList = topicsList;
      this.onData = onData || this.onData;
      this.onError = onError || this.onError;
      this.autoCommit = autoCommit || false;

      const consumerConfig = {
        groupId: groupId || config.DEFAULT_GROUP_ID,
        maxBytesPerPartition: config.maxBytesPerPartition,
        heartbeatInterval: config.heartbeatInterval,
        fromBeginning: config.fromBeginning
      };

      this.kafkaConsumer = kafkaClient.consumer(consumerConfig);
      this.kafkaConsumer.connect();
      topicsList.forEach((topic) => {
        this.kafkaConsumer.subscribe({ topic });
      });

      commitManager.start(this.kafkaConsumer, {});
      this.ready = true;
      console.log('Kafka consumer ready');

      await this.kafkaConsumer.run({
        eachBatch: this.onMessageBatch.bind(this)
      });
    } catch (error) {
      console.error('Error starting consumer:', error);
      throw error;
    }
  }

  async onMessageBatch({ batch, resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning }) {
    commitManager.setPartitionCBs({ partition: batch.partition, resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning });
    for (const message of batch.messages) {
      message.partition = batch.partition;
      message.topic = batch.topic;
      console.log(`Message received from Kafka, partition: ${message.partition}, offset: ${message.offset}`);
      if (config.maxParallelHandles) {
        this.processMessageQueue(message);
      } else {
        await this.handleMessage(message);
      }
    }
  }

  async handleMessage(data) {
    try {
      commitManager.notifyStartProcessing(data);
      await this.onData(data);
    } catch (error) {
      console.error(`Error handling message: ${error}`);
      data.headers = data.headers || {};
      data.headers.originalTopic = data.topic;
      await kafkaProducer.produce({
        topic: config.RETRY_TOPIC,
        messages: data
      });
    } finally {
      commitManager.notifyFinishedProcessing(data);
    }
  }

  processMessageQueue(message) {
    if (!this.msgQueue) {
      this.msgQueue = async.queue(async (data, done) => {
        await this.handleMessage(data);
        done();
      }, config.maxParallelHandles);
      this.msgQueue.drain(() => {
        if (this.paused) this.retryResume();
      });
    }
    this.msgQueue.push(message);
    if (this.msgQueue.length() > config.maxQueueSize && !this.paused) {
      this.pauseConsumer();
    }
  }

  pauseConsumer() {
    try {
      if (!this.autoCommit) commitManager.commitProcessedOffsets(true);
      this.kafkaConsumer.pause(this.topicsList.map((topic) => ({ topic })));
      this.paused = true;
      console.log(`Consumer paused for topics: ${JSON.stringify(this.topicsList)}`);
    } catch (error) {
      console.error('Error pausing consumer:', error);
    }
  }

  async retryResume() {
    const MAX_RETRIES = 5;
    let tryNum = 0;
    const interval = setInterval(async () => {
      tryNum++;
      if (tryNum > MAX_RETRIES) {
        console.error('Unable to resume consumption');
        process.kill(process.pid);
      }

      if (this.paused) {
        try {
          if (!this.autoCommit) await commitManager.commitProcessedOffsets(true);
          this.kafkaConsumer.resume(this.topicsList.map((topic) => ({ topic })));
          this.paused = false;
          console.log(`Resumed consumption for topics: ${JSON.stringify(this.topicsList)}`);
          clearInterval(interval);
        } catch (error) {
          console.error('Error resuming consumption:', error);
        }
      } else {
        clearInterval(interval);
      }
    }, 500);
  }

  onData(data) {
    console.log(`Handling received message with offset: ${data.offset}`);
    return Promise.resolve();
  }
}

module.exports = new Consumer();
