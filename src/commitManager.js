const logger = require('insights-logger');

class CommitManager {
  constructor() {
    this.partitionsData = {};
    this.partitionCallbacks = {};
    this.lastCommitted = [];
  }

  start(kafkaConsumer, config) {
    this.kafkaConsumer = kafkaConsumer;
    this.commitInterval = config.commitInterval || 5000;
    if (!config.autoCommit) {
      setInterval(() => {
        this.commitProcessedOffsets();
      }, this.commitInterval);
    }

    this.kafkaConsumer.on(this.kafkaConsumer.events.COMMIT_OFFSETS, (data) => {
      logger.debug(`Commit ${JSON.stringify(data)}`);
    });
  }

  notifyStartProcessing(data) { 
    const { partition, offset, topic } = data;
    this.partitionsData[partition] = this.partitionsData[partition] || [];
    this.partitionsData[partition].push({ offset, topic, done: false });
  }

  notifyFinishedProcessing(data) {
    const { partition, offset } = data;
    this.partitionsData[partition] = this.partitionsData[partition] || [];
    const record = this.partitionsData[partition].find(record => record.offset === offset);
    if (record) {
      record.done = true;
    }
  }

  async commitProcessedOffsets() {
    try {
      const offsetsToCommit = [];
      for (const key in this.partitionsData) {
        const partition = parseInt(key);
        const partitionData = this.partitionsData[key];
        const lastProcessedRecord = partitionData.find(record => !record.done);
        if (lastProcessedRecord) {
          if (!this.partitionCallbacks[partition].isRunning()) break;
          await this.partitionCallbacks[partition].resolveOffset(lastProcessedRecord.offset);
          await this.partitionCallbacks[partition].commitOffsetsIfNecessary();
          const index = partitionData.indexOf(lastProcessedRecord);
          this.partitionsData[key] = partitionData.slice(index + 1);
          offsetsToCommit.push({ partition, offset: lastProcessedRecord.offset, topic: lastProcessedRecord.topic });
        }
      }

      this.lastCommitted = offsetsToCommit.length > 0 ? offsetsToCommit : this.lastCommitted;
      return Promise.resolve();
    } catch (error) {
      return Promise.reject(error);
    }
  }

  setPartitionCBs({ partition, resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning }) {
    this.partitionCallbacks[partition] = { resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning };
  }

  getLastCommitted() {
    return this.lastCommitted;
  }
}

module.exports = new CommitManager();
