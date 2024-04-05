const logger = require('./logger');

const config = {
    brokerList: process.env.KAFKA_BROKERS_LIST || 'broker:9092',
    maxBytesPerPartition: parseInt(process.env.FETCH_MESSAGE_MAX_BYTES) || 10485760,
    maxParallelHandles: parseInt(process.env.MAX_PARALLEL_HANDLES) || 2000,
    maxQueueSize: parseInt(process.env.MAX_QUEUE_SIZE) || 5000,
    retryTopic: process.env.RETRY_TOPIC || 'retry',
    heartbeatInterval: parseInt(process.env.HEARTBEAT_INTERVAL) || 6000,
    commitInterval: parseInt(process.env.COMMIT_INTERVAL) || 5000,
    loggerLevel: process.env.LOGGER_LEVEL || 'info',
    defaultGroupId: process.env.DEFAULT_GROUP_ID || 'test-group',
    fromBeginning: process.env.FROM_BEGINNING !== 'false', // assunmindo falso quando eh uma string
    acks: parseInt(process.env.ACKS) || 1
};

logger.debug(`Config: ${JSON.stringify(config)}`);

module.exports = config;
