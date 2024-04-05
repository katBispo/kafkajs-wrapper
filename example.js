const { consumer, producer } = require('./index');

const topicsList = ['test-topic'];
const groupId = 'test-group';

const onError = (error) => {
  console.error(`Error handling message: ${error}`);
};

const onData = async (message) => {
  try {
    console.log(`Handling message: ${message.value}`);
    await sleep(1000);
  } catch (error) {
    console.error(`Error handling message: ${error}`);
    throw error;
  }
};

const run = async () => {
  try {
    await producer.start();
    await consumer.start({ groupId, topicsList, autoCommit: false, onData, onError });

    await producer.produce({ topic: topicsList[0], messages: [{ value: 'WRAPPER MESSAGER 0333/04' }] });
  } catch (error) {
    console.error(`An error occurred: ${error}`);
  }
};

const sleep = (ms) => {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
};

run();
