const Consumer = require('./Consumer'); // Importe sua classe Consumer
const kafkaProducer = require('./producer'); // Importe o producer para verificar se a mensagem foi enviada ao retry_topic

// Mock onData function to simulate an error
const mockOnData = jest.fn().mockRejectedValue(new Error('Simulated error'));

// Mock Kafka Producer's produce method
kafkaProducer.produce = jest.fn();

describe('Consumer', () => {
  let consumer;

  beforeEach(() => {
    consumer = new Consumer();
    consumer.onData = mockOnData; // Substitua a função onData pela mock
  });

  afterEach(() => {
    jest.clearAllMocks(); // Limpe os mocks após cada teste
  });

  it('should send message to retry_topic on error', async () => {
    // Simule a chamada do método start com dados fictícios
    await consumer.start({
      groupId: 'test-group',
      topicsList: ['test-topic'],
      onData: mockOnData,
      autoCommit: false, // Garanta que o autoCommit seja false para evitar commits automáticos
    });

    // Simule a chegada de uma mensagem
    const mockMessage = { value: 'Test message' };
    await consumer.onMessageBatch({
      batch: {
        partition: 0,
        topic: 'test-topic',
        messages: [mockMessage],
      },
      resolveOffset: jest.fn(),
      commitOffsetsIfNecessary: jest.fn(),
      heartbeat: jest.fn(),
      isRunning: jest.fn(),
    });

    // Verifique se a mensagem foi enviada para o retry_topic
    expect(kafkaProducer.produce).toHaveBeenCalledWith({
      topic: 'retry',
      messages: mockMessage,
    });
  });
});
