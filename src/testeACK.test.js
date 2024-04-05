const Producer = require('./producer');
const config = require('./config');

// Mock Logger para evitar logs durante os testes
jest.mock('./logger', () => ({
  info: jest.fn(),
  debug: jest.fn(),
  error: jest.fn(),
}));

describe('Producer', () => {
  let producer;

  beforeEach(async () => {
    producer = Producer;
    await producer.start(); // Certifique-se de que o produtor está pronto antes de cada teste
  });

  afterEach(() => {
    jest.clearAllMocks(); // Limpar todos os mocks após cada teste
  });

  it('should produce a message with the correct ACK level', async () => {
    // Defina o comportamento esperado da produção de mensagem
    const mockMessage = { value: 'Test message' };
    const topic = 'test-topic';
    const ackLevel = config.acks; // Use o valor de ACKS definido na configuração

    // Mock da função send do kafkaProducer
    producer.kafkaProducer.send = jest.fn();

    // Chame a função de produção de mensagem (assíncrona) e aguarde sua conclusão
    await producer.produce({ topic, messages: mockMessage });

    // Verifique se a função de envio foi chamada com o nível de ACK correto
    expect(producer.kafkaProducer.send).toHaveBeenCalledWith({
      acks: ackLevel,
      topic,
      messages: [{ value: 'Test message', headers: undefined, key: undefined }]
    });

    // Verifique se a função de log de debug foi chamada com a mensagem correta
    expect(require('./logger').debug).toHaveBeenCalledWith(`Producer sent message to topic: ${topic}`);
  }, 10000); // Define o tempo limite do teste para 10 segundos

  // Teste adicional para verificar se o produtor não está pronto
  it('should throw an error if producer is not ready', async () => {
    producer.ready = false; // Definir o produtor como não pronto

    await expect(producer.produce({ topic: 'test-topic', messages: 'Test message' })).rejects.toThrow('Producer not ready');
  });
});
