export const kafkaConfig = {
    clientId: 'ticket-platform',
    brokers: ['localhost:9092'],
    retry: {
        retries: 2,
        initialRetryTime: 100,
    },
};