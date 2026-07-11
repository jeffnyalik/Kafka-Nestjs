export interface KafkaModuleOptions {
    clientId: string;
    brokers: string[];
    groupId?: string;
    retry?: {
        retries: number;
        initialRetryTime: number;
    };
}
