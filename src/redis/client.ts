

export interface RedisConfig {
    url: string;
    enabled: boolean;
}

export interface RedisPipeline {
    zadd(key: string, nx: 'NX', score: number, member: string): this;
    zremrangebyscore(key: string, min: string | number, max: string | number): this;
    zcard(key: string): this;
    pexpire(key: string, ms: number): this;
    exec(): Promise<Array<[Error | null, unknown]>>;
}

export interface RedisClient {
    get(key: string): Promise<string | null>;
    set(key: string, value: string, options?: { ex?: number }): Promise<void>;
    exists(key: string): Promise<boolean>;
    close(): Promise<void>;
    // Sorted-set operations for SlidingWindowStore
    multi(): RedisPipeline;
    zcount(key: string, min: string | number, max: string | number): Promise<number>;
}

export interface RedisClientFactory {
    createClient(config: RedisConfig): Promise<RedisClient>;
}

class IORedisClient implements RedisClient {
    private client: import('ioredis').Redis;

    constructor(client: import('ioredis').Redis) {
        this.client = client;
    }

    async get(key: string): Promise<string | null> {
        return this.client.get(key) as Promise<string | null>;
    }

    async set(key: string, value: string, options?: { ex?: number }): Promise<void> {
        if (options?.ex) {
            await this.client.set(key, value, 'EX', options.ex);
        } else {
            await this.client.set(key, value);
        }
    }

    async exists(key: string): Promise<boolean> {
        const result = await this.client.exists(key);
        return result === 1;
    }

    async close(): Promise<void> {
        await this.client.quit();
    }

    multi(): RedisPipeline {
        const pipeline = this.client.multi();
        const wrapper: RedisPipeline = {
            zadd(key: string, nx: 'NX', score: number, member: string) {
                pipeline.zadd(key, 'NX', score, member);
                return wrapper;
            },
            zremrangebyscore(key: string, min: string | number, max: string | number) {
                pipeline.zremrangebyscore(key, min, max);
                return wrapper;
            },
            zcard(key: string) {
                pipeline.zcard(key);
                return wrapper;
            },
            pexpire(key: string, ms: number) {
                pipeline.pexpire(key, ms);
                return wrapper;
            },
            exec() {
                return pipeline.exec() as Promise<Array<[Error | null, unknown]>>;
            },
        };
        return wrapper;
    }

    async zcount(key: string, min: string | number, max: string | number): Promise<number> {
        return this.client.zcount(key, min, max);
    }
}

export class DefaultRedisClientFactory implements RedisClientFactory {
    async createClient(config: RedisConfig): Promise<RedisClient> {
        const { URL } = await import('url');
        const ioredis = await import('ioredis');
        const url = new URL(config.url);
        const port = parseInt(url.port ?? '6379', 10);
        const host = url.hostname ?? 'localhost';
        const password = url.password || undefined;

        const client = new ioredis.Redis(port, host, {
            password,
            lazyConnect: true,
            maxRetriesPerRequest: 3,
            enableReadyCheck: true,
            connectTimeout: 5000,
        });

        await client.connect();
        return new IORedisClient(client);
    }
}

let factory: RedisClientFactory = new DefaultRedisClientFactory();

export function setRedisClientFactory(f: RedisClientFactory): void {
    factory = f;
}

export function getRedisClientFactory(): RedisClientFactory {
    return factory;
}

export async function createRedisClient(config: RedisConfig): Promise<RedisClient> {
    return factory.createClient(config);
}

export class NoOpRedisClient implements RedisClient {
    async get(): Promise<string | null> {
        return null;
    }
    async set(): Promise<void> {
        return;
    }
    async exists(): Promise<boolean> {
        return false;
    }
    async close(): Promise<void> {
        return;
    }
    multi(): RedisPipeline {
        const noop: RedisPipeline = {
            zadd() { return noop; },
            zremrangebyscore() { return noop; },
            zcard() { return noop; },
            pexpire() { return noop; },
            async exec() { return []; },
        };
        return noop;
    }
    async zcount(): Promise<number> {
        return 0;
    }
}