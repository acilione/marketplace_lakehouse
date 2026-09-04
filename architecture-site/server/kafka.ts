import type { KafkaJS } from "@confluentinc/kafka-javascript";

const kafkaBroker = process.env.KAFKA_BROKER ?? "kafka:9092";
const maximumBufferedEvents = 100;
const maximumValueBytes = 16 * 1024;
const seedEventsPerPartition = 12;
const topicCacheMs = 2_000;

export interface KafkaTopicSummary {
  readonly name: string;
  readonly partitions: number;
  readonly messages: number;
}

export interface KafkaEvent {
  readonly id: string;
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly timestamp: string | null;
  readonly observedAt: string;
  readonly key: string | null;
  readonly value: unknown;
  readonly sizeBytes: number;
}

interface TopicSnapshot {
  readonly topics: readonly KafkaTopicSummary[];
  readonly offsets: ReadonlyMap<string, readonly { partition: number; low: string; high: string }[]>;
}

const eventBuffers = new Map<string, KafkaEvent[]>();
let kafkaClient: KafkaJS.Kafka | undefined;
let consumer: KafkaJS.Consumer | undefined;
let monitorPromise: Promise<void> | undefined;
let observerStartedAt: string | null = null;
let cachedSnapshot: { readonly expiresAt: number; readonly value: TopicSnapshot } | undefined;

export function isObservableTopic(topic: string): boolean {
  return /^marketplace\.[a-z0-9.-]+$/.test(topic);
}

export function countPartitionMessages(low: string, high: string): number {
  const difference = BigInt(high) - BigInt(low);
  if (difference <= 0n) return 0;
  return Number(difference > BigInt(Number.MAX_SAFE_INTEGER) ? BigInt(Number.MAX_SAFE_INTEGER) : difference);
}

export function decodeKafkaValue(value: Buffer | null): unknown {
  if (value === null) return null;
  const bounded = value.subarray(0, maximumValueBytes).toString("utf8");
  const text = value.length > maximumValueBytes ? `${bounded}\n… value truncated at ${maximumValueBytes} bytes` : bounded;
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return text;
  }
}

async function getKafkaClient(): Promise<KafkaJS.Kafka> {
  if (kafkaClient) return kafkaClient;
  const { KafkaJS: clientLibrary } = await import("@confluentinc/kafka-javascript");
  kafkaClient = new clientLibrary.Kafka({
    kafkaJS: {
      brokers: [kafkaBroker],
      clientId: "marketplace-dashboard",
      connectionTimeout: 3_000,
      requestTimeout: 5_000,
      enforceRequestTimeout: true,
      logLevel: clientLibrary.logLevel.NOTHING,
      retry: { retries: 3, maxRetryTime: 5_000 },
    },
  });
  return kafkaClient;
}

function toIsoTimestamp(value: string): string | null {
  const timestamp = Number(value);
  return Number.isFinite(timestamp) && timestamp > 0 ? new Date(timestamp).toISOString() : null;
}

function rememberEvent({ topic, partition, message }: KafkaJS.EachMessagePayload): void {
  const event: KafkaEvent = {
    id: `${topic}:${partition}:${message.offset}`,
    topic,
    partition,
    offset: message.offset,
    timestamp: toIsoTimestamp(message.timestamp),
    observedAt: new Date().toISOString(),
    key: message.key?.subarray(0, 512).toString("utf8") ?? null,
    value: decodeKafkaValue(message.value),
    sizeBytes: message.size ?? message.value?.length ?? 0,
  };
  const events = eventBuffers.get(topic) ?? [];
  const duplicateIndex = events.findIndex(({ id }) => id === event.id);
  if (duplicateIndex >= 0) events.splice(duplicateIndex, 1);
  events.unshift(event);
  eventBuffers.set(topic, events.slice(0, maximumBufferedEvents));
}

async function loadTopicSnapshot(): Promise<TopicSnapshot> {
  if (cachedSnapshot && cachedSnapshot.expiresAt > Date.now()) return cachedSnapshot.value;
  const admin = (await getKafkaClient()).admin({ kafkaJS: { retry: { retries: 2, maxRetryTime: 4_000 } } });
  await admin.connect();
  try {
    const names = (await admin.listTopics()).filter(isObservableTopic).sort();
    const metadata = names.length ? await admin.fetchTopicMetadata({ topics: names }) : [];
    const offsetEntries = await Promise.all(names.map(async (name) => [name, await admin.fetchTopicOffsets(name)] as const));
    const offsets = new Map(offsetEntries);
    const topics = names.map((name) => ({
      name,
      partitions: metadata.find(({ name: metadataName }) => metadataName === name)?.partitions.length ?? 0,
      messages: (offsets.get(name) ?? []).reduce(
        (total, partition) => total + countPartitionMessages(partition.low, partition.high),
        0,
      ),
    }));
    const value = { topics, offsets };
    cachedSnapshot = { expiresAt: Date.now() + topicCacheMs, value };
    return value;
  } finally {
    await admin.disconnect();
  }
}

async function waitForAssignment(nextConsumer: KafkaJS.Consumer): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (nextConsumer.assignment().length > 0) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("Kafka observer did not receive a partition assignment.");
}

async function startMonitor(snapshot: TopicSnapshot): Promise<void> {
  if (!snapshot.topics.length) return;
  const nextConsumer = (await getKafkaClient()).consumer({
    kafkaJS: {
      groupId: "marketplace-dashboard-observer",
      allowAutoTopicCreation: false,
      fromBeginning: false,
      autoCommit: false,
      maxBytes: 2 * 1024 * 1024,
      maxBytesPerPartition: 512 * 1024,
      maxWaitTimeInMs: 1_000,
      sessionTimeout: 10_000,
      retry: { retries: 3, maxRetryTime: 5_000 },
    },
  });
  consumer = nextConsumer;
  await nextConsumer.connect();
  await nextConsumer.subscribe({ topics: snapshot.topics.map(({ name }) => name) });
  await nextConsumer.run({
    partitionsConsumedConcurrently: Math.min(6, snapshot.topics.reduce((sum, topic) => sum + topic.partitions, 0)),
    eachMessage: async (payload) => rememberEvent(payload),
  });
  await waitForAssignment(nextConsumer);

  for (const [topic, partitions] of snapshot.offsets) {
    for (const partition of partitions) {
      const low = BigInt(partition.low);
      const high = BigInt(partition.high);
      const start = high - low > BigInt(seedEventsPerPartition) ? high - BigInt(seedEventsPerPartition) : low;
      nextConsumer.seek({ topic, partition: partition.partition, offset: start.toString() });
    }
  }
  observerStartedAt = new Date().toISOString();
}

async function ensureMonitor(snapshot: TopicSnapshot): Promise<void> {
  if (!snapshot.topics.length) return;
  if (monitorPromise) return monitorPromise;
  monitorPromise = startMonitor(snapshot).catch(async (error: unknown) => {
    monitorPromise = undefined;
    observerStartedAt = null;
    if (consumer) await consumer.disconnect().catch(() => undefined);
    consumer = undefined;
    throw error;
  });
  return monitorPromise;
}

export async function getKafkaTopics(): Promise<{
  readonly topics: readonly KafkaTopicSummary[];
  readonly observerStartedAt: string | null;
}> {
  const snapshot = await loadTopicSnapshot();
  await ensureMonitor(snapshot);
  return { topics: snapshot.topics, observerStartedAt };
}

export async function getRecentKafkaEvents(topic: string, limit: number): Promise<{
  readonly topic: string;
  readonly events: readonly KafkaEvent[];
  readonly observerStartedAt: string | null;
}> {
  if (!isObservableTopic(topic)) throw new Error("Invalid marketplace topic name.");
  const snapshot = await loadTopicSnapshot();
  if (!snapshot.topics.some(({ name }) => name === topic)) throw new Error(`Unknown topic: ${topic}`);
  await ensureMonitor(snapshot);
  return { topic, events: (eventBuffers.get(topic) ?? []).slice(0, limit), observerStartedAt };
}

export async function shutdownKafka(): Promise<void> {
  if (!consumer) return;
  await consumer.disconnect().catch(() => undefined);
  consumer = undefined;
  monitorPromise = undefined;
  observerStartedAt = null;
}
