import { useCallback, useEffect, useMemo, useState } from "react";

interface KafkaTopic {
  readonly name: string;
  readonly partitions: number;
  readonly messages: number;
}

interface KafkaEvent {
  readonly id: string;
  readonly partition: number;
  readonly offset: string;
  readonly timestamp: string | null;
  readonly observedAt: string;
  readonly key: string | null;
  readonly value: unknown;
  readonly sizeBytes: number;
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(path);
  const document = await response.json() as T & { error?: string };
  if (!response.ok) throw new Error(document.error ?? `Request failed with status ${response.status}.`);
  return document;
}

function formatCount(value: number): string {
  return new Intl.NumberFormat("en", { notation: value > 999_999 ? "compact" : "standard" }).format(value);
}

function eventName(value: unknown): string {
  if (value && typeof value === "object" && "event_type" in value && typeof value.event_type === "string") {
    return value.event_type;
  }
  return "Kafka record";
}

function eventBody(value: unknown): string {
  return typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

function formatTimestamp(value: string | null): string {
  if (!value) return "NO TIMESTAMP";
  return new Intl.DateTimeFormat("en", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
    hour12: false,
  }).format(new Date(value));
}

export function KafkaMonitor() {
  const [topics, setTopics] = useState<readonly KafkaTopic[]>([]);
  const [selectedTopic, setSelectedTopic] = useState<string>("");
  const [events, setEvents] = useState<readonly KafkaEvent[]>([]);
  const [observerStartedAt, setObserverStartedAt] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const loadTopics = useCallback(async () => {
    try {
      const document = await getJson<{ topics: readonly KafkaTopic[]; observerStartedAt: string | null }>("./api/kafka/topics");
      setTopics(document.topics);
      setObserverStartedAt(document.observerStartedAt);
      setSelectedTopic((current) => {
        if (document.topics.some(({ name }) => name === current)) return current;
        return document.topics.reduce<KafkaTopic | undefined>(
          (mostActive, topic) => !mostActive || topic.messages > mostActive.messages ? topic : mostActive,
          undefined,
        )?.name ?? "";
      });
      setError(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Kafka observer is unavailable.");
    } finally {
      setLoading(false);
    }
  }, []);

  const loadEvents = useCallback(async (topic: string) => {
    if (!topic) return;
    try {
      const document = await getJson<{ events: readonly KafkaEvent[]; observerStartedAt: string | null }>(
        `./api/kafka/topics/${encodeURIComponent(topic)}/events?limit=30`,
      );
      setEvents(document.events);
      setObserverStartedAt(document.observerStartedAt);
      setError(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Could not load Kafka events.");
    }
  }, []);

  useEffect(() => {
    const initial = window.setTimeout(() => void loadTopics(), 0);
    const timer = window.setInterval(() => void loadTopics(), 5_000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [loadTopics]);

  useEffect(() => {
    if (!selectedTopic) return;
    const initial = window.setTimeout(() => void loadEvents(selectedTopic), 0);
    const timer = window.setInterval(() => void loadEvents(selectedTopic), 2_000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [loadEvents, selectedTopic]);

  const selected = topics.find(({ name }) => name === selectedTopic);
  const totalMessages = useMemo(() => topics.reduce((sum, topic) => sum + topic.messages, 0), [topics]);

  return (
    <section className="kafka-monitor" aria-labelledby="kafka-monitor-title">
      <div className="kafka-monitor__heading">
        <div>
          <span>EVENT TRANSPORT / LIVE TAIL</span>
          <h3 id="kafka-monitor-title">Kafka topic observer</h3>
          <p>Read-only · isolated consumer · pipeline offsets are never committed</p>
        </div>
        <div className="kafka-monitor__pulse">
          <i className={observerStartedAt ? "is-live" : ""} />
          <span>{observerStartedAt ? "OBSERVER CONNECTED" : loading ? "CONNECTING" : "OBSERVER OFFLINE"}</span>
          <strong>{formatCount(totalMessages)} RETAINED</strong>
        </div>
      </div>

      <div className="kafka-monitor__body">
        <aside className="topic-list" aria-label="Kafka topics">
          {topics.map((topic) => (
            <button
              className={topic.name === selectedTopic ? "is-selected" : ""}
              type="button"
              key={topic.name}
              onClick={() => {
                setEvents([]);
                setSelectedTopic(topic.name);
              }}
            >
              <span>{topic.name.replace("marketplace.", "")}</span>
              <small>{topic.partitions} PARTITIONS</small>
              <strong>{formatCount(topic.messages)}</strong>
            </button>
          ))}
          {!topics.length && !loading && <p>No marketplace topics found.</p>}
        </aside>

        <div className="event-feed">
          <div className="event-feed__bar">
            <div><span>SELECTED TOPIC</span><strong>{selected?.name ?? "—"}</strong></div>
            <div><span>REFRESH</span><strong>2 SECONDS</strong></div>
            <button type="button" onClick={() => void loadEvents(selectedTopic)} disabled={!selectedTopic}>Refresh events</button>
          </div>
          {error && <div className="event-feed__error" role="alert">{error}</div>}
          <div className="event-feed__scroll" aria-live="polite">
            {events.map((event) => (
              <article className="event-record" key={event.id}>
                <div className="event-record__meta">
                  <span>{formatTimestamp(event.timestamp)}</span>
                  <span>P{event.partition} · OFFSET {event.offset}</span>
                  <span>{event.sizeBytes} B</span>
                </div>
                <div className="event-record__title">
                  <strong>{eventName(event.value)}</strong>
                  <small>{event.key ? `KEY ${event.key}` : "UNKEYED"}</small>
                </div>
                <pre>{eventBody(event.value)}</pre>
              </article>
            ))}
            {!events.length && !error && (
              <div className="event-feed__empty">
                <span>◉</span>
                <strong>{loading ? "Connecting to Kafka" : "Waiting for records"}</strong>
                <p>Run <code>make demo</code> in another terminal to watch events appear here.</p>
              </div>
            )}
          </div>
        </div>
      </div>
    </section>
  );
}
