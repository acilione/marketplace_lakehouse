import type { Currency, Evidence, Order, Snapshot } from "./model";

export const SNAPSHOT_TIME = "2026-10-09T10:30:00.000Z";
export const NEXT_BATCH_TIME = "2026-10-09T10:35:00.000Z";
const at = (minutesBefore: number) =>
  new Date(Date.parse(SNAPSHOT_TIME) - minutesBefore * 60_000).toISOString();
const scenarios: {
  amount: string;
  market: string;
  seller: string;
  age: number;
  capture?: string;
  captureCurrency?: Currency;
  currency?: Currency;
}[] = [
  { amount: "1248.00", market: "Italy", seller: "Linea Casa", age: 185 },
  {
    amount: "386.00",
    market: "Germany",
    seller: "Nord Studio",
    age: 142,
    capture: "286.00",
  },
  {
    amount: "215.00",
    market: "France",
    seller: "Atelier 09",
    age: 94,
    capture: "215.00",
    captureCurrency: "USD",
  },
  { amount: "89.90", market: "Spain", seller: "Forma Goods", age: 77 },
  {
    amount: "164.00",
    market: "Italy",
    seller: "Linea Casa",
    age: 62,
    capture: "149.00",
  },
  { amount: "72.50", market: "Germany", seller: "Nord Studio", age: 48 },
  {
    amount: "329.00",
    market: "France",
    seller: "Atelier 09",
    age: 39,
    capture: "319.00",
  },
  {
    amount: "149.00",
    market: "United Kingdom",
    seller: "Common Objects",
    age: 34,
    currency: "GBP",
  },
];
export function demoSnapshot(batch: number): Snapshot {
  const orders: Order[] = [];
  const events: Evidence[] = [];
  let offset = 8100;
  for (let index = 0; index < 64; index++) {
    const scenario = scenarios[index] ?? {
      amount: `${55 + index * 7}.00`,
      market: ["Italy", "Germany", "France", "Spain"][index % 4]!,
      seller: ["Linea Casa", "Nord Studio", "Atelier 09", "Forma Goods"][
        index % 4
      ]!,
      age: 35 + ((index * 11) % 260),
    };
    const order: Order = {
      id: `ORD-${10482 - index}`,
      amount: scenario.amount,
      market: scenario.market,
      seller: scenario.seller,
      currency: scenario.currency ?? "EUR",
      confirmedAt: at(scenario.age),
    };
    orders.push(order);
    events.push({
      id: `evt-order-${index}`,
      orderId: order.id,
      kind: "order.confirmed",
      occurredAt: order.confirmedAt,
      receivedAt: order.confirmedAt,
      amount: order.amount,
      currency: order.currency,
      topic: "orders.v1",
      offset: offset++,
    });
    events.push({
      id: `evt-auth-${index}`,
      orderId: order.id,
      paymentId: `PAY-${7400 + index}`,
      kind: "payment.authorized",
      occurredAt: at(scenario.age - 1),
      receivedAt: at(scenario.age - 1),
      amount: order.amount,
      currency: order.currency,
      topic: "payments.v1",
      offset: offset++,
    });
    if (scenario.capture !== undefined || index >= scenarios.length)
      events.push({
        id: `evt-capture-${index}`,
        orderId: order.id,
        paymentId: `PAY-${7400 + index}`,
        kind: "payment.captured",
        occurredAt: at(scenario.age - 3),
        receivedAt: at(scenario.age - 3),
        amount: scenario.capture ?? order.amount,
        currency: scenario.captureCurrency ?? order.currency,
        topic: "payments.v1",
        offset: offset++,
      });
  }
  if (batch > 0)
    events.push({
      id: "evt-late-capture-0",
      orderId: orders[0]!.id,
      paymentId: "PAY-7400",
      kind: "payment.captured",
      occurredAt: at(180),
      receivedAt: NEXT_BATCH_TIME,
      amount: orders[0]!.amount,
      currency: "EUR",
      topic: "payments.v1",
      offset,
    });
  return { orders, events, asOf: batch > 0 ? NEXT_BATCH_TIME : SNAPSHOT_TIME };
}
