import { useState } from "react";
import { createRoot } from "react-dom/client";
import { useLiveQuery } from "@chlive/client/react";

let setWriteError = () => { };

// Produce one JSON row to a Kafka topic through Redpanda's HTTP proxy.
async function write(topic, row) {
  const res = await fetch(`/kafka/topics/${topic}`, {
    method: "POST",
    headers: { "Content-Type": "application/vnd.kafka.json.v2+json" },
    body: JSON.stringify({ records: [{ value: row }] }),
  });
  setWriteError(res.ok ? null : `write to ${topic} failed: ${await res.text()}`);
}

function WriteError() {
  const [error, setError] = useState(null);
  setWriteError = setError;
  return error && <p style={{ color: "red" }}>{error}</p>;
}

const now = () => new Date().toISOString().replace("T", " ").replace("Z", "");
const pick = (xs) => xs[Math.floor(Math.random() * xs.length)];
const id = (prefix) => prefix + Math.random().toString(36).slice(2, 7);

const EVENTS_SQL = `SELECT ts, user_id, country, path, latency_ms FROM events ORDER BY ts DESC LIMIT 10`;
const ORDERS_SQL = `SELECT ts, order_id, customer_id, amount FROM orders ORDER BY ts DESC LIMIT 10`;
// Customers are append-only too: a rename is a new row, the latest one wins.
// (Aliases may not shadow source columns, hence customer_name / customer_country.)
const CUSTOMERS_SQL = `SELECT customer_id, argMax(name, ts) AS customer_name, argMax(country, ts) AS customer_country
FROM customers GROUP BY customer_id ORDER BY customer_id LIMIT 100`;

function Rows({ rows, columns }) {
  return (
    <table>
      <thead>
        <tr>{columns.map((c) => <th key={c}>{c}</th>)}</tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.__key}>{columns.map((c) => <td key={c}>{String(r[c] ?? "")}</td>)}</tr>
        ))}
      </tbody>
    </table>
  );
}

function Events() {
  const { rows, status, error } = useLiveQuery(EVENTS_SQL);
  const add = () =>
    write("events", {
      ts: now(),
      user_id: id("u"),
      country: pick(["PL", "DE", "US"]),
      path: pick(["/", "/cart", "/checkout"]),
      latency_ms: Math.floor(Math.random() * 500),
      amount: 0,
    });
  return (
    <section>
      <h2>Single table: events</h2>
      <code>{EVENTS_SQL}</code>
      <p>
        <button onClick={add}>Write event to Kafka</button> {status} {error}
      </p>
      <Rows rows={rows} columns={["ts", "user_id", "country", "path", "latency_ms"]} />
    </section>
  );
}

// Joins are not supported server-side, so the two live queries are joined here.
function OrdersWithCustomers() {
  const orders = useLiveQuery(ORDERS_SQL);
  const customers = useLiveQuery(CUSTOMERS_SQL);
  const byId = new Map(customers.rows.map((c) => [c.customer_id, c]));
  const rows = orders.rows.map((o) => ({ ...o, ...byId.get(o.customer_id) }));

  const addCustomer = () =>
    write("customers", { ts: now(), customer_id: id("c"), name: pick(["Ada", "Linus", "Grace", "Alan", "Barbara"]), country: pick(["PL", "DE", "US"]) });
  const noCustomers = customers.rows.length === 0;
  const addOrder = () => {
    write("orders", { ts: now(), order_id: id("o"), customer_id: pick(customers.rows).customer_id, amount: Math.round(Math.random() * 20000) / 100 });
  };
  const renameCustomer = () => {
    const c = pick(customers.rows);
    write("customers", { ts: now(), customer_id: c.customer_id, name: c.customer_name + "!", country: c.customer_country });
  };

  return (
    <section>
      <h2>Related tables: orders ⋈ customers</h2>
      <code>{ORDERS_SQL}</code>
      <code>{CUSTOMERS_SQL}</code>
      <p>
        <button onClick={addCustomer}>Write customer</button> <button onClick={addOrder} disabled={noCustomers}>Write order</button>{" "}
        <button onClick={renameCustomer} disabled={noCustomers}>Rename a customer</button> {orders.status}/{customers.status} {orders.error} {customers.error}
      </p>
      <Rows rows={rows} columns={["ts", "order_id", "customer_id", "customer_name", "customer_country", "amount"]} />
      <Rows rows={customers.rows} columns={["customer_id", "customer_name", "customer_country"]} />
    </section>
  );
}

createRoot(document.getElementById("root")).render(
  <>
    <h1>chlive simple</h1>
    <WriteError />
    <Events />
    <OrdersWithCustomers />
  </>,
);
