import { useEffect, useState } from "react";
import { OperationsDashboard } from "./OperationsDashboard";

export function DashboardLogin() {
  const [role, setRole] = useState<string | null>(null);
  const [user, setUser] = useState("operator");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    const expired = () => setRole(null);
    window.addEventListener("lakehouse-session-expired", expired);
    void fetch("./api/session").then(async (response) => {
      if (response.ok) setRole((await response.json() as { role: string }).role);
    }).catch(() => undefined);
    return () => window.removeEventListener("lakehouse-session-expired", expired);
  }, []);
  async function signIn() {
    setBusy(true); setError("");
    try {
      const response = await fetch("./api/login", {
        method: "POST", headers: { "Content-Type": "application/json", "X-Lakehouse-Request": "dashboard" },
        body: JSON.stringify({ user, password }),
      });
      const result = await response.json() as { role: string; error?: string };
      if (!response.ok) throw new Error(result.error ?? "Sign in failed.");
      setRole(result.role); setPassword("");
    } catch (caught) { setError(caught instanceof Error ? caught.message : "Dashboard unavailable."); }
    finally { setBusy(false); }
  }
  if (role) return <>
    <div className="ops-toolbar"><span>Signed in as {role}</span><button type="button" onClick={() => {
      void fetch("./api/logout", { method: "POST", headers: { "X-Lakehouse-Request": "dashboard" } }).then(() => setRole(null));
    }}>Sign out</button></div>
    <OperationsDashboard canControl={role === "operator"} />
  </>;
  return <section className="dashboard-login" aria-label="Dashboard sign in">
    <h3>Sign in to your lakehouse</h3>
    <p>Viewers can explore data. Operators can also start and stop optional services.</p>
    <label>Username<select value={user} onChange={(event) => setUser(event.target.value)}><option>operator</option><option>viewer</option></select></label>
    <label>Password<input type="password" autoComplete="current-password" value={password} onChange={(event) => setPassword(event.target.value)} onKeyDown={(event) => { if (event.key === "Enter" && !busy) void signIn(); }} /></label>
    <button type="button" disabled={busy || !password} onClick={() => void signIn()}>{busy ? "Signing in…" : "Sign in"}</button>
    {error && <p role="alert">{error}</p>}
  </section>;
}
