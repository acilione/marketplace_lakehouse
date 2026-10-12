import { useEffect, useState, type ReactNode } from "react";
import { OperationsDashboard } from "./OperationsDashboard";

export function DashboardLogin({
  children,
}: {
  children?: (role: string) => ReactNode;
}) {
  const [role, setRole] = useState<string | null>(null);
  const [user, setUser] = useState("operator");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    const controller = new AbortController();
    const expired = () => setRole(null);
    window.addEventListener("lakehouse-session-expired", expired);
    void fetch("./api/session", { signal: controller.signal })
      .then(async (response) => {
        if (response.ok && !controller.signal.aborted)
          setRole(((await response.json()) as { role: string }).role);
      })
      .catch(() => undefined);
    return () => {
      controller.abort();
      window.removeEventListener("lakehouse-session-expired", expired);
    };
  }, []);
  async function signIn() {
    setBusy(true);
    setError("");
    try {
      const response = await fetch("./api/login", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Lakehouse-Request": "dashboard",
        },
        body: JSON.stringify({ user, password }),
      });
      if (!response.headers.get("content-type")?.includes("application/json")) {
        throw new Error(
          "Live tools are unavailable. Run make dashboard-up and open the website at http://localhost:4173.",
        );
      }
      const result = (await response.json()) as {
        role: string;
        error?: string;
      };
      if (!response.ok) throw new Error(result.error ?? "Sign in failed.");
      setRole(result.role);
      setPassword("");
    } catch (caught) {
      setError(
        caught instanceof Error ? caught.message : "Dashboard unavailable.",
      );
    } finally {
      setBusy(false);
    }
  }
  async function signOut() {
    setBusy(true);
    setError("");
    try {
      const response = await fetch("./api/logout", {
        method: "POST",
        headers: { "X-Lakehouse-Request": "dashboard" },
        signal: AbortSignal.timeout(10_000),
      });
      if (!response.ok && response.status !== 401)
        throw new Error("Sign out failed. Please retry.");
      setRole(null);
    } catch (caught) {
      setError(
        caught instanceof Error ? caught.message : "Sign out unavailable.",
      );
    } finally {
      setBusy(false);
    }
  }
  if (role)
    return (
      <>
        <div className="ops-toolbar">
          <span>Signed in as {role}</span>
          <button type="button" disabled={busy} onClick={() => void signOut()}>
            Sign out
          </button>
          {error && <span role="alert">{error}</span>}
        </div>
        {children ? (
          children(role)
        ) : (
          <OperationsDashboard canControl={role === "operator"} />
        )}
      </>
    );
  return (
    <section className="dashboard-login" aria-label="Dashboard sign in">
      <h3>Sign in to your lakehouse</h3>
      <p>
        Viewers can explore data. Operators can also start and stop optional
        services.
      </p>
      {children && (
        <p>
          Start with <code>make showcase</code>, then open{" "}
          <code>http://localhost:24173</code>. Use Preview for the standalone
          dashboard.
        </p>
      )}
      <label>
        Username
        <select value={user} onChange={(event) => setUser(event.target.value)}>
          <option>operator</option>
          <option>viewer</option>
        </select>
      </label>
      <label>
        Password
        <input
          type="password"
          autoComplete="current-password"
          value={password}
          onChange={(event) => setPassword(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter" && !busy) void signIn();
          }}
        />
      </label>
      <button
        type="button"
        disabled={busy || !password}
        onClick={() => void signIn()}
      >
        {busy ? "Signing in…" : "Sign in"}
      </button>
      {error && <p role="alert">{error}</p>}
    </section>
  );
}
