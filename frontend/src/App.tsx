import { useEffect, useState } from "react";

type SessionResponse = {
  authenticated: boolean;
  display_name: string | null;
  spotify_user_id: string | null;
  email?: string | null;
};

type ProfileResponse = {
  id: string;
  display_name: string | null;
  email: string | null;
  product: string | null;
  country: string | null;
};

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

export function App() {
  const [session, setSession] = useState<SessionResponse | null>(null);
  const [profile, setProfile] = useState<ProfileResponse | null>(null);
  const [statusMessage, setStatusMessage] = useState("Checking authentication state...");
  const [loadingProfile, setLoadingProfile] = useState(false);

  useEffect(() => {
    const url = new URL(window.location.href);
    if (url.pathname === "/auth/callback") {
      const status = url.searchParams.get("status");
      if (status === "success") {
        setStatusMessage("Spotify login succeeded. Session restored.");
      } else {
        setStatusMessage("Spotify login did not complete successfully.");
      }

      window.history.replaceState({}, "", "/");
    }
  }, []);

  useEffect(() => {
    void loadSession();
  }, []);

  async function loadSession() {
    try {
      const response = await fetch(`${apiBaseUrl}/auth/session`, {
        credentials: "include",
      });

      if (!response.ok) {
        throw new Error("Failed to load auth session.");
      }

      const data = (await response.json()) as SessionResponse;
      setSession(data);

      if (data.authenticated) {
        setStatusMessage(`Connected to Spotify as ${data.display_name ?? "an authenticated user"}.`);
      } else {
        setStatusMessage("Not connected yet. Use Spotify login to start the auth flow.");
      }
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : "Failed to load session.");
    }
  }

  function startLogin() {
    window.location.href = `${apiBaseUrl}/auth/login`;
  }

  async function loadProfile() {
    setLoadingProfile(true);
    try {
      const response = await fetch(`${apiBaseUrl}/me`, {
        credentials: "include",
      });

      if (!response.ok) {
        throw new Error("Authenticated test request failed.");
      }

      const data = (await response.json()) as ProfileResponse;
      setProfile(data);
      setStatusMessage("Authenticated test endpoint returned Spotify profile data.");
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : "Failed to load Spotify profile.");
    } finally {
      setLoadingProfile(false);
    }
  }

  async function logout() {
    await fetch(`${apiBaseUrl}/auth/logout`, {
      method: "POST",
      credentials: "include",
    });
    setProfile(null);
    await loadSession();
  }

  return (
    <main className="app-shell">
      <section className="hero-card">
        <p className="eyebrow">ListenLab</p>
        <h1>Minimal Spotify auth milestone</h1>
        <p className="lede">
          This shell proves the frontend-to-backend-to-Spotify login flow before any analysis
          features are added.
        </p>

        <div className="actions">
          <button className="primary-button" onClick={startLogin} type="button">
            Log in with Spotify
          </button>
          <button
            className="secondary-button"
            disabled={!session?.authenticated || loadingProfile}
            onClick={() => void loadProfile()}
            type="button"
          >
            {loadingProfile ? "Checking..." : "Test authenticated endpoint"}
          </button>
          <button
            className="secondary-button"
            disabled={!session?.authenticated}
            onClick={() => void logout()}
            type="button"
          >
            Log out
          </button>
        </div>

        <div className="status-panel">
          <h2>Status</h2>
          <p>{statusMessage}</p>
        </div>

        <div className="info-grid">
          <div className="info-card">
            <h2>Session</h2>
            <pre>{JSON.stringify(session, null, 2)}</pre>
          </div>
          <div className="info-card">
            <h2>Profile Test</h2>
            <pre>{JSON.stringify(profile, null, 2)}</pre>
          </div>
        </div>
      </section>
    </main>
  );
}
