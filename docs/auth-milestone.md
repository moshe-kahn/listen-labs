# Auth Milestone Run Notes

## What this milestone includes
- React frontend shell with Spotify login button
- frontend callback handling
- FastAPI backend with Spotify OAuth endpoints
- backend token exchange and session storage
- authenticated `GET /me` test endpoint

## Status
- Implemented
- Manually verified locally against Spotify OAuth
- Uses `127.0.0.1` consistently for backend, frontend, and Spotify redirect configuration

## Backend setup
1. Copy `backend/.env.example` to `backend/.env`.
2. Fill in Spotify app credentials.
3. Install dependencies:

```bash
py -m pip install -r backend/requirements.txt
```

4. Run the API:

```bash
py -m uvicorn app.main:app --reload --app-dir backend
```

## Frontend setup
1. Copy `frontend/.env.example` to `frontend/.env`.
2. Install dependencies:

```bash
npm install --prefix frontend
```

3. Run the app:

```bash
npm run dev --prefix frontend
```

## Spotify app settings
Set the Spotify redirect URI to:

```text
http://127.0.0.1:8000/auth/callback
```

## Manual verification flow
1. Open the frontend at `http://127.0.0.1:5173`.
2. Click `Log in with Spotify`.
3. Complete Spotify authorization.
4. Confirm you return to the frontend and see an authenticated session.
5. Click `Test authenticated endpoint`.
6. Confirm profile data from Spotify appears in the UI.

## Known next step
- Replace the raw debug-style post-login display with a cleaner connected-account summary UI.
