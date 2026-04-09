# ListenLab

ListenLab is a Spotify web app that analyzes your real listening behavior to surface overlooked artists, music, and actionable insights.

The core idea is simple: use what you actually listen to, not generic recommendations, to help you rediscover what you already care about.

---

## MVP

The initial version focuses on one primary feature:
- surfacing artists you clearly engage with but have not followed

Planned MVP components:
- React frontend
- FastAPI backend
- Spotify OAuth
- engagement-based scoring
- explanation-first results
- optional playlist generation

---

## Status

This repository now includes the first implemented milestone:
- minimal React frontend shell
- FastAPI backend
- Spotify OAuth login flow
- session-based auth
- authenticated Spotify profile test endpoint

The broader analysis, scoring, ranking, and playlist features are still in planning.

---

## Project Direction

ListenLab is built around **"signal over suggestion"**:
- prioritize real listening behavior over inferred taste
- combine multiple engagement signals such as listening, likes, and saves
- explain why results are surfaced
- avoid black-box recommendations

---

## Docs

- [Architecture](docs/architecture.md)
- [Context](docs/context.md)
- [Roadmap](docs/roadmap.md)
- [Auth Milestone Notes](docs/auth-milestone.md)

---

## MVP Goal

Build a web app that:
- connects to a user's Spotify account
- aggregates engagement signals by artist
- ranks artists by actual engagement
- filters out artists the user already follows
- explains why each result was surfaced
- optionally creates a playlist from those artists

---

## First Implementation Defaults

- Spotify is the source of truth
- analysis runs on demand
- no database in MVP
- local development first, simple cloud deployment later
