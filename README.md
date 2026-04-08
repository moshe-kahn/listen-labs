# ListenLab

ListenLab is a Spotify web app concept focused on one idea: use real listening behavior to surface artists a user already cares about but has not fully acted on yet.

The MVP is planned as:
- React frontend
- FastAPI backend
- Spotify OAuth
- artist-level engagement scoring
- explanation-first overlooked artist results
- optional playlist generation

## Status
This repository is currently in the planning and documentation stage. There is no application code yet.

## Project Direction
ListenLab is built around "signal over suggestion":
- use actual listening behavior
- prioritize evidence over inference
- explain why each artist is surfaced

## Docs
- [Architecture](docs/architecture.md)
- [Context](docs/context.md)
- [Roadmap](docs/roadmap.md)

## MVP Goal
Build a web app that:
- connects to a user's Spotify account
- aggregates engagement signals by artist
- ranks artists by actual engagement
- filters out artists the user already follows
- explains why each result was surfaced
- optionally creates a playlist from those artists

## First Implementation Defaults
- Spotify is the source of truth
- analysis runs on demand
- no database in MVP
- local development first, simple cloud deployment later
