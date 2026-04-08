# ListenLab Context

## Purpose
This document explains the product intent, domain language, and scope boundaries for ListenLab. It is meant to give implementers enough shared context to make consistent decisions while building the MVP.

## Product Thesis
ListenLab turns real listening behavior into actionable insight.

The product is based on one belief: users already show what they care about through their listening patterns, even when they do not explicitly like tracks, save albums, or follow artists. ListenLab should surface those existing signals clearly instead of trying to predict taste through generic recommendations.

## Core Product Principle
Signal over suggestion.

The app should prioritize observed behavior over inferred similarity. Every surfaced result should be traceable to concrete evidence from the user's Spotify activity and library state.

## User Problem
Spotify users often:
- listen heavily to certain artists
- forget to follow artists they repeatedly return to
- engage with tracks without organizing their library
- lose track of albums or artists they already value

This creates a gap between actual listening behavior and explicit user actions. ListenLab closes that gap by showing what the user is already demonstrating through use.

## Target User
The MVP is optimized for an individual Spotify user who:
- listens regularly
- has enough history or library activity to generate signals
- wants help noticing artists they already care about
- values explanations more than black-box recommendations

## Desired User Outcome
After analysis, the user should feel:
- "These results are obviously based on my real behavior."
- "I can see why each artist was surfaced."
- "I want to follow these artists or save this playlist."

## In-Scope MVP Outcome
The MVP should help the user:
- authenticate with Spotify
- analyze their own listening-related signals
- see a ranked list of overlooked artists
- understand each result through explanation text and signal breakdown
- optionally create a playlist from selected artists

## Out-of-Scope for MVP
The MVP should not include:
- AI recommendation features
- similarity, genre, or collaborative filtering logic
- social sharing or multi-user features
- complex personalization settings
- album completion detection
- listening-insights dashboards
- concert integrations

Those items belong to later phases only.

## Domain Vocabulary
### Overlooked artist
An artist the user clearly engages with but does not currently follow. This is the primary surfaced entity for the MVP.

### Engagement signal
A measurable piece of user behavior or library state that contributes to artist ranking. Examples include listening minutes, play count, liked track count, saved album count, and recency.

### `ArtistProfile`
The internal artist-level aggregate that combines all gathered signals into one normalized record. This is the canonical unit for scoring.

### `EngagementSignalBreakdown`
A structured record of how the final score was computed, including raw values, weights, weighted contributions, and signal availability status.

### Explanation payload
The user-facing explanation content that tells the user why an artist appeared in the ranked list. It must be evidence-based and easy to read.

### Playlist generation input
The selected set of surfaced artist IDs plus playlist settings such as name and tracks per artist. This input is used to create a Spotify playlist from the analysis results.

## Product Rules
- Do not surface artists using genre or similarity assumptions.
- Do not hide why an artist was ranked.
- Do not rely on liked tracks alone.
- Treat listening behavior as a first-class signal whenever available.
- Provide a useful fallback when listening-time data is incomplete.

## Why Explanations Matter
Explanation is not decorative UI. It is part of the product value.

Users need to trust that ListenLab is reflecting their own behavior rather than inventing recommendations. The explanation layer makes results legible and credible, especially when scores are built from mixed signal quality.

Bad result:
- "Recommended artist"

Good result:
- "You played 18 tracks and spent 120 minutes listening to this artist, but you do not follow them."

## Primary User Flow
1. User logs in with Spotify.
2. User runs analysis.
3. Backend gathers available Spotify signals and computes artist rankings.
4. Frontend shows overlooked artists with explanations and score breakdowns.
5. User optionally selects artists and creates a playlist.

## Data Availability Constraints
Spotify may not expose every signal needed for perfect behavioral measurement.

Implementation should therefore assume:
- some users will have richer listening history than others
- listening minutes may be partial or approximated
- play counts may need proxy or recent-history logic
- results still need to feel correct even under fallback conditions

## UX Priorities
- Make the main call to action obvious.
- Show ranked results quickly once analysis completes.
- Put explanation text near the artist name and score.
- Keep the interface simple enough that the insight feels immediate.
- Prefer clarity over dashboard complexity.

## Success Criteria
The MVP succeeds when:
- users can authenticate without confusion
- surfaced artists feel obviously correct
- the ranking still works when some signals are missing
- explanations make sense at a glance
- playlist creation feels like a natural follow-up action

## Defaults for Implementers
- Optimize for one user analyzing their own account.
- Assume no persistent database in MVP.
- Keep scoring weights configurable in code, not in the public UI.
- Keep the backend as the owner of ranking, filtering, and explanation generation.
- Keep Spotify as the source of truth rather than copying user data into app storage.
