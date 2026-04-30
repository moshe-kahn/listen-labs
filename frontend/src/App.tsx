import { Fragment, type ReactNode, useEffect, useRef, useState } from "react";

type SessionResponse = {
  authenticated: boolean;
  display_name: string | null;
  spotify_user_id: string | null;
  email?: string | null;
};

type ProfileProgressResponse = {
  active: boolean;
  phase: string | null;
  elapsed_seconds: number;
  events?: Array<{
    phase: string;
    at_seconds: number;
  }>;
};

type RecentTrack = {
  event_id?: number | null;
  track_id: string | null;
  track_name: string | null;
  artist_name: string | null;
  album_name: string | null;
  album_release_year?: string | null;
  artists?: Array<{
    artist_id?: string | null;
    id?: string | null;
    name?: string | null;
    uri?: string | null;
    url?: string | null;
  }> | null;
  duration_ms?: number | null;
  duration_seconds?: number | null;
  uri?: string | null;
  preview_url?: string | null;
  url?: string | null;
  image_url?: string | null;
  album_id?: string | null;
  album_url?: string | null;
  spotify_played_at?: string | null;
  spotify_played_at_unix_ms?: number | null;
  spotify_context_type?: string | null;
  spotify_context_uri?: string | null;
  spotify_context_url?: string | null;
  spotify_context_href?: string | null;
  spotify_is_local?: boolean | null;
  spotify_track_type?: string | null;
  spotify_track_number?: number | null;
  spotify_disc_number?: number | null;
  spotify_explicit?: boolean | null;
  spotify_popularity?: number | null;
  spotify_album_type?: string | null;
  spotify_album_total_tracks?: number | null;
  spotify_available_markets_count?: number | null;
  played_at_gap_ms?: number | null;
  estimated_played_ms?: number | null;
  estimated_played_seconds?: number | null;
  estimated_completion_ratio?: number | null;
  play_count?: number | null;
  all_time_play_count?: number | null;
  recent_play_count?: number | null;
  first_played_at?: string | null;
  last_played_at?: string | null;
  listening_span_days?: number | null;
  listening_span_years?: number | null;
  active_months_count?: number | null;
  span_months_count?: number | null;
  consistency_ratio?: number | null;
  longevity_score?: number | null;
  has_recent_source?: boolean | null;
  has_history_source?: boolean | null;
  source_label?: "recent" | "history" | "both" | "api" | null;
  recent_source_event_count?: number | null;
  history_source_event_count?: number | null;
  matched_source_event_count?: number | null;
  timing_source?: string | null;
  matched_state?: string | null;
  raw_spotify_recent_id?: number | null;
  raw_spotify_history_id?: number | null;
  spotify_skipped?: boolean | null;
  spotify_shuffle?: boolean | null;
  spotify_offline?: boolean | null;
  formula_rank_delta?: number | null;
};

type MatchCounts = {
  short_term_top: number;
  long_term_top: number;
  recently_played: number;
  liked: number;
  playlist_size: number;
};

type TopPlaylist = {
  playlist_id: string | null;
  playlist_name: string | null;
  playlist_url: string | null;
  image_url?: string | null;
  track_count: number | null;
  score: number;
  match_counts: MatchCounts;
};

type OwnedPlaylist = {
  playlist_id: string | null;
  name: string | null;
  track_count: number | null;
  description?: string | null;
  is_public?: boolean | null;
  url: string | null;
  image_url?: string | null;
};

type FollowedArtist = {
  artist_id: string | null;
  name: string | null;
  followers_total: number | null;
  genres: string[];
  popularity?: number | null;
  url: string | null;
  image_url?: string | null;
  debug?: {
    source?: string;
    score?: number;
    total_ms?: number;
    play_count?: number;
    distinct_tracks?: number;
  };
};

type TopAlbum = {
  album_id: string | null;
  name: string | null;
  artist_name: string | null;
  release_year?: string | null;
  url: string | null;
  image_url?: string | null;
  track_representation_count: number;
  rank_score: number;
  album_score: number;
  represented_track_names: string[];
  debug?: {
    source?: string;
    score?: number;
    total_ms?: number;
    play_count?: number;
    distinct_tracks?: number;
  };
};

type ProfileResponse = {
  id: string;
  display_name: string | null;
  email: string | null;
  product: string | null;
  country: string | null;
  username: string | null;
  followers_total: number | null;
  followed_artists_total: number | null;
  followed_artists_available: boolean;
  followed_artists: FollowedArtist[];
  followed_artists_list_available: boolean;
  recent_top_artists: FollowedArtist[];
  recent_top_artists_available: boolean;
  top_tracks: RecentTrack[];
  top_tracks_available: boolean;
  recent_top_tracks: RecentTrack[];
  recent_top_tracks_available: boolean;
  top_albums: TopAlbum[];
  top_albums_available: boolean;
  recent_top_albums: TopAlbum[];
  recent_top_albums_available: boolean;
  analysis_mode?: "quick" | "full";
  experience_mode?: "full" | "local";
  recent_range?: "short_term" | "medium_term";
  recent_window_days?: number;
  top_playlists_recent: TopPlaylist[];
  top_playlists_all_time: TopPlaylist[];
  top_playlists_available: boolean;
  history_insights_available?: boolean;
  history_first_played_at?: string | null;
  history_last_played_at?: string | null;
  history_total_listen_ms?: number | null;
  history_total_play_count?: number | null;
  profile_url: string | null;
  image_url: string | null;
  recent_tracks: RecentTrack[];
  recent_tracks_available: boolean;
  owned_playlists: OwnedPlaylist[];
  owned_playlists_available: boolean;
  recent_likes_tracks: RecentTrack[];
  recent_likes_available: boolean;
  extended_loaded?: boolean;
  stale_sections?: string[];
  local_last_synced_at?: number | null;
};

type RecentSectionResponse = {
  recent_range: "short_term" | "medium_term";
  recent_window_days: number;
  recent_top_artists: FollowedArtist[];
  recent_top_artists_available: boolean;
  recent_top_tracks: RecentTrack[];
  recent_top_tracks_available: boolean;
  recent_top_albums: TopAlbum[];
  recent_top_albums_available: boolean;
  recent_tracks: RecentTrack[];
  recent_tracks_available: boolean;
  recent_likes_tracks: RecentTrack[];
  recent_likes_available: boolean;
};

type RecentArchiveResponse = {
  items: RecentTrack[];
  has_more: boolean;
  limit: number;
  offset: number;
};

type ListeningLogResponse = {
  items: RecentTrack[];
  has_more: boolean;
  limit: number;
  offset: number;
  source_filter: "all" | "api" | "history" | "both";
};

type MergedTrackSourceFilter = "all" | "recent" | "history" | "both";
type RecentDebugSourceFilter = "all" | "api" | "history" | "both";

type MergedTrackAggregateResponse = {
  limit: number;
  recent_window_days: number;
  source_filter: MergedTrackSourceFilter;
  returned_items: number;
  excluded_unknown_identity_count: number;
  items: RecentTrack[];
};

type TrackIdentityAuditExample = Record<string, unknown>;

type TrackIdentityAuditResponse = {
  limit: number;
  same_name_canonical_splits: TrackIdentityAuditExample[];
  release_track_source_splits: TrackIdentityAuditExample[];
  analysis_track_groups: TrackIdentityAuditExample[];
};

type IdentityAuditTab = "overview" | "canonical" | "composition" | "family" | "release";

type AmbiguousReviewComponent = {
  label: string;
  family: string;
  semantic_category: string;
  groupable_by_default: boolean;
};

type AmbiguousReviewItem = {
  entry_id: string;
  bucket: "grouped" | "ungrouped";
  release_track_id: number;
  release_track_name: string;
  artist_name: string;
  analysis_name: string | null;
  song_family_key: string | null;
  confidence: number | null;
  review_families: string[];
  dominant_family: string | null;
  base_title_anchor: string | null;
  components: AmbiguousReviewComponent[];
  raw_component_summary: string;
};

type AmbiguousReviewResponse = {
  source: {
    kind: string;
    path: string;
    generated_at: string | null;
  };
  summary: {
    grouped_review_entries: number;
    ungrouped_review_entries: number;
    total_review_entries: number;
  };
  family_counts: Array<{
    family: string;
    count: number;
  }>;
  pagination: {
    limit: number;
    offset: number;
    returned: number;
    has_more: boolean;
  };
  filters: {
    family: string | null;
    bucket: string | null;
  };
  items: AmbiguousReviewItem[];
  parse_warning: string;
};

type SuggestedGroupReleaseTrack = {
  release_track_id: number;
  release_track_name: string;
  normalized_name: string;
  primary_artists: string;
  album_names: string;
  source_refs: string;
  source_map_methods: string;
};

type SuggestedAnalysisGroup = {
  analysis_track_id: number;
  analysis_track_name: string;
  grouping_note: string;
  grouping_hash: string | null;
  song_family_key: string | null;
  match_method: string;
  confidence: number;
  status: string;
  release_track_count: number;
  release_tracks: SuggestedGroupReleaseTrack[];
};

type SuggestedGroupsResponse = {
  summary: {
    total_groups: number;
    status: string;
  };
  pagination: {
    limit: number;
    offset: number;
    returned: number;
    has_more: boolean;
  };
  items: SuggestedAnalysisGroup[];
};

type LocalReviewVerdict = "good_to_group" | "not_good" | "skipped" | "unsure";
type LocalGroupingTarget = "same_composition" | "same_release_track_only" | null;
type LocalReviewDecision = {
  verdict: LocalReviewVerdict;
  grouping_target: LocalGroupingTarget;
  note: string;
  updated_at_ms: number;
};

type SubmissionPreviewValidationResponse = {
  ok: boolean;
  summary: {
    total_decisions: number;
    group_decisions: number;
    track_decisions: number;
    approved: number;
    rejected: number;
    skipped: number;
    unknown_groups: number;
    unknown_tracks: number;
    warnings: number;
  };
  warnings: string[];
  unknown_items: {
    groups: Array<Record<string, unknown>>;
    tracks: Array<Record<string, unknown>>;
  };
  validated: {
    groups: {
      approved: Array<Record<string, unknown>>;
      rejected: Array<Record<string, unknown>>;
      skipped: Array<Record<string, unknown>>;
    };
    tracks: {
      approved: Array<Record<string, unknown>>;
      rejected: Array<Record<string, unknown>>;
      skipped: Array<Record<string, unknown>>;
    };
  };
};

type IdentityAuditSubmissionSaveResponse = {
  ok: boolean;
  submission_id: number;
  status: string;
  created_at: string;
  summary: {
    total_decisions: number;
    group_decisions: number;
    track_decisions: number;
    approved: number;
    rejected: number;
    skipped: number;
    unknown_groups: number;
    unknown_tracks: number;
    warnings: number;
  };
  warnings: string[];
  unknown_items: {
    groups: Array<Record<string, unknown>>;
    tracks: Array<Record<string, unknown>>;
  };
};

type IdentityAuditSavedSubmissionListItem = {
  id: number;
  created_at: string;
  status: string;
  summary: {
    total_decisions?: number;
    group_decisions?: number;
    track_decisions?: number;
    approved?: number;
    rejected?: number;
    skipped?: number;
    unknown_groups?: number;
    unknown_tracks?: number;
    warnings?: number;
  };
  warnings_count: number;
  unknown_groups: number;
  unknown_tracks: number;
  notes: string | null;
};

type IdentityAuditSavedSubmissionListResponse = {
  ok: boolean;
  items: IdentityAuditSavedSubmissionListItem[];
  total: number;
};

type IdentityAuditSavedSubmissionReadResponse = {
  ok: boolean;
  item: {
    id: number;
    created_at: string;
    status: string;
    payload: Record<string, unknown>;
    validation: SubmissionPreviewValidationResponse;
    notes: string | null;
    promoted_at: string | null;
  };
};

type IdentityAuditSubmissionDryRunResponse = {
  ok: boolean;
  submission_id: number;
  status: "dry_run";
  validation: SubmissionPreviewValidationResponse;
  summary: {
    approved_groups: number;
    approved_tracks: number;
    rejected: number;
    skipped: number;
    would_apply: number;
    warnings: number;
    unknown_groups: number;
    unknown_tracks: number;
  };
  plan: {
    groups: Array<Record<string, unknown>>;
    tracks: Array<Record<string, unknown>>;
  };
  noops: {
    rejected: Array<Record<string, unknown>>;
    skipped: Array<Record<string, unknown>>;
  };
  warnings: string[];
};

type CatalogBackfillRunItem = {
  id: number;
  started_at: string | null;
  completed_at: string | null;
  market: string | null;
  status: string | null;
  tracks_seen: number;
  tracks_fetched: number;
  tracks_upserted: number;
  albums_seen: number;
  albums_fetched: number;
  album_tracks_upserted: number;
  album_tracklists_seen?: number;
  album_tracklists_skipped_by_policy?: number;
  album_tracklists_fetched?: number;
  skipped: number;
  errors: number;
  requests_total: number;
  requests_success: number;
  requests_429: number;
  requests_failed: number;
  initial_request_delay_seconds: number;
  final_request_delay_seconds: number;
  effective_requests_per_minute: number;
  peak_requests_last_30_seconds: number;
  max_retry_after_seconds: number;
  last_retry_after_seconds?: number;
  has_more: boolean;
  last_error: string | null;
  warnings?: string[];
  warnings_count?: number;
  partial?: boolean | null;
  stop_reason?: string | null;
  album_tracklist_policy?: "all" | "priority_only" | "relevant_albums" | "none" | string;
};

type CatalogBackfillRunsResponse = {
  ok: boolean;
  items: CatalogBackfillRunItem[];
  total: number;
};

type CatalogBackfillQueueItem = {
  id: number;
  entity_type: "track" | "album" | string;
  spotify_id: string;
  reason: string | null;
  priority: number;
  status: "pending" | "done" | "error" | string;
  requested_at: string | null;
  last_attempted_at: string | null;
  attempts: number;
  last_error: string | null;
};

type CatalogBackfillQueueResponse = {
  ok: boolean;
  items: CatalogBackfillQueueItem[];
  total: number;
  counts: {
    pending: number;
    done: number;
    error: number;
  };
};

type CatalogBackfillQueueRepairResponse = {
  ok: boolean;
  repaired: number;
};

type AlbumCatalogLookupItem = {
  release_album_id: number;
  release_album_name: string;
  artist_name: string;
  spotify_album_id: string | null;
  spotify_album_name: string | null;
  album_type: string | null;
  release_date: string | null;
  total_tracks: number | null;
  album_track_rows: number;
  tracklist_complete: boolean;
  catalog_fetched_at: string | null;
  catalog_last_status: string | null;
  catalog_last_error: string | null;
  queue_status: "not_queued" | "pending" | "done" | "error" | string;
  queue_priority: number | null;
  queue_requested_at: string | null;
  queue_attempts: number | null;
  queue_last_error: string | null;
};

type AlbumCatalogLookupResponse = {
  ok: boolean;
  items: AlbumCatalogLookupItem[];
  total: number;
};

type AlbumDuplicateReleaseItem = {
  release_album_id: number;
  release_album_name: string;
  artist_name: string;
  album_track_rows: number;
  total_tracks: number | null;
  catalog_status: string | null;
  queue_status: "not_queued" | "pending" | "done" | "error" | string;
};

type AlbumDuplicateGroupItem = {
  spotify_album_id: string;
  spotify_album_name: string | null;
  duplicate_count: number;
  release_albums: AlbumDuplicateReleaseItem[];
};

type AlbumDuplicateLookupResponse = {
  ok: boolean;
  items: AlbumDuplicateGroupItem[];
  total: number;
};

type AlbumNameDuplicateGroupItem = {
  normalized_album_name: string;
  normalized_primary_artist: string;
  duplicate_count: number;
  spotify_album_ids: string[];
  release_albums: Array<{
    release_album_id: number;
    release_album_name: string;
    artist_name: string;
    spotify_album_id: string | null;
    spotify_album_name: string | null;
    album_track_rows: number;
    total_tracks: number | null;
    catalog_status: string | null;
    queue_status: "not_queued" | "pending" | "done" | "error" | string;
  }>;
};

type AlbumNameDuplicateLookupResponse = {
  ok: boolean;
  items: AlbumNameDuplicateGroupItem[];
  total: number;
};

type ReleaseAlbumMergePreviewResponse = {
  ok: boolean;
  survivor_release_album_id: number | null;
  merge_release_album_ids: number[];
  merge_readiness: "safe_candidate" | "needs_review" | "unsafe" | string;
  readiness_reasons: string[];
  warnings: string[];
  affected: {
    source_album_map_rows: number;
    album_artist_rows: number;
    release_track_rows: number;
    album_track_rows: number;
    album_track_conflicts: number;
    raw_play_event_rows: number;
  };
  proposed_operations: string[];
};

type ReleaseAlbumMergeDryRunResponse = {
  ok: boolean;
  blocked: boolean;
  blocked_reasons: string[];
  merge_readiness: "safe_candidate" | "needs_review" | "unsafe" | string;
  readiness_reasons: string[];
  survivor_release_album_id: number | null;
  merge_release_album_ids: number[];
  rows_affected: Record<string, number>;
  plan: {
    source_album_map_repoints: Array<Record<string, unknown>>;
    album_artist_inserts: Array<Record<string, unknown>>;
    album_artist_deletes: Array<Record<string, unknown>>;
    album_track_repoints: Array<Record<string, unknown>>;
    album_track_conflicts: Array<Record<string, unknown>>;
    release_album_retirements: Array<Record<string, unknown>>;
  };
  statements: string[];
};

type TrackDuplicateReleaseItem = {
  release_track_id: number;
  release_track_name: string;
  artist_name: string;
  release_album_name: string;
  spotify_album_id: string | null;
  catalog_status: string | null;
  queue_status: "not_queued" | "pending" | "done" | "error" | string;
};

type TrackDuplicateGroupItem = {
  spotify_track_id: string;
  spotify_track_name: string | null;
  duration_ms: number | null;
  duration_display: string | null;
  duplicate_count: number;
  release_tracks: TrackDuplicateReleaseItem[];
};

type TrackDuplicateLookupResponse = {
  ok: boolean;
  items: TrackDuplicateGroupItem[];
  total: number;
};

type TrackCatalogLookupItem = {
  release_track_id: number;
  release_track_name: string;
  artist_name: string;
  release_album_name: string;
  spotify_track_id: string | null;
  spotify_track_name: string | null;
  duration_ms: number | null;
  duration_display: string | null;
  album_id: string | null;
  catalog_fetched_at: string | null;
  catalog_last_status: string | null;
  catalog_last_error: string | null;
  queue_status: "not_queued" | "pending" | "done" | "error" | string;
  queue_priority: number | null;
  queue_requested_at: string | null;
  queue_attempts: number | null;
  queue_last_error: string | null;
};

type TrackCatalogLookupResponse = {
  ok: boolean;
  items: TrackCatalogLookupItem[];
  total: number;
};

type CatalogBackfillCoverageResponse = {
  ok: boolean;
  known_release_tracks: number;
  track_catalog_rows: number;
  track_duration_coverage_count: number;
  track_duration_coverage_percent: number;
  known_release_albums: number;
  album_catalog_rows: number;
  album_track_rows: number;
  latest_run: CatalogBackfillRunItem | null;
  recent_errors_count: number;
};

type CatalogBackfillRunResponse = {
  ok: boolean;
  run_id: number;
  status: string;
  tracks_seen: number;
  tracks_fetched: number;
  tracks_upserted: number;
  albums_seen: number;
  albums_fetched: number;
  album_tracks_upserted: number;
  album_tracklists_seen?: number;
  album_tracklists_skipped_by_policy?: number;
  album_tracklists_fetched?: number;
  skipped: number;
  errors: number;
  requests_total: number;
  requests_success: number;
  requests_429: number;
  requests_failed: number;
  initial_request_delay_seconds: number;
  final_request_delay_seconds: number;
  effective_requests_per_minute: number;
  peak_requests_last_30_seconds: number;
  max_retry_after_seconds: number;
  last_retry_after_seconds: number;
  has_more: boolean;
  warnings: string[];
  warnings_count?: number;
  partial: boolean;
  stop_reason: string | null;
  market: string;
  limit: number;
  offset: number;
  include_albums: boolean;
  force_refresh: boolean;
  album_tracklist_policy?: "all" | "priority_only" | "relevant_albums" | "none" | string;
  max_runtime_seconds: number;
  max_requests: number;
  max_errors: number;
  max_album_tracks_pages_per_album: number;
  max_429: number;
  last_error: string | null;
};

type CatalogBackfillEnqueueResponse = {
  ok: boolean;
  received: number;
  enqueued: number;
  already_complete: number;
  updated: number;
  invalid: number;
};

type UnifiedReviewItem = {
  decision_key: string;
  item_type: "group" | "track";
  title: string;
  subtitle: string;
  bucket_label: string;
  family_label: string;
  group: SuggestedAnalysisGroup | null;
  track: AmbiguousReviewItem | null;
};

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "/api";
const githubRepoUrl = "https://github.com/moshe-kahn/listen-labs";
const EXPERIENCE_MODE_STORAGE_KEY = "listenlab-experience-mode";
const LIVE_PLAYBACK_POLL_INTERVAL_MS = 10_000;
const LIVE_PLAYBACK_PROGRESS_TICK_MS = 500;
const LIVE_TRACK_END_RECENT_POLL_DELAY_MS = 3_500;
const DEFAULT_PLAYER_VOLUME = 0.8;
const PREVIEW_RAMP_START_VOLUME = 0.24;
const PREVIEW_RAMP_DURATION_MS = 4_200;
const PREVIEW_RAMP_STEP_MS = 90;
const PAGE_SIZE = 5;
const RECENT_SECTION_FETCH_LIMIT = 10;
const PLAYLISTS_PAGE_SIZE = 10;
const TRACKS_FORMULA_FETCH_LIMIT = 100;
const IDENTITY_AUDIT_AMBIGUOUS_VISIBLE_STEP = 100;
const DEBUG_SESSION_BREAK_MS = 45 * 60 * 1000;
const DEBUG_GAP_MARKER_MIN_MS = 5_000;
const DEBUG_GAP_MARKER_MAX_MS = 10 * 60 * 1000;
const RECENT_RANGE_OPTIONS = [
  { value: "short_term", label: "4 weeks" },
  { value: "medium_term", label: "6 months" },
] as const;
const MERGED_TRACK_SOURCE_FILTER_OPTIONS = [
  { value: "all", label: "All plays" },
  { value: "recent", label: "API only" },
  { value: "history", label: "History only" },
  { value: "both", label: "Matched" },
] as const;
const RECENT_DEBUG_SOURCE_FILTER_OPTIONS = [
  { value: "all", label: "All" },
  { value: "api", label: "API" },
  { value: "history", label: "History" },
  { value: "both", label: "Both" },
] as const;
const RANK_MOVEMENT_FILTER_OPTIONS = [
  { value: "all", label: "All" },
  { value: "risers", label: "Risers" },
  { value: "fallers", label: "Fallers" },
] as const;
type RecentRange = (typeof RECENT_RANGE_OPTIONS)[number]["value"];
type AnalysisMode = "quick" | "full";
type ExperienceMode = "full" | "local";
type ExperienceVisualMode = ExperienceMode | "test";
type TrackRankingMode = "plays" | "mix" | "longevity";
type RankMovementFilter = (typeof RANK_MOVEMENT_FILTER_OPTIONS)[number]["value"];
type AppPage = "dashboard" | "formulaLab" | "identityAudit" | "recentDebug" | "catalogBackfill" | "searchLookup";
type CatalogBackfillTab = "recentRuns" | "queue";
const spotifyLogoDataUrl =
  "data:image/svg+xml;utf8," +
  encodeURIComponent(
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 168 168">
      <circle cx="84" cy="84" r="84" fill="#1ed760"/>
      <path d="M121.2 113.3a6 6 0 0 1-8.3 2C90.2 101.5 61.6 98.6 27.8 106.6a6 6 0 1 1-2.8-11.7c36.8-8.8 68.3-5.5 93.8 9.9a6 6 0 0 1 2.4 8.5z" fill="#0b120f"/>
      <path d="M130.5 89.8a7.4 7.4 0 0 1-10.2 2.4c-26-16-65.6-20.7-96.3-11.4a7.4 7.4 0 0 1-4.3-14.1c35.2-10.7 79.2-5.3 108.3 12.6a7.4 7.4 0 0 1 2.5 10.5z" fill="#0b120f"/>
      <path d="M131.6 65.3C100.9 47 50.2 45.4 20.9 54.2A8.9 8.9 0 0 1 15.8 37c33.7-10.2 89.7-8.3 124.9 12.7a8.9 8.9 0 1 1-9.1 15.6z" fill="#0b120f"/>
    </svg>`,
  );
const spotifyAppsUrl = "https://www.spotify.com/us/account/apps/";

type SectionKey =
  | "artists"
  | "artistsAllTime"
  | "artistsRecent"
  | "tracks"
  | "tracksAllTime"
  | "tracksAllTimeNew"
  | "tracksAllTimeCurrent"
  | "tracksRecent"
  | "albums"
  | "albumsAllTime"
  | "albumsRecent"
  | "playlists"
  | "playlistsAllTime"
  | "playlistsRecent"
  | "recent"
  | "likes";

const INITIAL_OPEN_SECTIONS: Record<SectionKey, boolean> = {
  artists: false,
  artistsAllTime: false,
  artistsRecent: false,
  tracks: false,
  tracksAllTime: false,
  tracksRecent: false,
  tracksAllTimeNew: false,
  tracksAllTimeCurrent: false,
  albums: false,
  albumsAllTime: false,
  albumsRecent: false,
  playlists: false,
  playlistsAllTime: false,
  playlistsRecent: false,
  recent: false,
  likes: false,
};

const INITIAL_SECTION_PAGES: Record<SectionKey, number> = {
  artists: 0,
  artistsAllTime: 0,
  artistsRecent: 0,
  tracks: 0,
  tracksAllTime: 0,
  tracksAllTimeNew: 0,
  tracksAllTimeCurrent: 0,
  tracksRecent: 0,
  albums: 0,
  albumsAllTime: 0,
  albumsRecent: 0,
  playlists: 0,
  playlistsAllTime: 0,
  playlistsRecent: 0,
  recent: 0,
  likes: 0,
};

type DashboardListCardProps = {
  href?: string | null;
  entityId?: string | null;
  imageUrl?: string | null;
  imageAlt: string;
  fallbackLabel: string;
  primaryText: string;
  secondaryText?: string | null;
  tertiaryText?: string | null;
  metricText?: string | null;
  primaryBadgeText?: string | null;
  secondaryBadgeText?: string | null;
  trackUri?: string | null;
  previewTrack?: RecentTrack | null;
  primaryClamp?: "single-line-ellipsis" | "two-line-clamp";
};

type PreviewItem = {
  image: string | null;
  fallbackLabel?: string;
  label: string;
  meta: string | null;
  detail: string | null;
  kind: "artist" | "track" | "album" | "playlist";
  entityId: string | null;
  trackUri: string | null;
  url: string;
  trackId?: string | null;
  albumId?: string | null;
  artistName?: string | null;
  sourceTrack?: RecentTrack | null;
};

type RepresentativePreviewResponse = {
  track: RecentTrack | null;
  reason?: string | null;
};

type AuthTokenResponse = {
  access_token: string;
  token_type: string;
  expires_in?: number | null;
};

type RecentIngestResultResponse = {
  has_result: boolean;
  flow?: string;
  auth_succeeded?: boolean;
  ingest_succeeded?: boolean;
  error?: string | null;
  row_count?: number;
  earliest_api_played_at?: string | null;
  latest_api_played_at?: string | null;
};

type RecentBeforeProbeResponse = {
  ok: boolean;
  token_source?: string;
  days?: number;
  limit?: number;
  before_iso?: string;
  returned_items?: number;
  earliest_played_at?: string | null;
  latest_played_at?: string | null;
  detail?: string;
};

type RecentBackfillProbeResponse = {
  ok: boolean;
  token_source?: string;
  limit?: number;
  max_pages?: number;
  pages_fetched?: number;
  total_items?: number;
  earliest_played_at?: string | null;
  latest_played_at?: string | null;
  detail?: string;
};

type FullAvailabilityResponse = {
  available: boolean;
  blocked: boolean;
  reason: string;
  detail?: string | null;
  retry_after_seconds?: number | null;
};

type CurrentPlaybackSnapshot = {
  item_type: string | null;
  item_id: string | null;
  name: string | null;
  uri: string | null;
  image_url: string | null;
  artist_names: string[];
  album_name: string | null;
  device_id: string | null;
  progress_ms: number | null;
  duration_ms: number | null;
  is_playing: boolean;
  device_name: string | null;
  device_type: string | null;
  timestamp: number | null;
};

type CurrentPlaybackResponse = {
  status: "ok" | "failed" | "skipped";
  has_playback?: boolean;
  snapshot?: CurrentPlaybackSnapshot | null;
};

type PlayerTrackSummary = {
  name: string;
  artists: string;
  album: string;
  image: string | null;
  uri: string | null;
  durationMs: number;
};

type SpotifyPlayerState = {
  paused: boolean;
  position: number;
  duration: number;
  track_window: {
    current_track: {
      name: string;
      uri: string;
      duration_ms: number;
      album: { name: string; images: Array<{ url: string }> };
      artists: Array<{ name: string }>;
    };
  };
};

type AlbumTrackEntry = {
  id: string | null;
  name: string;
  uri: string | null;
  durationMs: number | null;
  artistName: string | null;
  sourceTrack: RecentTrack | null;
  lastPlayedAt: string | null;
  isSelected: boolean;
  isTopTrack: boolean;
};

type SpotifyPlayerInstance = {
  addListener: (event: string, callback: (payload: any) => void) => void;
  connect: () => Promise<boolean>;
  disconnect: () => void;
  pause: () => Promise<void>;
  resume: () => Promise<void>;
  seek: (positionMs: number) => Promise<void>;
  setVolume?: (volume: number) => Promise<void>;
  togglePlay: () => Promise<void>;
};

type PopupTrackPlaybackOptions = {
  optimisticTrack?: PlayerTrackSummary | null;
};

declare global {
  interface Window {
    onSpotifyWebPlaybackSDKReady?: () => void;
    Spotify?: {
      Player: new (options: {
        name: string;
        getOAuthToken: (callback: (token: string) => void) => void;
        volume?: number;
      }) => SpotifyPlayerInstance;
    };
  }
}

export function App() {
  const [session, setSession] = useState<SessionResponse | null>(null);
  const [profile, setProfile] = useState<ProfileResponse | null>(null);
  const [statusMessage, setStatusMessage] = useState("Checking authentication state...");
  const [statusHistory, setStatusHistory] = useState<string[]>([]);
  const [recentIngestCallbackPending, setRecentIngestCallbackPending] = useState(false);
  const [recentIngestResult, setRecentIngestResult] = useState<RecentIngestResultResponse | null>(null);
  const [recentBeforeProbeResult, setRecentBeforeProbeResult] = useState<RecentBeforeProbeResponse | null>(null);
  const [recentBackfillProbeResult, setRecentBackfillProbeResult] = useState<RecentBackfillProbeResponse | null>(null);
  const [authTransitioning, setAuthTransitioning] = useState(false);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [loadingExtendedProfile, setLoadingExtendedProfile] = useState(false);
  const [loadingRecentSection, setLoadingRecentSection] = useState(false);
  const [loadingHistoryRecompute, setLoadingHistoryRecompute] = useState(false);
  const [profileLoadAttempted, setProfileLoadAttempted] = useState(false);
  const [reloadCooldownUntil, setReloadCooldownUntil] = useState<number | null>(null);
  const [reloadCooldownDurationMs, setReloadCooldownDurationMs] = useState<number>(60_000);
  const [reloadCountdownNow, setReloadCountdownNow] = useState(Date.now());
  const [openSections, setOpenSections] = useState<Record<SectionKey, boolean>>(INITIAL_OPEN_SECTIONS);
  const [sectionPages, setSectionPages] = useState<Record<SectionKey, number>>(INITIAL_SECTION_PAGES);
  const [profileMenuOpen, setProfileMenuOpen] = useState(false);
  const [brandMenuOpen, setBrandMenuOpen] = useState(false);
  const [experimentalMenuOpen, setExperimentalMenuOpen] = useState(false);
  const [profileSettingsOpen, setProfileSettingsOpen] = useState(false);
  const [playerMenuOpen, setPlayerMenuOpen] = useState(false);
  const [rateLimitMenuOpen, setRateLimitMenuOpen] = useState(false);
  const [selectedPreview, setSelectedPreview] = useState<PreviewItem | null>(null);
  const [albumTrackEntries, setAlbumTrackEntries] = useState<AlbumTrackEntry[]>([]);
  const [albumTrackEntriesLoading, setAlbumTrackEntriesLoading] = useState(false);
  const [albumTrackEntriesError, setAlbumTrackEntriesError] = useState<string | null>(null);
  const [representativeTrack, setRepresentativeTrack] = useState<RecentTrack | null>(null);
  const [representativeLoading, setRepresentativeLoading] = useState(false);
  const [representativeReason, setRepresentativeReason] = useState<string | null>(null);
  const [playerReady, setPlayerReady] = useState(false);
  const [playerError, setPlayerError] = useState<string | null>(null);
  const [currentTrack, setCurrentTrack] = useState<PlayerTrackSummary | null>(null);
  const [playbackPaused, setPlaybackPaused] = useState(true);
  const [playbackPositionMs, setPlaybackPositionMs] = useState(0);
  const [playbackDurationMs, setPlaybackDurationMs] = useState(0);
  const [overlayTrackPlaybackExpanded, setOverlayTrackPlaybackExpanded] = useState(false);
  const [overlaySeekMs, setOverlaySeekMs] = useState<number | null>(null);
  const [pausedTimeFlashOn, setPausedTimeFlashOn] = useState(true);
  const [previewingTrackUri, setPreviewingTrackUri] = useState<string | null>(null);
  const [previewPlayedTrackKeys, setPreviewPlayedTrackKeys] = useState<Set<string>>(new Set());
  const [livePlaybackSnapshot, setLivePlaybackSnapshot] = useState<CurrentPlaybackSnapshot | null>(null);
  const [liveDerivedProgressMs, setLiveDerivedProgressMs] = useState(0);
  const [liveAwaitingNextTrack, setLiveAwaitingNextTrack] = useState(false);
  const [livePlaybackProbeComplete, setLivePlaybackProbeComplete] = useState(false);
  const [pendingSeekMs, setPendingSeekMs] = useState<number | null>(null);
  const [liveControlOverrideUntilMs, setLiveControlOverrideUntilMs] = useState<number | null>(null);
  const [recentRange, setRecentRange] = useState<RecentRange>("short_term");
  const [trackRankingMode, setTrackRankingMode] = useState<TrackRankingMode>("plays");
  const [trackRankingRefreshPending, setTrackRankingRefreshPending] = useState(false);
  const [appPage, setAppPage] = useState<AppPage>("dashboard");
  const [showDebugLinkFields, setShowDebugLinkFields] = useState(false);
  const [openDebugSessions, setOpenDebugSessions] = useState<Record<string, boolean>>({});
  const [openDebugTracks, setOpenDebugTracks] = useState<Record<string, boolean>>({});
  const [listeningLogTracks, setListeningLogTracks] = useState<RecentTrack[]>([]);
  const [listeningLogHasMore, setListeningLogHasMore] = useState(false);
  const [listeningLogOffset, setListeningLogOffset] = useState(0);
  const [listeningLogLoading, setListeningLogLoading] = useState(false);
  const [listeningLogLoaded, setListeningLogLoaded] = useState(false);
  const [listeningLogError, setListeningLogError] = useState("");
  const [listeningLogLastLoadedAt, setListeningLogLastLoadedAt] = useState<number | null>(null);
  const [recentDebugSourceFilter, setRecentDebugSourceFilter] = useState<RecentDebugSourceFilter>("all");
  const [catalogBackfillCoverage, setCatalogBackfillCoverage] = useState<CatalogBackfillCoverageResponse | null>(null);
  const [catalogBackfillCoverageLoading, setCatalogBackfillCoverageLoading] = useState(false);
  const [catalogBackfillCoverageLoaded, setCatalogBackfillCoverageLoaded] = useState(false);
  const [catalogBackfillCoverageError, setCatalogBackfillCoverageError] = useState("");
  const [catalogBackfillCoverageLastLoadedAt, setCatalogBackfillCoverageLastLoadedAt] = useState<number | null>(null);
  const [catalogBackfillRuns, setCatalogBackfillRuns] = useState<CatalogBackfillRunsResponse | null>(null);
  const [catalogBackfillRunsLoading, setCatalogBackfillRunsLoading] = useState(false);
  const [catalogBackfillRunsLoaded, setCatalogBackfillRunsLoaded] = useState(false);
  const [catalogBackfillRunsError, setCatalogBackfillRunsError] = useState("");
  const [catalogBackfillRunsLastLoadedAt, setCatalogBackfillRunsLastLoadedAt] = useState<number | null>(null);
  const [catalogBackfillQueue, setCatalogBackfillQueue] = useState<CatalogBackfillQueueResponse | null>(null);
  const [catalogBackfillQueueLoading, setCatalogBackfillQueueLoading] = useState(false);
  const [catalogBackfillQueueLoaded, setCatalogBackfillQueueLoaded] = useState(false);
  const [catalogBackfillQueueError, setCatalogBackfillQueueError] = useState("");
  const [catalogBackfillQueueLastLoadedAt, setCatalogBackfillQueueLastLoadedAt] = useState<number | null>(null);
  const [catalogBackfillQueueStatusFilter, setCatalogBackfillQueueStatusFilter] = useState<"all" | "pending" | "done" | "error">("all");
  const [catalogBackfillTab, setCatalogBackfillTab] = useState<CatalogBackfillTab>("recentRuns");
  const [catalogBackfillQueueRepairLoading, setCatalogBackfillQueueRepairLoading] = useState(false);
  const [catalogBackfillQueueRepairMessage, setCatalogBackfillQueueRepairMessage] = useState("");
  const [catalogBackfillRunLoading, setCatalogBackfillRunLoading] = useState(false);
  const [catalogBackfillRunError, setCatalogBackfillRunError] = useState("");
  const [catalogBackfillLatestResult, setCatalogBackfillLatestResult] = useState<CatalogBackfillRunResponse | null>(null);
  const [catalogBackfillLimit, setCatalogBackfillLimit] = useState(25);
  const [catalogBackfillOffset, setCatalogBackfillOffset] = useState(0);
  const [catalogBackfillMarket, setCatalogBackfillMarket] = useState("US");
  const [catalogBackfillIncludeAlbums, setCatalogBackfillIncludeAlbums] = useState(true);
  const [catalogBackfillForceRefresh, setCatalogBackfillForceRefresh] = useState(false);
  const [catalogBackfillRequestDelaySeconds, setCatalogBackfillRequestDelaySeconds] = useState(0.5);
  const [catalogBackfillMaxRuntimeSeconds, setCatalogBackfillMaxRuntimeSeconds] = useState(60);
  const [catalogBackfillMaxRequests, setCatalogBackfillMaxRequests] = useState(150);
  const [catalogBackfillMaxErrors, setCatalogBackfillMaxErrors] = useState(10);
  const [catalogBackfillMaxAlbumTracksPagesPerAlbum, setCatalogBackfillMaxAlbumTracksPagesPerAlbum] = useState(10);
  const [catalogBackfillMax429, setCatalogBackfillMax429] = useState(3);
  const [catalogBackfillAlbumTracklistPolicy, setCatalogBackfillAlbumTracklistPolicy] = useState<"all" | "priority_only" | "relevant_albums" | "none">("relevant_albums");
  const [searchLookupEntityType, setSearchLookupEntityType] = useState<"albums" | "tracks" | "duplicate_albums">("albums");
  const [searchLookupQueueStatus, setSearchLookupQueueStatus] = useState<"all" | "not_queued" | "pending" | "done" | "error">("all");
  const [searchLookupSort, setSearchLookupSort] = useState<"default" | "recently_backfilled" | "name" | "incomplete_first">("default");
  const [albumCatalogLookupQ, setAlbumCatalogLookupQ] = useState("");
  const [albumCatalogLookupStatus, setAlbumCatalogLookupStatus] = useState<"all" | "backfilled" | "not_backfilled" | "tracklist_complete" | "tracklist_incomplete" | "error">("all");
  const [albumCatalogLookupResult, setAlbumCatalogLookupResult] = useState<AlbumCatalogLookupResponse | null>(null);
  const [albumCatalogLookupLoading, setAlbumCatalogLookupLoading] = useState(false);
  const [albumCatalogLookupLoaded, setAlbumCatalogLookupLoaded] = useState(false);
  const [albumCatalogLookupError, setAlbumCatalogLookupError] = useState("");
  const [albumCatalogLookupLastLoadedAt, setAlbumCatalogLookupLastLoadedAt] = useState<number | null>(null);
  const [trackCatalogLookupStatus, setTrackCatalogLookupStatus] = useState<"all" | "backfilled" | "not_backfilled" | "duration_missing" | "error">("all");
  const [trackCatalogLookupResult, setTrackCatalogLookupResult] = useState<TrackCatalogLookupResponse | null>(null);
  const [trackCatalogLookupLoading, setTrackCatalogLookupLoading] = useState(false);
  const [trackCatalogLookupLoaded, setTrackCatalogLookupLoaded] = useState(false);
  const [trackCatalogLookupError, setTrackCatalogLookupError] = useState("");
  const [trackCatalogLookupLastLoadedAt, setTrackCatalogLookupLastLoadedAt] = useState<number | null>(null);
  const [albumDuplicateLookupResult, setAlbumDuplicateLookupResult] = useState<AlbumDuplicateLookupResponse | null>(null);
  const [albumDuplicateLookupLoading, setAlbumDuplicateLookupLoading] = useState(false);
  const [albumDuplicateLookupLoaded, setAlbumDuplicateLookupLoaded] = useState(false);
  const [albumDuplicateLookupError, setAlbumDuplicateLookupError] = useState("");
  const [albumDuplicateLookupLastLoadedAt, setAlbumDuplicateLookupLastLoadedAt] = useState<number | null>(null);
  const [albumNameDuplicateLookupResult, setAlbumNameDuplicateLookupResult] = useState<AlbumNameDuplicateLookupResponse | null>(null);
  const [albumNameDuplicateLookupLoading, setAlbumNameDuplicateLookupLoading] = useState(false);
  const [albumNameDuplicateLookupLoaded, setAlbumNameDuplicateLookupLoaded] = useState(false);
  const [albumNameDuplicateLookupError, setAlbumNameDuplicateLookupError] = useState("");
  const [albumNameDuplicateLookupLastLoadedAt, setAlbumNameDuplicateLookupLastLoadedAt] = useState<number | null>(null);
  const [releaseAlbumMergePreviewByKey, setReleaseAlbumMergePreviewByKey] = useState<Record<string, ReleaseAlbumMergePreviewResponse>>({});
  const [releaseAlbumMergePreviewLoadingKey, setReleaseAlbumMergePreviewLoadingKey] = useState<string | null>(null);
  const [releaseAlbumMergePreviewErrorByKey, setReleaseAlbumMergePreviewErrorByKey] = useState<Record<string, string>>({});
  const [releaseAlbumMergeDryRunByKey, setReleaseAlbumMergeDryRunByKey] = useState<Record<string, ReleaseAlbumMergeDryRunResponse>>({});
  const [releaseAlbumMergeDryRunLoadingKey, setReleaseAlbumMergeDryRunLoadingKey] = useState<string | null>(null);
  const [releaseAlbumMergeDryRunErrorByKey, setReleaseAlbumMergeDryRunErrorByKey] = useState<Record<string, string>>({});
  const [trackDuplicateLookupResult, setTrackDuplicateLookupResult] = useState<TrackDuplicateLookupResponse | null>(null);
  const [trackDuplicateLookupLoading, setTrackDuplicateLookupLoading] = useState(false);
  const [trackDuplicateLookupLoaded, setTrackDuplicateLookupLoaded] = useState(false);
  const [trackDuplicateLookupError, setTrackDuplicateLookupError] = useState("");
  const [trackDuplicateLookupLastLoadedAt, setTrackDuplicateLookupLastLoadedAt] = useState<number | null>(null);
  const [albumCatalogLookupEnqueueLoading, setAlbumCatalogLookupEnqueueLoading] = useState(false);
  const [albumCatalogLookupEnqueueError, setAlbumCatalogLookupEnqueueError] = useState("");
  const [albumCatalogLookupEnqueueResult, setAlbumCatalogLookupEnqueueResult] = useState<CatalogBackfillEnqueueResponse | null>(null);
  const [mergedTracks, setMergedTracks] = useState<RecentTrack[]>([]);
  const [mergedTracksLoading, setMergedTracksLoading] = useState(false);
  const [mergedTracksLoaded, setMergedTracksLoaded] = useState(false);
  const [mergedTracksError, setMergedTracksError] = useState("");
  const [mergedTracksExcludedUnknownCount, setMergedTracksExcludedUnknownCount] = useState(0);
  const [mergedTracksLastLoadedAt, setMergedTracksLastLoadedAt] = useState<number | null>(null);
  const [mergedTrackSourceFilter, setMergedTrackSourceFilter] = useState<MergedTrackSourceFilter>("all");
  const [rankMovementFilter, setRankMovementFilter] = useState<RankMovementFilter>("all");
  const [identityAudit, setIdentityAudit] = useState<TrackIdentityAuditResponse | null>(null);
  const [identityAuditLoading, setIdentityAuditLoading] = useState(false);
  const [identityAuditLoaded, setIdentityAuditLoaded] = useState(false);
  const [identityAuditError, setIdentityAuditError] = useState("");
  const [identityAuditLastLoadedAt, setIdentityAuditLastLoadedAt] = useState<number | null>(null);
  const [identityAuditTab, setIdentityAuditTab] = useState<IdentityAuditTab>("overview");
  const [identityAuditSuggestedGroups, setIdentityAuditSuggestedGroups] = useState<SuggestedGroupsResponse | null>(null);
  const [identityAuditSuggestedLoading, setIdentityAuditSuggestedLoading] = useState(false);
  const [identityAuditSuggestedLoaded, setIdentityAuditSuggestedLoaded] = useState(false);
  const [identityAuditSuggestedError, setIdentityAuditSuggestedError] = useState("");
  const [identityAuditSuggestedLastLoadedAt, setIdentityAuditSuggestedLastLoadedAt] = useState<number | null>(null);
  const [identityAuditAmbiguous, setIdentityAuditAmbiguous] = useState<AmbiguousReviewResponse | null>(null);
  const [identityAuditAmbiguousLoading, setIdentityAuditAmbiguousLoading] = useState(false);
  const [identityAuditAmbiguousLoaded, setIdentityAuditAmbiguousLoaded] = useState(false);
  const [identityAuditAmbiguousError, setIdentityAuditAmbiguousError] = useState("");
  const [identityAuditAmbiguousLastLoadedAt, setIdentityAuditAmbiguousLastLoadedAt] = useState<number | null>(null);
  const [identityAuditAmbiguousFamilyFilter, setIdentityAuditAmbiguousFamilyFilter] = useState("all");
  const [identityAuditAmbiguousBucketFilter, setIdentityAuditAmbiguousBucketFilter] = useState<"all" | "grouped" | "ungrouped">("all");
  const [identityAuditAmbiguousVisibleCount, setIdentityAuditAmbiguousVisibleCount] = useState(IDENTITY_AUDIT_AMBIGUOUS_VISIBLE_STEP);
  const [identityAuditLocalDecisions, setIdentityAuditLocalDecisions] = useState<Record<string, LocalReviewDecision>>({});
  const [identityAuditFocusedReviewKey, setIdentityAuditFocusedReviewKey] = useState<string | null>(null);
  const [identityAuditPreviewCopyStatus, setIdentityAuditPreviewCopyStatus] = useState("");
  const [identityAuditPreviewValidationLoading, setIdentityAuditPreviewValidationLoading] = useState(false);
  const [identityAuditPreviewValidationError, setIdentityAuditPreviewValidationError] = useState("");
  const [identityAuditPreviewValidationResult, setIdentityAuditPreviewValidationResult] = useState<SubmissionPreviewValidationResponse | null>(null);
  const [identityAuditPreviewValidatedAt, setIdentityAuditPreviewValidatedAt] = useState<number | null>(null);
  const [identityAuditSubmissionSaveLoading, setIdentityAuditSubmissionSaveLoading] = useState(false);
  const [identityAuditSubmissionSaveError, setIdentityAuditSubmissionSaveError] = useState("");
  const [identityAuditSubmissionSaveResult, setIdentityAuditSubmissionSaveResult] = useState<IdentityAuditSubmissionSaveResponse | null>(null);
  const [identityAuditSavedSubmissions, setIdentityAuditSavedSubmissions] = useState<IdentityAuditSavedSubmissionListResponse | null>(null);
  const [identityAuditSavedSubmissionsLoading, setIdentityAuditSavedSubmissionsLoading] = useState(false);
  const [identityAuditSavedSubmissionsError, setIdentityAuditSavedSubmissionsError] = useState("");
  const [identityAuditSavedSubmissionDetail, setIdentityAuditSavedSubmissionDetail] = useState<IdentityAuditSavedSubmissionReadResponse | null>(null);
  const [identityAuditSavedSubmissionDetailLoading, setIdentityAuditSavedSubmissionDetailLoading] = useState(false);
  const [identityAuditSavedSubmissionDetailError, setIdentityAuditSavedSubmissionDetailError] = useState("");
  const [identityAuditSavedSubmissionDryRun, setIdentityAuditSavedSubmissionDryRun] = useState<IdentityAuditSubmissionDryRunResponse | null>(null);
  const [identityAuditSavedSubmissionDryRunLoading, setIdentityAuditSavedSubmissionDryRunLoading] = useState(false);
  const [identityAuditSavedSubmissionDryRunError, setIdentityAuditSavedSubmissionDryRunError] = useState("");
  const [identityAuditSavedSubmissionDryRunAt, setIdentityAuditSavedSubmissionDryRunAt] = useState<number | null>(null);
  const [recentRangeRefreshPending, setRecentRangeRefreshPending] = useState(false);
  const [analysisMode, setAnalysisMode] = useState<AnalysisMode>("quick");
  const [experienceMode, setExperienceMode] = useState<ExperienceMode>(() => {
    const stored = window.localStorage.getItem(EXPERIENCE_MODE_STORAGE_KEY);
    return stored === "local" ? "local" : "full";
  });
  const [testingFullExperience, setTestingFullExperience] = useState(false);
  const [testFullSuccessPinned, setTestFullSuccessPinned] = useState(false);
  const [testProbeModeVisual, setTestProbeModeVisual] = useState<ExperienceVisualMode | null>(null);
  const profileMenuRef = useRef<HTMLDivElement | null>(null);
  const brandMenuRef = useRef<HTMLDivElement | null>(null);
  const experimentalMenuRef = useRef<HTMLDivElement | null>(null);
  const playerMenuRef = useRef<HTMLDivElement | null>(null);
  const rateLimitMenuRef = useRef<HTMLDivElement | null>(null);
  const spotifyPlayerRef = useRef<SpotifyPlayerInstance | null>(null);
  const spotifyDeviceIdRef = useRef<string | null>(null);
  const previewStopTimerRef = useRef<number | null>(null);
  const previewVolumeRampTimerRef = useRef<number | null>(null);
  const previewingTrackUriRef = useRef<string | null>(null);
  const currentPlayerVolumeRef = useRef(DEFAULT_PLAYER_VOLUME);
  const loadedAlbumTracksAlbumIdRef = useRef<string | null>(null);
  const liveProgressAnchorRef = useRef<{ baseProgressMs: number; receivedAtMs: number; durationMs: number } | null>(null);
  const liveEndRefreshRequestedRef = useRef(false);
  const profileLoadInFlightRef = useRef(false);
  const extendedLoadInFlightRef = useRef(false);
  const quickRecentAutoAttemptRef = useRef<string | null>(null);
  const hasPremiumPlayback = profile?.product?.toLowerCase() === "premium";
  const usingLivePlaybackSnapshot = Boolean(livePlaybackSnapshot);
  const liveControlOverrideActive = Boolean(
    liveControlOverrideUntilMs != null
    && liveControlOverrideUntilMs > Date.now()
    && playerReady,
  );
  const livePlaybackOnListenLabDevice = Boolean(
    usingLivePlaybackSnapshot
    && (
      (
        livePlaybackSnapshot?.device_id
        && spotifyDeviceIdRef.current
        && livePlaybackSnapshot.device_id === spotifyDeviceIdRef.current
      )
      || ((livePlaybackSnapshot?.device_name ?? "").toLocaleLowerCase().includes("listenlab"))
    ),
  );
  const liveReadOnlyMode = usingLivePlaybackSnapshot && !livePlaybackOnListenLabDevice && !liveControlOverrideActive;
  const shouldUseLiveSnapshotDisplay = liveReadOnlyMode || (usingLivePlaybackSnapshot && !currentTrack);
  const playerDisplayTrack: PlayerTrackSummary | null = shouldUseLiveSnapshotDisplay
    ? {
      name: livePlaybackSnapshot?.name ?? "Spotify Playback",
      artists: (livePlaybackSnapshot?.artist_names ?? []).join(", ") || "Unknown artist",
      album: livePlaybackSnapshot?.album_name ?? "Unknown album",
      image: livePlaybackSnapshot?.image_url ?? null,
      uri: livePlaybackSnapshot?.uri ?? null,
      durationMs: Math.max(0, Number(livePlaybackSnapshot?.duration_ms ?? 0)),
    }
    : currentTrack;
  const playerDisplayPaused = shouldUseLiveSnapshotDisplay
    ? !Boolean(livePlaybackSnapshot?.is_playing)
    : playbackPaused;
  const playerDisplayPositionMs = shouldUseLiveSnapshotDisplay
    ? Math.max(0, liveDerivedProgressMs)
    : playbackPositionMs;
  const playerDisplayDurationMs = shouldUseLiveSnapshotDisplay
    ? Math.max(0, Number(livePlaybackSnapshot?.duration_ms ?? 0))
    : playbackDurationMs;
  const selectedPreviewPrimaryArtistName = selectedPreview?.kind === "track"
    ? (
      firstArtistFromRecentTrack(selectedPreview.sourceTrack)?.name
      ?? selectedPreview.artistName
      ?? primaryArtistName(selectedPreview.meta)
      ?? null
    )
    : null;
  const selectedPreviewArtistImageUrl = selectedPreview && selectedPreview.kind === "track"
    ? findArtistImageUrl(selectedPreviewPrimaryArtistName ?? selectedPreview.artistName ?? selectedPreview.meta)
    : null;
  const selectedPreviewCanOpenArtist = Boolean(selectedPreview?.kind === "track" && selectedPreviewPrimaryArtistName);
  const selectedPreviewCanOpenAlbum = Boolean(
    selectedPreview?.kind === "track"
    && (selectedPreview.albumId || selectedPreview.sourceTrack?.album_id || selectedPreview.sourceTrack?.album_name || selectedPreview.detail),
  );
  const selectedPreviewMatchedAlbumTrack = selectedPreview?.kind === "track"
    ? (
      albumTrackEntries.find((row) => {
        const rowTrackUri = trackUriWithFallback(row.uri, row.id);
        if (selectedPreview.trackId && row.id && selectedPreview.trackId === row.id) {
          return true;
        }
        if (selectedPreview.trackUri && rowTrackUri && selectedPreview.trackUri === rowTrackUri) {
          return true;
        }
        return false;
      }) ?? null
    )
    : null;
  const selectedPreviewEffectiveTrackUri = selectedPreview?.kind === "track"
    ? trackUriWithFallback(
      selectedPreviewMatchedAlbumTrack?.uri ?? selectedPreview.trackUri,
      selectedPreview.trackId ?? selectedPreviewMatchedAlbumTrack?.id ?? null,
    )
    : null;
  const selectedPreviewTrackIsCurrent = Boolean(
    selectedPreview?.kind === "track"
    && selectedPreviewEffectiveTrackUri
    && currentTrack?.uri === selectedPreviewEffectiveTrackUri,
  );
  const selectedPreviewTrackBaseDurationMs = selectedPreview?.kind === "track"
    ? (
      selectedPreviewMatchedAlbumTrack?.durationMs
      ?? selectedPreview.sourceTrack?.duration_ms
      ?? (selectedPreviewTrackIsCurrent
        ? (playbackDurationMs > 0 ? playbackDurationMs : currentTrack?.durationMs ?? null)
        : null)
    )
    : null;
  const selectedPreviewTrackElapsedMs = selectedPreviewTrackIsCurrent
    ? (
      selectedPreviewTrackBaseDurationMs != null
        ? Math.min(Math.max(0, playbackPositionMs), selectedPreviewTrackBaseDurationMs)
        : Math.max(0, playbackPositionMs)
    )
    : 0;
  const selectedPreviewTrackElapsedDisplayMs = selectedPreviewTrackIsCurrent ? selectedPreviewTrackElapsedMs : 0;
  const selectedPreviewTrackTotalDisplayMs = selectedPreviewTrackBaseDurationMs ?? 0;
  const selectedPreviewTrackProgressPercent = selectedPreviewTrackBaseDurationMs != null && selectedPreviewTrackBaseDurationMs > 0
    ? Math.max(0, Math.min(100, (selectedPreviewTrackElapsedMs / selectedPreviewTrackBaseDurationMs) * 100))
    : 0;
  const selectedPreviewTrackOptimisticSummary: PlayerTrackSummary | null = selectedPreview?.kind === "track"
    ? {
      name: selectedPreview.label,
      artists: selectedPreviewPrimaryArtistName ?? selectedPreview.meta ?? "Unknown artist",
      album: selectedPreview.sourceTrack?.album_name ?? selectedPreview.detail ?? "Unknown album",
      image: selectedPreview.image ?? null,
      uri: selectedPreviewEffectiveTrackUri,
      durationMs: selectedPreviewTrackBaseDurationMs ?? 0,
    }
    : null;
  const knownPlayerTracks = profile
    ? [
      ...(profile.recent_tracks ?? []),
      ...(profile.top_tracks ?? []),
      ...(profile.recent_top_tracks ?? []),
      ...(profile.recent_likes_tracks ?? []),
    ]
    : [];
  const playerDisplayTrackId = playerDisplayTrack ? spotifyTrackIdFromUri(playerDisplayTrack.uri) : null;
  const playerDisplayPrimaryArtistFromDisplay = primaryArtistName(playerDisplayTrack?.artists ?? null);
  const playerDisplayKnownTrack = playerDisplayTrack && knownPlayerTracks.length > 0
    ? (
      knownPlayerTracks.find((track) => {
        if (playerDisplayTrackId && track.track_id && track.track_id === playerDisplayTrackId) {
          return true;
        }
        return normalizedTrackArtistKey(track.track_name, track.artist_name) === normalizedTrackArtistKey(
          playerDisplayTrack.name,
          playerDisplayTrack.artists,
        );
      }) ?? null
    )
    : null;
  const playerDisplayArtist = firstArtistFromRecentTrack(playerDisplayKnownTrack);
  const playerDisplayArtistName = playerDisplayArtist?.name ?? playerDisplayPrimaryArtistFromDisplay ?? null;
  const playerDisplayArtistId = playerDisplayArtist?.artist_id ?? playerDisplayArtist?.id ?? null;
  const playerDisplayAlbumName = playerDisplayKnownTrack?.album_name ?? playerDisplayTrack?.album ?? null;
  const playerDisplayAlbumId = playerDisplayKnownTrack?.album_id ?? null;
  const playerDisplayAlbumYear = playerDisplayKnownTrack?.album_release_year ?? null;
  const playerDisplayAlbumLabel = playerDisplayAlbumName
    ? (playerDisplayAlbumYear ? `${playerDisplayAlbumYear} - ${playerDisplayAlbumName}` : playerDisplayAlbumName)
    : "Choose something to play";
  const canControlPlayback = !liveReadOnlyMode || livePlaybackOnListenLabDevice;
  const canSeekSelectedPreview = Boolean(
    canControlPlayback
    && selectedPreviewTrackIsCurrent
    && selectedPreviewTrackBaseDurationMs != null
    && selectedPreviewTrackBaseDurationMs > 0,
  );
  const livePlaybackControlTooltip = liveReadOnlyMode
    ? `Playing on ${livePlaybackSnapshot?.device_name ?? "another device"}. Click to control on ListenLab.`
    : undefined;

  function clampProgress(progressMs: number, durationMs: number) {
    const safeDuration = Math.max(0, Number(durationMs || 0));
    const safeProgress = Math.max(0, Number(progressMs || 0));
    return safeDuration > 0 ? Math.min(safeProgress, safeDuration) : safeProgress;
  }
  const reloadSecondsRemaining =
    reloadCooldownUntil == null ? 0 : Math.max(0, Math.ceil((reloadCooldownUntil - reloadCountdownNow) / 1000));
  const reloadReady = reloadSecondsRemaining <= 0;
  const spotifyCooldownActive = reloadCooldownUntil != null && !reloadReady;
  const showRateLimitReload = experienceMode === "full" && (spotifyCooldownActive || Boolean(statusMessage && statusMessage.includes("rate-limiting")));
  const reloadProgress =
    reloadCooldownUntil == null
      ? 1
      : Math.max(0, Math.min(1, 1 - (reloadCooldownUntil - reloadCountdownNow) / Math.max(reloadCooldownDurationMs, 1)));

  function groupDecisionKey(group: SuggestedAnalysisGroup): string {
    return `group:${group.analysis_track_id}`;
  }

  function trackDecisionKey(track: AmbiguousReviewItem): string {
    return `track:${track.entry_id}`;
  }

  function isReviewedDecision(decision: LocalReviewDecision | undefined): boolean {
    return Boolean(decision && decision.verdict !== "unsure");
  }

  function computeAmbiguousTrackItems(): AmbiguousReviewItem[] {
    const allItems = identityAuditAmbiguous?.items ?? [];
    return allItems.filter((item) => {
      if (identityAuditAmbiguousFamilyFilter !== "all") {
        if (!item.review_families.map((name) => name.toLowerCase()).includes(identityAuditAmbiguousFamilyFilter.toLowerCase())) {
          return false;
        }
      }
      if (identityAuditAmbiguousBucketFilter !== "all" && item.bucket !== identityAuditAmbiguousBucketFilter) {
        return false;
      }
      return true;
    });
  }

  function computeUnifiedReviewItems(): UnifiedReviewItem[] {
    const suggestedItems = identityAuditSuggestedGroups?.items ?? [];
    const trackItems = computeAmbiguousTrackItems();
    const suggested = suggestedItems.map((group): UnifiedReviewItem => ({
      decision_key: groupDecisionKey(group),
      item_type: "group",
      title: group.analysis_track_name || `Analysis Track ${group.analysis_track_id}`,
      subtitle: `${group.release_track_count} release tracks | ${Math.round(group.confidence * 100)}% confidence`,
      bucket_label: "Suggested groups",
      family_label: group.song_family_key || "Suggested groups",
      group,
      track: null,
    }));
    const tracks = trackItems.map((track): UnifiedReviewItem => ({
      decision_key: trackDecisionKey(track),
      item_type: "track",
      title: track.release_track_name,
      subtitle: `${track.artist_name} | ${track.bucket}`,
      bucket_label: "Ambiguous tracks",
      family_label: track.dominant_family || track.review_families[0] || track.bucket || "Ambiguous tracks",
      group: null,
      track,
    }));
    return [...suggested, ...tracks];
  }

  function findNextUnreviewedDecisionKey(
    items: UnifiedReviewItem[],
    afterKey: string | null = null,
    decisions: Record<string, LocalReviewDecision> = identityAuditLocalDecisions,
  ): string | null {
    if (items.length === 0) {
      return null;
    }
    const startIndex = afterKey == null ? -1 : items.findIndex((item) => item.decision_key === afterKey);
    for (let index = startIndex + 1; index < items.length; index += 1) {
      if (!isReviewedDecision(decisions[items[index].decision_key])) {
        return items[index].decision_key;
      }
    }
    for (let index = 0; index <= startIndex; index += 1) {
      if (!isReviewedDecision(decisions[items[index].decision_key])) {
        return items[index].decision_key;
      }
    }
    return null;
  }

  useEffect(() => {
    if (appPage !== "recentDebug" || !profile || listeningLogLoaded || listeningLogLoading) {
      return;
    }
    void loadListeningLogBatch(true);
  }, [appPage, listeningLogLoaded, listeningLogLoading, profile, recentDebugSourceFilter]);

  useEffect(() => {
    if (appPage !== "formulaLab" || !profile) {
      return;
    }
    if (!mergedTracksLoaded && !mergedTracksLoading) {
      void loadMergedTrackRankings();
    }
  }, [
    appPage,
    mergedTracksLoaded,
    mergedTracksLoading,
    profile,
  ]);

  useEffect(() => {
    if (appPage !== "searchLookup" || !profile) {
      return;
    }
    if (searchLookupEntityType === "duplicate_albums" && !albumDuplicateLookupLoaded && !albumDuplicateLookupLoading) {
      void loadAlbumDuplicateLookup(true);
      return;
    }
    if (searchLookupEntityType === "tracks" && !trackCatalogLookupLoaded && !trackCatalogLookupLoading) {
      void loadTrackCatalogLookup(true);
      return;
    }
    if (searchLookupEntityType === "albums" && !albumCatalogLookupLoaded && !albumCatalogLookupLoading) {
      void loadAlbumCatalogLookup(true);
    }
  }, [
    appPage,
    profile,
    searchLookupEntityType,
    albumCatalogLookupLoaded,
    albumCatalogLookupLoading,
    trackCatalogLookupLoaded,
    trackCatalogLookupLoading,
    albumDuplicateLookupLoaded,
    albumDuplicateLookupLoading,
  ]);

  useEffect(() => {
    if (appPage !== "catalogBackfill" || !profile) {
      return;
    }
    if (!catalogBackfillCoverageLoaded && !catalogBackfillCoverageLoading) {
      void loadCatalogBackfillCoverage();
    }
    if (!catalogBackfillRunsLoaded && !catalogBackfillRunsLoading) {
      void loadCatalogBackfillRuns();
    }
    if (!catalogBackfillQueueLoaded && !catalogBackfillQueueLoading) {
      void loadCatalogBackfillQueue();
    }
  }, [
    appPage,
    profile,
    catalogBackfillCoverageLoaded,
    catalogBackfillCoverageLoading,
    catalogBackfillRunsLoaded,
    catalogBackfillRunsLoading,
    catalogBackfillQueueLoaded,
    catalogBackfillQueueLoading,
  ]);

  useEffect(() => {
    if (appPage !== "identityAudit" || !profile) {
      return;
    }
    if (!identityAuditLoaded && !identityAuditLoading) {
      void loadIdentityAudit();
    }
    if (!identityAuditSuggestedLoaded && !identityAuditSuggestedLoading) {
      void loadIdentityAuditSuggestedGroups();
    }
    if (!identityAuditAmbiguousLoaded && !identityAuditAmbiguousLoading) {
      void loadIdentityAuditAmbiguousReview();
    }
    if (!identityAuditSavedSubmissions && !identityAuditSavedSubmissionsLoading) {
      void loadIdentityAuditSavedSubmissions();
    }
  }, [
    appPage,
    identityAuditAmbiguousLoaded,
    identityAuditAmbiguousLoading,
    identityAuditLoaded,
    identityAuditLoading,
    identityAuditSuggestedLoaded,
    identityAuditSuggestedLoading,
    identityAuditSavedSubmissions,
    identityAuditSavedSubmissionsLoading,
    profile,
  ]);

  useEffect(() => {
    setIdentityAuditSavedSubmissionDryRun(null);
    setIdentityAuditSavedSubmissionDryRunError("");
    setIdentityAuditSavedSubmissionDryRunLoading(false);
    setIdentityAuditSavedSubmissionDryRunAt(null);
  }, [identityAuditSavedSubmissionDetail?.item.id]);

  useEffect(() => {
    setIdentityAuditAmbiguousVisibleCount(IDENTITY_AUDIT_AMBIGUOUS_VISIBLE_STEP);
  }, [identityAuditAmbiguousFamilyFilter, identityAuditAmbiguousBucketFilter]);

  useEffect(() => {
    setIdentityAuditPreviewValidationResult(null);
    setIdentityAuditPreviewValidationError("");
    setIdentityAuditPreviewValidatedAt(null);
    setIdentityAuditSubmissionSaveLoading(false);
    setIdentityAuditSubmissionSaveError("");
    setIdentityAuditSubmissionSaveResult(null);
  }, [identityAuditLocalDecisions]);

  useEffect(() => {
    if (appPage !== "identityAudit" || identityAuditTab !== "family") {
      return;
    }
    const unifiedItems = computeUnifiedReviewItems();
    if (unifiedItems.length === 0) {
      if (identityAuditFocusedReviewKey != null) {
        setIdentityAuditFocusedReviewKey(null);
      }
      return;
    }
    const hasCurrentFocus = identityAuditFocusedReviewKey != null
      && unifiedItems.some((item) => item.decision_key === identityAuditFocusedReviewKey);
    const focusedReviewed = hasCurrentFocus
      ? isReviewedDecision(identityAuditLocalDecisions[identityAuditFocusedReviewKey as string])
      : false;
    if (!hasCurrentFocus || focusedReviewed) {
      const nextKey = findNextUnreviewedDecisionKey(unifiedItems, identityAuditFocusedReviewKey);
      setIdentityAuditFocusedReviewKey(nextKey);
    }
  }, [
    appPage,
    identityAuditTab,
    identityAuditFocusedReviewKey,
    identityAuditLocalDecisions,
    identityAuditSuggestedGroups,
    identityAuditAmbiguous,
    identityAuditAmbiguousFamilyFilter,
    identityAuditAmbiguousBucketFilter,
  ]);

  useEffect(() => {
    if (appPage !== "identityAudit" || identityAuditTab !== "family") {
      return;
    }

    function onKeydown(event: KeyboardEvent) {
      const target = event.target as HTMLElement | null;
      const isTypingTarget = Boolean(
        target
        && (
          target.tagName === "INPUT"
          || target.tagName === "TEXTAREA"
          || target.tagName === "SELECT"
          || target.isContentEditable
        )
      );
      if (isTypingTarget) {
        return;
      }
      if (event.metaKey || event.ctrlKey || event.altKey) {
        return;
      }

      const unifiedItems = computeUnifiedReviewItems();
      if (unifiedItems.length === 0) {
        return;
      }
      const focusedKey = identityAuditFocusedReviewKey ?? findNextUnreviewedDecisionKey(unifiedItems);

      if (event.key.toLowerCase() === "n") {
        event.preventDefault();
        setIdentityAuditFocusedReviewKey(findNextUnreviewedDecisionKey(unifiedItems, focusedKey));
        return;
      }

      if (!focusedKey) {
        return;
      }

      if (event.key.toLowerCase() === "a") {
        event.preventDefault();
        const nextDecisions = {
          ...identityAuditLocalDecisions,
          [focusedKey]: {
            verdict: "good_to_group" as LocalReviewVerdict,
            grouping_target: identityAuditLocalDecisions[focusedKey]?.grouping_target ?? "same_composition",
            note: identityAuditLocalDecisions[focusedKey]?.note ?? "",
            updated_at_ms: Date.now(),
          },
        };
        updateLocalReviewDecision(focusedKey, {
          verdict: "good_to_group",
          grouping_target: identityAuditLocalDecisions[focusedKey]?.grouping_target ?? "same_composition",
        });
        setIdentityAuditFocusedReviewKey(findNextUnreviewedDecisionKey(unifiedItems, focusedKey, nextDecisions));
        return;
      }

      if (event.key.toLowerCase() === "r") {
        event.preventDefault();
        const nextDecisions = {
          ...identityAuditLocalDecisions,
          [focusedKey]: {
            verdict: "not_good" as LocalReviewVerdict,
            grouping_target: null,
            note: identityAuditLocalDecisions[focusedKey]?.note ?? "",
            updated_at_ms: Date.now(),
          },
        };
        updateLocalReviewDecision(focusedKey, {
          verdict: "not_good",
          grouping_target: null,
        });
        setIdentityAuditFocusedReviewKey(findNextUnreviewedDecisionKey(unifiedItems, focusedKey, nextDecisions));
        return;
      }

      if (event.key.toLowerCase() === "s") {
        event.preventDefault();
        const nextDecisions = {
          ...identityAuditLocalDecisions,
          [focusedKey]: {
            verdict: "skipped" as LocalReviewVerdict,
            grouping_target: null,
            note: identityAuditLocalDecisions[focusedKey]?.note ?? "",
            updated_at_ms: Date.now(),
          },
        };
        updateLocalReviewDecision(focusedKey, {
          verdict: "skipped",
          grouping_target: null,
        });
        setIdentityAuditFocusedReviewKey(findNextUnreviewedDecisionKey(unifiedItems, focusedKey, nextDecisions));
      }
    }

    window.addEventListener("keydown", onKeydown);
    return () => window.removeEventListener("keydown", onKeydown);
  }, [
    appPage,
    identityAuditTab,
    identityAuditFocusedReviewKey,
    identityAuditLocalDecisions,
    identityAuditSuggestedGroups,
    identityAuditAmbiguous,
    identityAuditAmbiguousFamilyFilter,
    identityAuditAmbiguousBucketFilter,
  ]);

  useEffect(() => {
    if (appPage !== "identityAudit" || identityAuditTab !== "release") {
      return;
    }
    if (!albumDuplicateLookupLoaded && !albumDuplicateLookupLoading) {
      void loadAlbumDuplicateLookup(true);
    }
    if (!albumNameDuplicateLookupLoaded && !albumNameDuplicateLookupLoading) {
      void loadAlbumNameDuplicateLookup(true);
    }
    if (!trackDuplicateLookupLoaded && !trackDuplicateLookupLoading) {
      void loadTrackDuplicateLookup(true);
    }
  }, [
    appPage,
    identityAuditTab,
    albumDuplicateLookupLoaded,
    albumDuplicateLookupLoading,
    albumNameDuplicateLookupLoaded,
    albumNameDuplicateLookupLoading,
    trackDuplicateLookupLoaded,
    trackDuplicateLookupLoading,
  ]);

  useEffect(() => {
    if (
      usingLivePlaybackSnapshot
      && livePlaybackOnListenLabDevice
      && playerError
      && playerError.includes("Spotify player could not connect")
    ) {
      setPlayerError(null);
    }
  }, [livePlaybackOnListenLabDevice, playerError, usingLivePlaybackSnapshot]);

  useEffect(() => {
    if (!selectedPreview) {
      return;
    }
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = previousOverflow;
    };
  }, [selectedPreview]);

  useEffect(() => {
    if (!selectedPreview || selectedPreview.kind !== "track") {
      setOverlayTrackPlaybackExpanded(false);
    }
  }, [selectedPreview]);

  useEffect(() => {
    if (selectedPreview?.kind === "track" && selectedPreviewTrackIsCurrent) {
      setOverlayTrackPlaybackExpanded(true);
    }
  }, [selectedPreview?.kind, selectedPreviewTrackIsCurrent]);

  useEffect(() => {
    if (!currentTrack?.uri || !playbackPaused) {
      setPausedTimeFlashOn(true);
      return;
    }
    const timer = window.setInterval(() => {
      setPausedTimeFlashOn((current) => !current);
    }, 1400);
    return () => {
      window.clearInterval(timer);
    };
  }, [currentTrack?.uri, playbackPaused]);

  useEffect(() => {
    return () => {
      if (previewStopTimerRef.current != null) {
        window.clearTimeout(previewStopTimerRef.current);
      }
      if (previewVolumeRampTimerRef.current != null) {
        window.clearInterval(previewVolumeRampTimerRef.current);
      }
    };
  }, []);

  useEffect(() => {
    const url = new URL(window.location.href);
    if (url.pathname === "/auth/callback") {
      const status = url.searchParams.get("status");
      const flow = url.searchParams.get("flow");
      setAuthTransitioning(status === "success");
      setStatusMessage(
        status === "success"
          ? "Spotify login succeeded. Session restored."
          : "Spotify login did not complete successfully.",
      );
      if (flow === "recent_ingest") {
        setRecentIngestCallbackPending(true);
      }
      window.history.replaceState({}, "", "/");
    }
  }, []);

  useEffect(() => {
    void loadSession();
  }, []);

  useEffect(() => {
    if (!session?.authenticated || experienceMode === "local" || !hasPremiumPlayback) {
      setLivePlaybackSnapshot(null);
      setLiveAwaitingNextTrack(false);
      setLiveControlOverrideUntilMs(null);
      setLivePlaybackProbeComplete(false);
      return;
    }

    let cancelled = false;
    let pollTimer: number | null = null;
    const refresh = async () => {
      await loadCurrentPlaybackSnapshot();
      if (!cancelled) {
        setLivePlaybackProbeComplete(true);
      }
    };

    void refresh();
    pollTimer = window.setInterval(() => {
      if (!cancelled) {
        void refresh();
      }
    }, LIVE_PLAYBACK_POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      if (pollTimer != null) {
        window.clearInterval(pollTimer);
      }
    };
  }, [experienceMode, hasPremiumPlayback, session?.authenticated, session?.spotify_user_id]);

  useEffect(() => {
    if (!livePlaybackSnapshot) {
      liveProgressAnchorRef.current = null;
      liveEndRefreshRequestedRef.current = false;
      setLiveDerivedProgressMs(0);
      return;
    }
    const receivedAtMs = Date.now();
    const durationMs = Math.max(0, Number(livePlaybackSnapshot.duration_ms ?? 0));
    const progressMs = Math.max(0, Number(livePlaybackSnapshot.progress_ms ?? 0));
    const correctedBaseProgressMs = clampProgress(progressMs, durationMs);
    liveProgressAnchorRef.current = {
      baseProgressMs: correctedBaseProgressMs,
      receivedAtMs,
      durationMs,
    };
    liveEndRefreshRequestedRef.current = false;
    setLiveDerivedProgressMs(correctedBaseProgressMs);
  }, [livePlaybackSnapshot]);

  useEffect(() => {
    if (!usingLivePlaybackSnapshot || !livePlaybackSnapshot?.is_playing) {
      return;
    }
    const timer = window.setInterval(() => {
      const anchor = liveProgressAnchorRef.current;
      if (!anchor) {
        return;
      }
      const elapsedSinceReceiptMs = Math.max(0, Date.now() - anchor.receivedAtMs);
      const next = clampProgress(anchor.baseProgressMs + elapsedSinceReceiptMs, anchor.durationMs);
      setLiveDerivedProgressMs((current) => (current === next ? current : next));
      if (
        anchor.durationMs > 0
        && next >= anchor.durationMs
        && !liveEndRefreshRequestedRef.current
      ) {
        liveEndRefreshRequestedRef.current = true;
        setLiveAwaitingNextTrack(true);
        void loadCurrentPlaybackSnapshot();
        window.setTimeout(() => {
          if (session?.authenticated && experienceMode === "full") {
            void triggerRecentIngestPollForTrackEnd();
          }
        }, LIVE_TRACK_END_RECENT_POLL_DELAY_MS);
      }
    }, LIVE_PLAYBACK_PROGRESS_TICK_MS);

    return () => {
      window.clearInterval(timer);
    };
  }, [experienceMode, livePlaybackSnapshot?.is_playing, session?.authenticated, usingLivePlaybackSnapshot]);

  useEffect(() => {
    if (usingLivePlaybackSnapshot && pendingSeekMs != null) {
      setPendingSeekMs(null);
    }
  }, [pendingSeekMs, usingLivePlaybackSnapshot]);

  useEffect(() => {
    if (!recentIngestCallbackPending) {
      return;
    }
    void loadRecentIngestResult();
  }, [recentIngestCallbackPending]);

  useEffect(() => {
    window.localStorage.setItem(EXPERIENCE_MODE_STORAGE_KEY, experienceMode);
  }, [experienceMode]);

  useEffect(() => {
    if (
      experienceMode === "local"
      && !profile
      && !loadingProfile
      && !profileLoadAttempted
      && !profileLoadInFlightRef.current
    ) {
      void loadProfile();
      return;
    }
    if (
      session?.authenticated
      && !profile
      && !loadingProfile
      && !profileLoadAttempted
      && !profileLoadInFlightRef.current
    ) {
      void loadProfile();
    }
  }, [experienceMode, loadingProfile, profile, profileLoadAttempted, session]);

  useEffect(() => {
    const hasRecentDataLoaded = Boolean(
      profile
      && (
        profile.recent_tracks_available
        || profile.recent_likes_available
        || profile.recent_top_tracks_available
        || profile.recent_top_artists_available
        || profile.recent_top_albums_available
      ),
    );
    if (
      (experienceMode !== "local" && !session?.authenticated)
      || experienceMode === "local"
      || !profile
      || analysisMode !== "quick"
      || loadingProfile
      || loadingExtendedProfile
      || loadingRecentSection
      || spotifyCooldownActive
    ) {
      return;
    }
    const currentRange = profile.recent_range ?? "short_term";
    const shouldFetchRecent = currentRange !== recentRange || !hasRecentDataLoaded;
    if (!shouldFetchRecent) {
      return;
    }
    const attemptKey = `${profile.id}:${recentRange}`;
    if (quickRecentAutoAttemptRef.current === attemptKey) {
      return;
    }
    quickRecentAutoAttemptRef.current = attemptKey;
    void refreshRecentSection(recentRange);
  }, [
    analysisMode,
    experienceMode,
    loadingExtendedProfile,
    loadingProfile,
    loadingRecentSection,
    profile,
    recentRange,
    session,
    spotifyCooldownActive,
  ]);

  useEffect(() => {
    quickRecentAutoAttemptRef.current = null;
  }, [session?.spotify_user_id]);

  useEffect(() => {
    if (!recentRangeRefreshPending) {
      return;
    }
    const currentRange = profile?.recent_range ?? null;
    if (
      currentRange === recentRange
      && !loadingRecentSection
      && !loadingExtendedProfile
    ) {
      setRecentRangeRefreshPending(false);
    }
  }, [loadingExtendedProfile, loadingRecentSection, profile?.recent_range, recentRange, recentRangeRefreshPending]);

  useEffect(() => {
    if (!trackRankingRefreshPending) {
      return;
    }
    const timer = window.setTimeout(() => {
      setTrackRankingRefreshPending(false);
    }, 450);
    return () => {
      window.clearTimeout(timer);
    };
  }, [trackRankingRefreshPending]);

  useEffect(() => {
    if (reloadCooldownUntil == null) {
      return;
    }

    setReloadCountdownNow(Date.now());
    const timer = window.setInterval(() => {
      setReloadCountdownNow(Date.now());
    }, 1000);

    return () => {
      window.clearInterval(timer);
    };
  }, [reloadCooldownUntil]);

  useEffect(() => {
    if (!showRateLimitReload) {
      setRateLimitMenuOpen(false);
    }
  }, [showRateLimitReload]);

  useEffect(() => {
    function handlePointerDown(event: MouseEvent) {
      if (!profileMenuRef.current?.contains(event.target as Node)) {
        setProfileMenuOpen(false);
        setProfileSettingsOpen(false);
      }
      if (!brandMenuRef.current?.contains(event.target as Node)) {
        setBrandMenuOpen(false);
      }
      if (!experimentalMenuRef.current?.contains(event.target as Node)) {
        setExperimentalMenuOpen(false);
      }
      if (!playerMenuRef.current?.contains(event.target as Node)) {
        setPlayerMenuOpen(false);
      }
      if (!rateLimitMenuRef.current?.contains(event.target as Node)) {
        setRateLimitMenuOpen(false);
      }
    }

    document.addEventListener("mousedown", handlePointerDown);
    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
    };
  }, []);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setSelectedPreview(null);
        setRateLimitMenuOpen(false);
        setExperimentalMenuOpen(false);
      }
    }

    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    setRepresentativeTrack(null);
    setRepresentativeLoading(false);
    setRepresentativeReason(null);

    if (experienceMode === "local" || !hasPremiumPlayback || !selectedPreview?.entityId || spotifyCooldownActive) {
      return () => {
        cancelled = true;
      };
    }

    if (selectedPreview.kind !== "artist" && selectedPreview.kind !== "album") {
      return () => {
        cancelled = true;
      };
    }

    const activePreview = selectedPreview;

    async function loadRepresentativeTrack() {
      setRepresentativeLoading(true);
      try {
        const response = await fetch(
          `${apiBaseUrl}/preview/representative?kind=${encodeURIComponent(activePreview.kind)}&spotify_id=${encodeURIComponent(activePreview.entityId ?? "")}`,
          { credentials: "include" },
        );
        if (!response.ok) {
          throw new Error("Failed to load representative track.");
        }
        const data = (await response.json()) as RepresentativePreviewResponse;
        if (!cancelled) {
          setRepresentativeTrack(data.track);
          setRepresentativeReason(data.reason ?? null);
        }
      } catch {
        if (!cancelled) {
          setRepresentativeTrack(null);
          setRepresentativeReason("request_failed");
        }
      } finally {
        if (!cancelled) {
          setRepresentativeLoading(false);
        }
      }
    }

    void loadRepresentativeTrack();
    return () => {
      cancelled = true;
    };
  }, [experienceMode, hasPremiumPlayback, selectedPreview, spotifyCooldownActive]);

  useEffect(() => {
    let cancelled = false;

    if (
      experienceMode === "local"
      || !selectedPreview
      || (selectedPreview.kind !== "track" && selectedPreview.kind !== "album")
      || spotifyCooldownActive
    ) {
      loadedAlbumTracksAlbumIdRef.current = null;
      setAlbumTrackEntries([]);
      setAlbumTrackEntriesLoading(false);
      setAlbumTrackEntriesError(null);
      return () => {
        cancelled = true;
      };
    }

    const selectedTrackId = selectedPreview.kind === "track"
      ? (selectedPreview.trackId ?? selectedPreview.entityId)
      : null;
    const albumId = selectedPreview.albumId ?? selectedPreview.sourceTrack?.album_id ?? selectedPreview.entityId ?? null;
    if (!albumId) {
      loadedAlbumTracksAlbumIdRef.current = null;
      setAlbumTrackEntries([]);
      setAlbumTrackEntriesLoading(false);
      setAlbumTrackEntriesError("Album track list is unavailable for this item.");
      return () => {
        cancelled = true;
      };
    }
    const albumIdSafe = albumId;
    const albumAlreadyLoaded = loadedAlbumTracksAlbumIdRef.current === albumIdSafe;
    if (albumAlreadyLoaded) {
      setAlbumTrackEntries((current) => current.map((row) => ({
        ...row,
        isSelected: Boolean(selectedTrackId && row.id && selectedTrackId === row.id),
      })));
      setAlbumTrackEntriesLoading(false);
      setAlbumTrackEntriesError(null);
      return () => {
        cancelled = true;
      };
    }

    const normalizedTopTrackKeys = new Set(
      [
        ...(profile?.top_tracks ?? []),
        ...(profile?.recent_top_tracks ?? []),
      ].map((track) => normalizedTrackArtistKey(track.track_name, track.artist_name)),
    );
    const topTrackIds = new Set(
      [
        ...(profile?.top_tracks ?? []),
        ...(profile?.recent_top_tracks ?? []),
      ].map((track) => track.track_id).filter((value): value is string => Boolean(value)),
    );
    const knownTracksById = new Map<string, RecentTrack>();
    const latestPlayedAtByTrackId = new Map<string, string>();
    const knownTrackRows = [
      ...(profile?.recent_tracks ?? []),
      ...(profile?.top_tracks ?? []),
      ...(profile?.recent_top_tracks ?? []),
      ...(profile?.recent_likes_tracks ?? []),
    ];
    for (const knownTrack of knownTrackRows) {
      const knownTrackId = knownTrack.track_id;
      if (!knownTrackId) {
        continue;
      }
      if (!knownTracksById.has(knownTrackId)) {
        knownTracksById.set(knownTrackId, knownTrack);
      }
      for (const candidatePlayedAt of [knownTrack.spotify_played_at, knownTrack.last_played_at]) {
        if (!candidatePlayedAt) {
          continue;
        }
        const candidateMs = parseTimestampMs(candidatePlayedAt);
        if (candidateMs == null) {
          continue;
        }
        const existingPlayedAt = latestPlayedAtByTrackId.get(knownTrackId);
        const existingMs = parseTimestampMs(existingPlayedAt);
        if (existingMs == null || candidateMs > existingMs) {
          latestPlayedAtByTrackId.set(knownTrackId, candidatePlayedAt);
        }
      }
    }

    async function loadAlbumTrackEntries() {
      setAlbumTrackEntriesLoading(true);
      try {
        const token = await fetchPlaybackToken();
        const response = await fetch(
          `https://api.spotify.com/v1/albums/${encodeURIComponent(albumIdSafe)}/tracks?limit=50`,
          {
            headers: {
              Authorization: `Bearer ${token}`,
            },
          },
        );

        if (!response.ok) {
          throw new Error(`Failed to load album tracks (${response.status}).`);
        }

        const payload = (await response.json()) as {
          items?: Array<{
            id?: string | null;
            name?: string | null;
            uri?: string | null;
            duration_ms?: number | null;
            artists?: Array<{ name?: string | null }>;
          }>;
        };
        const rows = (payload.items ?? []).map((item) => {
          const id = item.id ?? null;
          const artistNames = (item.artists ?? []).map((artist) => artist.name ?? "").filter(Boolean).join(", ");
          const normalizedKey = normalizedTrackArtistKey(item.name ?? null, artistNames || null);
          const isTopTrack = Boolean((id && topTrackIds.has(id)) || normalizedTopTrackKeys.has(normalizedKey));
          const sourceTrack = id ? (knownTracksById.get(id) ?? null) : null;
          const lastPlayedAt = id ? (latestPlayedAtByTrackId.get(id) ?? null) : null;
          return {
            id,
            name: item.name ?? "Unknown track",
            uri: item.uri ?? null,
            durationMs: typeof item.duration_ms === "number" && Number.isFinite(item.duration_ms) ? Math.max(0, item.duration_ms) : null,
            artistName: artistNames || null,
            sourceTrack,
            lastPlayedAt,
            isSelected: Boolean(selectedTrackId && id && selectedTrackId === id),
            isTopTrack,
          } satisfies AlbumTrackEntry;
        });

        if (!cancelled) {
          setAlbumTrackEntries(rows);
          loadedAlbumTracksAlbumIdRef.current = albumIdSafe;
          if (rows.length === 0) {
            setAlbumTrackEntriesError("No tracks were returned for this album.");
          }
        }
      } catch (error) {
        if (!cancelled) {
          setAlbumTrackEntriesError(error instanceof Error ? error.message : "Album track list could not be loaded.");
        }
      } finally {
        if (!cancelled) {
          setAlbumTrackEntriesLoading(false);
        }
      }
    }

    void loadAlbumTrackEntries();
    return () => {
      cancelled = true;
    };
  }, [experienceMode, profile?.recent_likes_tracks, profile?.recent_top_tracks, profile?.recent_tracks, profile?.top_tracks, selectedPreview, spotifyCooldownActive]);

  function representativeReasonMessage(reason: string | null, kind: "artist" | "album") {
    switch (reason) {
      case "spotify_rejected_lookup":
        return "Spotify rejected the lookup for this item, so no representative song could be loaded.";
      case "item_not_found":
        return `Spotify did not return a matching ${kind} for this item.`;
      case "rate_limited":
        return "Spotify rate-limited this lookup. Try again in a moment.";
      case "spotify_lookup_failed":
        return "Spotify did not return usable data for this lookup.";
      case "no_representative_track":
        return kind === "artist"
          ? "Spotify did not return a top song for this artist."
          : "Spotify did not return a representative song for this album.";
      case "request_failed":
        return "The app could not load a representative song for this item.";
      default:
        return "No representative song was found for this item.";
    }
  }

  async function fetchPlaybackToken() {
    if (experienceMode === "local") {
      throw new Error("Playback is unavailable in restricted local mode.");
    }
    if (spotifyCooldownActive) {
      throw new Error(formatCooldownCopy(reloadSecondsRemaining));
    }
    const response = await fetch(`${apiBaseUrl}/auth/token`, {
      credentials: "include",
    });
    if (!response.ok) {
      throw new Error("Spotify playback authorization is not available.");
    }
    const data = (await response.json()) as AuthTokenResponse;
    return data.access_token;
  }

  async function spotifyApiRequest(path: string, init: RequestInit) {
    const token = await fetchPlaybackToken();
    const response = await fetch(`https://api.spotify.com/v1${path}`, {
      ...init,
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        ...(init.headers ?? {}),
      },
    });
    if (!response.ok && response.status !== 204) {
      throw new Error(`Spotify playback request failed (${response.status}).`);
    }
  }

  function currentTrackFromState(state: SpotifyPlayerState): PlayerTrackSummary {
    const current = state.track_window.current_track;
    return {
      name: current.name,
      artists: current.artists.map((artist) => artist.name).join(", "),
      album: current.album.name,
      image: current.album.images[0]?.url ?? null,
      uri: current.uri,
      durationMs: state.duration || current.duration_ms || 0,
    };
  }

  function spotifyTrackUrl(trackUri: string | null) {
    if (!trackUri?.startsWith("spotify:track:")) {
      return null;
    }
    const trackId = trackUri.split(":")[2];
    return trackId ? `https://open.spotify.com/track/${trackId}` : null;
  }

  function spotifyEntityUrl(kind: "track" | "artist" | "album", id: string | null | undefined) {
    return id ? `https://open.spotify.com/${kind}/${id}` : "";
  }

  function spotifyTrackIdFromUri(trackUri: string | null) {
    if (!trackUri?.startsWith("spotify:track:")) {
      return null;
    }
    const trackId = trackUri.split(":")[2];
    return trackId || null;
  }

  function trackUriWithFallback(trackUri: string | null | undefined, trackId: string | null | undefined) {
    if (trackUri && trackUri.startsWith("spotify:track:")) {
      return trackUri;
    }
    if (trackId) {
      return `spotify:track:${trackId}`;
    }
    return null;
  }

  function openPlayerTrackDetails() {
    if (!playerDisplayTrack || !usingLivePlaybackSnapshot) {
      return;
    }
    const trackId = spotifyTrackIdFromUri(playerDisplayTrack.uri) ?? livePlaybackSnapshot?.item_id ?? null;
    const trackUrl = spotifyTrackUrl(playerDisplayTrack.uri) ?? (trackId ? `https://open.spotify.com/track/${trackId}` : "");
    if (!trackUrl) {
      return;
    }
    setSelectedPreview({
      image: playerDisplayTrack.image ?? playerDisplayKnownTrack?.image_url ?? null,
      label: playerDisplayTrack.name,
      meta: playerDisplayTrack.artists || null,
      detail: (playerDisplayKnownTrack?.album_name ?? playerDisplayTrack.album) || null,
      kind: "track",
      entityId: trackId,
      trackUri: playerDisplayTrack.uri,
      url: trackUrl,
      trackId,
      albumId: playerDisplayKnownTrack?.album_id ?? null,
      artistName: playerDisplayTrack.artists || null,
      sourceTrack: playerDisplayKnownTrack ?? null,
    });
  }

  function openPlayerArtistDetails() {
    if (!playerDisplayArtistName) {
      return;
    }
    const artistUrl = playerDisplayArtist?.url ?? spotifyEntityUrl("artist", playerDisplayArtistId);
    setSelectedPreview({
      image: findArtistImageUrl(playerDisplayArtistName) ?? null,
      fallbackLabel: "A",
      label: playerDisplayArtistName,
      meta: null,
      detail: null,
      kind: "artist",
      entityId: playerDisplayArtistId,
      trackUri: null,
      url: artistUrl,
      trackId: null,
      albumId: null,
      artistName: playerDisplayArtistName,
      sourceTrack: playerDisplayKnownTrack ?? null,
    });
  }

  function openPlayerAlbumDetails() {
    if (!playerDisplayAlbumName) {
      return;
    }
    const albumUrl = playerDisplayKnownTrack?.album_url ?? spotifyEntityUrl("album", playerDisplayAlbumId);
    setSelectedPreview({
      image: playerDisplayTrack?.image ?? playerDisplayKnownTrack?.image_url ?? null,
      fallbackLabel: "L",
      label: playerDisplayAlbumName,
      meta: playerDisplayTrack?.artists ?? playerDisplayKnownTrack?.artist_name ?? null,
      detail: playerDisplayAlbumYear,
      kind: "album",
      entityId: playerDisplayAlbumId,
      trackUri: null,
      url: albumUrl,
      trackId: null,
      albumId: playerDisplayAlbumId,
      artistName: playerDisplayArtistName,
      sourceTrack: playerDisplayKnownTrack ?? null,
    });
  }

  function formatListeningSince(firstPlayedAt: string | null | undefined) {
    if (!firstPlayedAt) {
      return null;
    }
    const firstDate = new Date(firstPlayedAt);
    if (Number.isNaN(firstDate.getTime())) {
      return null;
    }
    return `Listening since ${firstDate.getUTCFullYear()}`;
  }

  function formatPlaybackClock(totalMs: number) {
    const safeSeconds = Math.max(0, Math.floor(totalMs / 1000));
    const minutes = Math.floor(safeSeconds / 60);
    const seconds = safeSeconds % 60;
    return `${minutes}:${seconds.toString().padStart(2, "0")}`;
  }

  function parseTimestampMs(value: string | null | undefined): number | null {
    if (!value) {
      return null;
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      return null;
    }
    return parsed.getTime();
  }

  function formatMonthDay(value: string | null | undefined): string | null {
    const parsedMs = parseTimestampMs(value);
    if (parsedMs == null) {
      return null;
    }
    const parsed = new Date(parsedMs);
    return `${parsed.getMonth() + 1}/${parsed.getDate()}`;
  }

  function primaryArtistName(value: string | null | undefined): string | null {
    if (!value) {
      return null;
    }
    const primary = value.split(",")[0]?.trim() ?? "";
    return primary || null;
  }

  function firstArtistFromRecentTrack(track: RecentTrack | null | undefined) {
    return track?.artists?.find((artist) => Boolean(artist?.name || artist?.id || artist?.artist_id)) ?? null;
  }

  function findArtistImageUrl(artistName: string | null | undefined): string | null {
    const target = primaryArtistName(artistName)?.toLocaleLowerCase() ?? null;
    if (!target || !profile) {
      return null;
    }
    const artistPools = [
      ...(profile.followed_artists ?? []),
      ...(profile.recent_top_artists ?? []),
    ];
    for (const artist of artistPools) {
      const candidate = artist.name?.trim().toLocaleLowerCase() ?? null;
      if (!candidate || !artist.image_url) {
        continue;
      }
      if (candidate === target) {
        return artist.image_url;
      }
    }
    return null;
  }

  function previewAlbumHeading(preview: PreviewItem): string {
    const albumName = preview.sourceTrack?.album_name ?? preview.detail ?? "Album";
    const albumYear = preview.sourceTrack?.album_release_year ?? null;
    return albumYear ? `${albumYear} - ${albumName}` : albumName;
  }

  useEffect(() => {
    if (!session?.authenticated || !profile || profile.product?.toLowerCase() !== "premium") {
      setPlayerReady(false);
      setPlayerError(profile && profile.product?.toLowerCase() !== "premium" ? "Spotify Premium is required for full playback." : null);
      setPlayerMenuOpen(false);
      spotifyPlayerRef.current?.disconnect();
      spotifyPlayerRef.current = null;
      spotifyDeviceIdRef.current = null;
      setCurrentTrack(null);
      setPlaybackPositionMs(0);
      setPlaybackDurationMs(0);
      return;
    }

    let cancelled = false;
    let connectTimeout: number | null = null;

    async function initializePlayer() {
      try {
        await fetchPlaybackToken();
      } catch (error) {
        if (!cancelled) {
          setPlayerError(error instanceof Error ? error.message : "Spotify playback authorization is not available.");
        }
        return;
      }

      const createPlayer = () => {
        if (cancelled || !window.Spotify || spotifyPlayerRef.current) {
          return;
        }

        setPlayerError(null);
        const player = new window.Spotify.Player({
          name: "ListenLab Player",
          getOAuthToken: (callback) => {
            void fetchPlaybackToken()
              .then((token) => callback(token))
              .catch(() => {
                setPlayerError("Spotify playback authorization expired. Reconnect Spotify.");
              });
          },
          volume: DEFAULT_PLAYER_VOLUME,
        });

        player.addListener("ready", ({ device_id }: { device_id: string }) => {
          spotifyDeviceIdRef.current = device_id;
          setPlayerReady(true);
          setPlayerError(null);
          if (connectTimeout != null) {
            window.clearTimeout(connectTimeout);
            connectTimeout = null;
          }
        });
        player.addListener("not_ready", () => {
          spotifyDeviceIdRef.current = null;
          setPlayerReady(false);
        });
        player.addListener("player_state_changed", (state: SpotifyPlayerState | null) => {
          if (!state) {
            return;
          }
          setLiveControlOverrideUntilMs(Date.now() + LIVE_PLAYBACK_POLL_INTERVAL_MS);
          setCurrentTrack(currentTrackFromState(state));
          setPlaybackPaused(state.paused);
          setPlaybackPositionMs(state.position ?? 0);
          setPlaybackDurationMs(state.duration ?? state.track_window.current_track.duration_ms ?? 0);
        });
        player.addListener("initialization_error", ({ message }: { message: string }) => setPlayerError(message));
        player.addListener("authentication_error", ({ message }: { message: string }) => setPlayerError(message));
        player.addListener("account_error", ({ message }: { message: string }) => setPlayerError(message));
        player.addListener("playback_error", ({ message }: { message: string }) => {
          const normalized = message.toLocaleLowerCase();
          if (normalized.includes("no list was loaded")) {
            // SDK can emit this while API fallback playback control is still valid.
            setPlayerError(null);
            void loadCurrentPlaybackSnapshot();
            return;
          }
          setPlayerError(message);
        });

        spotifyPlayerRef.current = player;
        connectTimeout = window.setTimeout(() => {
          if (!cancelled && !spotifyDeviceIdRef.current) {
            setPlayerError("Spotify player could not connect. Open Spotify on a device and try again.");
          }
        }, 12000);
        void player.connect().then((connected) => {
          if (!connected && !cancelled) {
            setPlayerError("Spotify player connection was rejected. Reconnect Spotify and try again.");
          }
        });
      };

      if (window.Spotify) {
        createPlayer();
        return;
      }

      const script = document.querySelector<HTMLScriptElement>('script[data-spotify-sdk="true"]');
      if (!script) {
        const spotifyScript = document.createElement("script");
        spotifyScript.src = "https://sdk.scdn.co/spotify-player.js";
        spotifyScript.async = true;
        spotifyScript.dataset.spotifySdk = "true";
        document.body.appendChild(spotifyScript);
      }
      window.onSpotifyWebPlaybackSDKReady = createPlayer;
    }

    void initializePlayer();
    return () => {
      cancelled = true;
      if (connectTimeout != null) {
        window.clearTimeout(connectTimeout);
      }
    };
  }, [profile, session]);

  useEffect(() => {
    if (!currentTrack || playbackPaused) {
      return;
    }

    const timer = window.setInterval(() => {
      setPlaybackPositionMs((current) => {
        const ceiling = playbackDurationMs || currentTrack.durationMs || 0;
        return ceiling > 0 ? Math.min(current + 1000, ceiling) : current + 1000;
      });
    }, 1000);

    return () => {
      window.clearInterval(timer);
    };
  }, [currentTrack, playbackDurationMs, playbackPaused]);

  useEffect(() => {
    if (pendingSeekMs == null) {
      return;
    }
    setPendingSeekMs(null);
  }, [currentTrack?.uri]);

  useEffect(() => {
    if (overlaySeekMs == null) {
      return;
    }
    setOverlaySeekMs(null);
  }, [currentTrack?.uri, selectedPreview?.trackUri]);

  useEffect(() => {
    if (
      !hasPremiumPlayback
      || currentTrack
      || !profile
      || !livePlaybackProbeComplete
      || usingLivePlaybackSnapshot
    ) {
      return;
    }

    const seedTrack = profile.recent_likes_tracks[0] ?? null;
    if (!seedTrack?.track_name) {
      return;
    }

    setCurrentTrack({
      name: seedTrack.track_name,
      artists: seedTrack.artist_name ?? "Unknown artist",
      album: seedTrack.album_name ?? "Unknown album",
      image: seedTrack.image_url ?? null,
      uri: seedTrack.uri ?? null,
      durationMs: 0,
    });
    setPlaybackPaused(true);
    setPlaybackPositionMs(0);
    setPlaybackDurationMs(0);
  }, [currentTrack, hasPremiumPlayback, livePlaybackProbeComplete, profile, usingLivePlaybackSnapshot]);

  async function playTrackUri(trackUri: string | null, positionMs = 0) {
    if (!trackUri) {
      setPlayerError("This item does not have a playable Spotify track.");
      return false;
    }
    const deviceId = spotifyDeviceIdRef.current;

    const payload = JSON.stringify({
      uris: [trackUri],
      position_ms: Math.max(0, Math.floor(positionMs)),
    });
    try {
      if (deviceId) {
        await spotifyApiRequest("/me/player", {
          method: "PUT",
          body: JSON.stringify({ device_ids: [deviceId], play: false }),
        });
        await spotifyApiRequest(`/me/player/play?device_id=${encodeURIComponent(deviceId)}`, {
          method: "PUT",
          body: payload,
        });
        setPlayerError(null);
        return true;
      }
    } catch (primaryError) {
      try {
        if (deviceId) {
          await spotifyApiRequest(`/me/player/play?device_id=${encodeURIComponent(deviceId)}`, {
            method: "PUT",
            body: payload,
          });
          setPlayerError(null);
          return true;
        }
      } catch {
        // Fall through to global play endpoint.
      }
      setPlayerError(primaryError instanceof Error ? primaryError.message : "Spotify playback could not be started.");
    }
    try {
      await spotifyApiRequest("/me/player/play", {
        method: "PUT",
        body: payload,
      });
      setPlayerError(null);
      return true;
    } catch (fallbackError) {
      setPlayerError(
        fallbackError instanceof Error
          ? fallbackError.message
          : "Spotify playback could not be started.",
      );
      return false;
    }
  }

  async function pausePlayback() {
    const player = spotifyPlayerRef.current;
    if (player) {
      try {
        await player.pause();
        setPlaybackPaused(true);
        setPlayerError(null);
        return true;
      } catch {
        // Fall back to web API pause.
      }
    }
    try {
      await spotifyApiRequest("/me/player/pause", {
        method: "PUT",
      });
      setPlaybackPaused(true);
      setPlayerError(null);
      return true;
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be paused.");
      return false;
    }
  }

  async function resumePlayback() {
    if (currentTrack?.uri && (playbackDurationMs <= 0 || currentTrack.durationMs <= 0)) {
      return playTrackUri(currentTrack.uri, Math.max(0, playbackPositionMs));
    }
    const player = spotifyPlayerRef.current;
    if (player) {
      try {
        await player.resume();
        setPlaybackPaused(false);
        setPlayerError(null);
        return true;
      } catch {
        // Fall back to web API play.
      }
    }
    const deviceId = spotifyDeviceIdRef.current;
    try {
      if (deviceId) {
        await spotifyApiRequest(`/me/player/play?device_id=${encodeURIComponent(deviceId)}`, {
          method: "PUT",
        });
      } else {
        await spotifyApiRequest("/me/player/play", {
          method: "PUT",
        });
      }
      setPlaybackPaused(false);
      setPlayerError(null);
      return true;
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be resumed.");
      return false;
    }
  }

  async function togglePlayerPlayback() {
    try {
      let updated = false;
      if (playbackPaused) {
        updated = await resumePlayback();
      } else {
        updated = await pausePlayback();
      }
      if (!updated) {
        await loadCurrentPlaybackSnapshot();
      }
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be updated.");
      await loadCurrentPlaybackSnapshot();
    }
  }

  async function takeOverPlaybackFromLiveSnapshot() {
    const deviceId = spotifyDeviceIdRef.current;

    try {
      if (deviceId) {
        await spotifyApiRequest("/me/player", {
          method: "PUT",
          body: JSON.stringify({ device_ids: [deviceId], play: true }),
        });
      }
      if (playerDisplayTrack?.uri) {
        if (deviceId) {
          await spotifyApiRequest(`/me/player/play?device_id=${encodeURIComponent(deviceId)}`, {
            method: "PUT",
            body: JSON.stringify({
              uris: [playerDisplayTrack.uri],
              position_ms: Math.max(0, Math.floor(playerDisplayPositionMs)),
            }),
          });
        } else {
          await spotifyApiRequest("/me/player/play", {
            method: "PUT",
            body: JSON.stringify({
              uris: [playerDisplayTrack.uri],
              position_ms: Math.max(0, Math.floor(playerDisplayPositionMs)),
            }),
          });
        }
      }
      setPlayerError(null);
      setLiveControlOverrideUntilMs(Date.now() + LIVE_PLAYBACK_POLL_INTERVAL_MS);
      await loadCurrentPlaybackSnapshot();
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be switched.");
    }
  }

  async function takeOverAndPausePlayback() {
    const deviceId = spotifyDeviceIdRef.current;
    try {
      if (deviceId) {
        await spotifyApiRequest("/me/player", {
          method: "PUT",
          body: JSON.stringify({ device_ids: [deviceId], play: false }),
        });
      }
      await pausePlayback();
      setPlayerError(null);
      setLiveControlOverrideUntilMs(Date.now() + LIVE_PLAYBACK_POLL_INTERVAL_MS);
      await loadCurrentPlaybackSnapshot();
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be paused on ListenLab.");
    }
  }

  function handlePlayerPrimaryButtonClick() {
    if (!liveReadOnlyMode) {
      void togglePlayerPlayback();
      return;
    }
    if (!playerDisplayPaused) {
      void takeOverAndPausePlayback();
      return;
    }
    void takeOverPlaybackFromLiveSnapshot();
  }

  async function handlePopupTrackPlayback(trackUri: string | null, options?: PopupTrackPlaybackOptions) {
    clearPreviewPlaybackState();
    if (!trackUri) {
      setPlayerError("This item does not have a playable Spotify track.");
      return false;
    }

    const player = spotifyPlayerRef.current;
    const isCurrent = currentTrack?.uri === trackUri;

    try {
      if (isCurrent && !playbackPaused && player) {
        await player.pause();
        setPlaybackPaused(true);
        return true;
      }

      if (isCurrent && playbackPaused && player) {
        await player.resume();
        setPlaybackPaused(false);
        return true;
      }

      const playbackStarted = await playTrackUri(trackUri);
      if (!playbackStarted) {
        return false;
      }
      const optimisticTrack = options?.optimisticTrack ?? null;
      if (optimisticTrack) {
        setCurrentTrack({
          ...optimisticTrack,
          uri: trackUri,
        });
      } else {
        setCurrentTrack((current) => (
          current && current.uri === trackUri
            ? current
            : {
              name: "Spotify Playback",
              artists: "Unknown artist",
              album: "Unknown album",
              image: null,
              uri: trackUri,
              durationMs: 0,
            }
        ));
      }
      setPlaybackPaused(false);
      setPlaybackPositionMs(0);
      setPlaybackDurationMs(Math.max(0, options?.optimisticTrack?.durationMs ?? 0));
      return true;
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be updated.");
      return false;
    }
  }

  function handleSelectedPreviewTrackPlay(trackUri: string | null) {
    clearPreviewPlaybackState();
    setOverlayTrackPlaybackExpanded(true);
    void handlePopupTrackPlayback(trackUri, {
      optimisticTrack: selectedPreviewTrackOptimisticSummary,
    });
  }

  async function seekPlayer(positionMs: number) {
    const safePositionMs = Math.max(0, Math.floor(positionMs));
    const player = spotifyPlayerRef.current;
    if (player) {
      try {
        await player.seek(safePositionMs);
        setPlaybackPositionMs(safePositionMs);
        setPendingSeekMs(null);
        setOverlaySeekMs(null);
        setPlayerError(null);
        return;
      } catch {
        // Fall back to Web API seek.
      }
    }
    const deviceId = spotifyDeviceIdRef.current;
    try {
      const query = deviceId
        ? `/me/player/seek?position_ms=${encodeURIComponent(String(safePositionMs))}&device_id=${encodeURIComponent(deviceId)}`
        : `/me/player/seek?position_ms=${encodeURIComponent(String(safePositionMs))}`;
      await spotifyApiRequest(query, {
        method: "PUT",
      });
      setPlaybackPositionMs(safePositionMs);
      setPendingSeekMs(null);
      setOverlaySeekMs(null);
      setPlayerError(null);
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback position could not be updated.");
    }
  }

  function isTrackPlaying(trackUri: string | null) {
    return Boolean(trackUri && currentTrack?.uri === trackUri && !playbackPaused);
  }

  function openAlbumTrackPreview(track: AlbumTrackEntry) {
    if (!selectedPreview || (selectedPreview.kind !== "track" && selectedPreview.kind !== "album")) {
      return;
    }
    const previewTrackUri = trackUriWithFallback(track.uri, track.id);
    const previewTrackUrl = spotifyTrackUrl(previewTrackUri) ?? (track.id ? `https://open.spotify.com/track/${track.id}` : selectedPreview.url);
    setSelectedPreview({
      image: track.sourceTrack?.image_url ?? selectedPreview.image ?? null,
      fallbackLabel: selectedPreview.fallbackLabel,
      label: track.name,
      meta: track.artistName ?? track.sourceTrack?.artist_name ?? selectedPreview.meta ?? null,
      detail: track.sourceTrack?.album_name ?? selectedPreview.detail,
      kind: "track",
      entityId: track.id ?? null,
      trackUri: previewTrackUri,
      url: previewTrackUrl,
      trackId: track.id ?? null,
      albumId: selectedPreview.albumId ?? selectedPreview.sourceTrack?.album_id ?? selectedPreview.entityId ?? null,
      artistName: track.artistName ?? track.sourceTrack?.artist_name ?? selectedPreview.artistName ?? null,
      sourceTrack: track.sourceTrack ?? selectedPreview.sourceTrack ?? null,
    });
  }

  function openSelectedTrackArtistPreview() {
    if (!selectedPreview || selectedPreview.kind !== "track" || !selectedPreviewPrimaryArtistName) {
      return;
    }
    const sourceTrack = selectedPreview.sourceTrack ?? null;
    const artist = firstArtistFromRecentTrack(sourceTrack);
    const artistId = artist?.artist_id ?? artist?.id ?? null;
    setSelectedPreview({
      image: findArtistImageUrl(selectedPreviewPrimaryArtistName) ?? selectedPreviewArtistImageUrl ?? selectedPreview.image ?? null,
      fallbackLabel: "A",
      label: selectedPreviewPrimaryArtistName,
      meta: null,
      detail: null,
      kind: "artist",
      entityId: artistId,
      trackUri: null,
      url: artist?.url ?? spotifyEntityUrl("artist", artistId),
      trackId: null,
      albumId: null,
      artistName: selectedPreviewPrimaryArtistName,
      sourceTrack,
    });
  }

  function openSelectedTrackAlbumPreview() {
    if (!selectedPreview || selectedPreview.kind !== "track") {
      return;
    }
    const sourceTrack = selectedPreview.sourceTrack ?? null;
    const albumId = selectedPreview.albumId ?? sourceTrack?.album_id ?? null;
    const albumName = sourceTrack?.album_name ?? selectedPreview.detail ?? "Unknown album";
    const albumYear = sourceTrack?.album_release_year ?? null;
    setSelectedPreview({
      image: selectedPreview.image ?? sourceTrack?.image_url ?? null,
      fallbackLabel: "L",
      label: albumName,
      meta: sourceTrack?.artist_name ?? selectedPreview.artistName ?? selectedPreview.meta ?? null,
      detail: albumYear,
      kind: "album",
      entityId: albumId,
      trackUri: null,
      url: sourceTrack?.album_url ?? spotifyEntityUrl("album", albumId),
      trackId: null,
      albumId,
      artistName: sourceTrack?.artist_name ?? selectedPreview.artistName ?? null,
      sourceTrack,
    });
  }

  function playerSummaryFromAlbumTrack(track: AlbumTrackEntry): PlayerTrackSummary {
    const previewTrackUri = trackUriWithFallback(track.uri, track.id);
    return {
      name: track.name,
      artists: track.artistName ?? track.sourceTrack?.artist_name ?? selectedPreview?.artistName ?? "Unknown artist",
      album: track.sourceTrack?.album_name ?? selectedPreview?.sourceTrack?.album_name ?? selectedPreview?.detail ?? "Unknown album",
      image: track.sourceTrack?.image_url ?? selectedPreview?.image ?? null,
      uri: previewTrackUri,
      durationMs: Math.max(0, track.durationMs ?? track.sourceTrack?.duration_ms ?? 0),
    };
  }

  function albumTrackPreviewKey(track: AlbumTrackEntry, rowTrackUri: string | null) {
    return track.id ?? rowTrackUri ?? normalizedTrackArtistKey(track.name, track.artistName);
  }

  function clearPreviewVolumeRamp() {
    if (previewVolumeRampTimerRef.current != null) {
      window.clearInterval(previewVolumeRampTimerRef.current);
      previewVolumeRampTimerRef.current = null;
    }
  }

  async function setPlayerVolumeSafe(nextVolume: number) {
    const player = spotifyPlayerRef.current;
    if (!player?.setVolume) {
      return;
    }
    const safeVolume = Math.max(0, Math.min(1, nextVolume));
    try {
      await player.setVolume(safeVolume);
      currentPlayerVolumeRef.current = safeVolume;
    } catch {
      // Ignore volume updates when SDK volume is unavailable.
    }
  }

  function restoreDefaultPlayerVolume() {
    clearPreviewVolumeRamp();
    void setPlayerVolumeSafe(DEFAULT_PLAYER_VOLUME);
  }

  function startPreviewVolumeRamp() {
    clearPreviewVolumeRamp();
    const steps = Math.max(1, Math.floor(PREVIEW_RAMP_DURATION_MS / PREVIEW_RAMP_STEP_MS));
    let step = 0;
    previewVolumeRampTimerRef.current = window.setInterval(() => {
      step += 1;
      const progress = Math.min(1, step / steps);
      const nextVolume = PREVIEW_RAMP_START_VOLUME + ((DEFAULT_PLAYER_VOLUME - PREVIEW_RAMP_START_VOLUME) * progress);
      void setPlayerVolumeSafe(nextVolume);
      if (progress >= 1) {
        clearPreviewVolumeRamp();
      }
    }, PREVIEW_RAMP_STEP_MS);
  }

  function setPreviewingTrackUriState(nextUri: string | null) {
    previewingTrackUriRef.current = nextUri;
    setPreviewingTrackUri(nextUri);
  }

  function clearPreviewPlaybackState() {
    if (previewStopTimerRef.current != null) {
      window.clearTimeout(previewStopTimerRef.current);
      previewStopTimerRef.current = null;
    }
    setPreviewingTrackUriState(null);
    restoreDefaultPlayerVolume();
  }

  async function stopTrackPreviewPlayback() {
    const hasActivePreview = Boolean(previewingTrackUriRef.current);
    clearPreviewPlaybackState();
    if (!hasActivePreview) {
      return;
    }
    const player = spotifyPlayerRef.current;
    let paused = false;
    if (player) {
      try {
        await player.pause();
        paused = true;
      } catch {
        paused = false;
      }
    }
    if (!paused) {
      try {
        await spotifyApiRequest("/me/player/pause", {
          method: "PUT",
        });
        paused = true;
      } catch {
        // Ignore pause errors for preview stop fallback.
      }
    }
    if (paused) {
      setPlaybackPaused(true);
    }
  }

  async function toggleAlbumTrackPreview(track: AlbumTrackEntry, rowTrackUri: string | null) {
    if (!rowTrackUri) {
      return;
    }
    const previewIsActiveForRow = Boolean(previewingTrackUriRef.current && previewingTrackUriRef.current === rowTrackUri);
    if (previewIsActiveForRow) {
      await stopTrackPreviewPlayback();
      return;
    }
    clearPreviewPlaybackState();
    const durationMs = Math.max(0, track.durationMs ?? track.sourceTrack?.duration_ms ?? 0);
    if (durationMs < 60_000) {
      setPlayerError("Preview is only available for tracks longer than 60 seconds.");
      return;
    }
    const minStartMs = 20_000;
    const maxStartMs = durationMs - 40_000;
    if (maxStartMs < minStartMs) {
      setPlayerError("Preview window could not be generated for this track.");
      return;
    }
    const randomStartMs = Math.floor(Math.random() * (maxStartMs - minStartMs + 1)) + minStartMs;
    await setPlayerVolumeSafe(PREVIEW_RAMP_START_VOLUME);
    const playbackStarted = await playTrackUri(rowTrackUri, randomStartMs);
    if (!playbackStarted) {
      restoreDefaultPlayerVolume();
      return;
    }
    const previewKey = albumTrackPreviewKey(track, rowTrackUri);
    setPreviewPlayedTrackKeys((current) => {
      const next = new Set(current);
      next.add(previewKey);
      return next;
    });
    setCurrentTrack({
      ...playerSummaryFromAlbumTrack(track),
      uri: rowTrackUri,
      durationMs,
    });
    setPlaybackPaused(false);
    setPlaybackPositionMs(randomStartMs);
    setPlaybackDurationMs(durationMs);
    setPreviewingTrackUriState(rowTrackUri);
    startPreviewVolumeRamp();
    previewStopTimerRef.current = window.setTimeout(() => {
      void stopTrackPreviewPlayback();
    }, 20_000);
  }

  async function handleAlbumTrackPlay(track: AlbumTrackEntry, trackUri: string | null) {
    if (!trackUri) {
      return;
    }
    clearPreviewPlaybackState();
    setOverlayTrackPlaybackExpanded(true);
    const playbackStarted = await handlePopupTrackPlayback(trackUri, {
      optimisticTrack: playerSummaryFromAlbumTrack(track),
    });
    if (playbackStarted) {
      openAlbumTrackPreview(track);
    }
  }

  function recentRangeLabel(range: RecentRange) {
    return RECENT_RANGE_OPTIONS.find((option) => option.value === range)?.label ?? "Recent";
  }

  function handleExperienceModeChange(nextMode: ExperienceMode) {
    if (nextMode === experienceMode) {
      return;
    }
    setTestFullSuccessPinned(false);
    setTestProbeModeVisual(null);
    setExperienceMode(nextMode);
    setProfileLoadAttempted(false);
    setProfile(null);
    setOpenSections(INITIAL_OPEN_SECTIONS);
    setSectionPages(INITIAL_SECTION_PAGES);
    setRateLimitMenuOpen(false);
    setReloadCooldownUntil(null);
    setReloadCooldownDurationMs(60_000);
    if (nextMode === "local") {
      setStatusMessage("Loading local-only experience...");
      setAuthTransitioning(true);
      return;
    }
    if (session?.authenticated) {
      setStatusMessage("Loading full Spotify experience...");
      setAuthTransitioning(true);
    } else {
      setStatusMessage("Not connected yet. Use Spotify login to start the auth flow.");
      setAuthTransitioning(false);
    }
  }

  function renderExperienceModeToggle() {
    const visualMode: ExperienceVisualMode =
      testProbeModeVisual ?? (testFullSuccessPinned ? "test" : experienceMode);
    const localActive = visualMode === "local" || (testingFullExperience && experienceMode === "local");
    const testActive = visualMode === "test" || testingFullExperience;
    return (
      <div className="experience-toggle" role="group" aria-label="Experience mode">
        <span
          className={`experience-toggle-slider experience-toggle-slider-${visualMode}${testingFullExperience ? " experience-toggle-slider-testing" : ""}${testFullSuccessPinned ? " experience-toggle-slider-flash" : ""}`}
          aria-hidden="true"
        />
        <button
          className={`experience-chip${localActive ? " experience-chip-active" : ""}`}
          onClick={() => handleExperienceModeChange("local")}
          type="button"
        >
          Local
        </button>
        <button
          className={`experience-chip experience-chip-test${testActive ? " experience-chip-active" : ""}${testingFullExperience ? " experience-chip-test-running" : ""}${testFullSuccessPinned ? " experience-chip-test-success" : ""}`}
          disabled={testingFullExperience}
          onClick={() => void testFullExperienceAvailability()}
          type="button"
        >
          {testingFullExperience ? "Testing..." : "Test"}
        </button>
        <button
          className={`experience-chip${visualMode === "full" ? " experience-chip-active" : ""}`}
          onClick={() => handleExperienceModeChange("full")}
          type="button"
        >
          Full
        </button>
      </div>
    );
  }

  async function testFullExperienceAvailability() {
    if (testingFullExperience) {
      return;
    }
    setTestFullSuccessPinned(false);
    setTestProbeModeVisual("test");
    setTestingFullExperience(true);
    try {
      const response = await fetch(`${apiBaseUrl}/auth/full-availability`, {
        credentials: "include",
      });
      if (!response.ok) {
        throw new Error("Could not test full experience availability.");
      }
      const data = (await response.json()) as FullAvailabilityResponse;
      if (data.available) {
        setTestFullSuccessPinned(true);
        setTestProbeModeVisual("test");
        setStatusMessage("Full experience is available. You can switch modes anytime.");
        setStatusHistory((current) => [...current, "Full experience test: available."]);
        return;
      }
      const retryAfter = Math.max(0, Number(data.retry_after_seconds ?? 0));
      if (data.blocked && retryAfter > 0) {
        setTestFullSuccessPinned(false);
        setTestProbeModeVisual("local");
        setReloadCooldownDurationMs(retryAfter * 1000);
        setReloadCooldownUntil(Date.now() + retryAfter * 1000);
        const message = formatCooldownCopy(retryAfter);
        setStatusMessage(message);
        setStatusHistory((current) => [...current, `Full experience test: blocked (${message})`]);
        return;
      }
      setTestFullSuccessPinned(false);
      setTestProbeModeVisual("local");
      const detail = data.detail || "Full experience is not currently available.";
      setStatusMessage(detail);
      setStatusHistory((current) => [...current, `Full experience test: ${detail}`]);
    } catch (error) {
      setTestFullSuccessPinned(false);
      setTestProbeModeVisual("local");
      const message = error instanceof Error ? error.message : "Could not test full experience availability.";
      setStatusMessage(message);
      setStatusHistory((current) => [...current, `Full experience test error: ${message}`]);
    } finally {
      setTestingFullExperience(false);
    }
  }

  function renderRecentRangeHeader() {
    const showRangeRefreshSpinner = recentRangeRefreshPending || loadingRecentSection;
    return (
      <div className="section-column-header">
        <h3>Recents</h3>
        <div className="recent-range-toggle" role="group" aria-label="Recent range">
          {showRangeRefreshSpinner ? (
            <span className="recent-range-vinyl-spinner" aria-hidden="true">
              <span className="recent-range-vinyl-center" />
            </span>
          ) : null}
          {RECENT_RANGE_OPTIONS.map((option) => (
            <button
              key={option.value}
              className={`recent-range-chip${recentRange === option.value ? " recent-range-chip-active" : ""}`}
              onClick={() => {
                if (option.value === recentRange || loadingRecentSection) {
                  return;
                }
                setRecentRangeRefreshPending(true);
                void refreshRecentSection(option.value);
              }}
              type="button"
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>
    );
  }

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
      setProfileLoadAttempted(false);
      setOpenSections(INITIAL_OPEN_SECTIONS);
      setSectionPages(INITIAL_SECTION_PAGES);

      if (data.authenticated) {
        setStatusMessage("");
        setStatusHistory([]);
        setAuthTransitioning(true);
      } else {
        setProfile(null);
        setProfileMenuOpen(false);
        setProfileSettingsOpen(false);
        setBrandMenuOpen(false);
        setPlayerMenuOpen(false);
        setLivePlaybackSnapshot(null);
        setLivePlaybackProbeComplete(false);
        setLiveControlOverrideUntilMs(null);
        setCurrentTrack(null);
        setPlayerReady(false);
        setStatusMessage("Not connected yet. Use Spotify login to start the auth flow.");
        setStatusHistory([]);
        setAuthTransitioning(false);
      }
    } catch (error) {
      setStatusMessage(formatUiErrorMessage(error, "Failed to load session."));
    }
  }

  async function loadCurrentPlaybackSnapshot() {
    try {
      const response = await fetch(`${apiBaseUrl}/auth/current-playback`, {
        credentials: "include",
      });
      if (!response.ok) {
        setLivePlaybackSnapshot(null);
        setLiveAwaitingNextTrack(false);
        return;
      }
      const data = (await response.json()) as CurrentPlaybackResponse;
      if (data.status === "ok" && data.has_playback && data.snapshot) {
        setLivePlaybackSnapshot(data.snapshot);
        setLiveAwaitingNextTrack(false);
        return;
      }
      setLivePlaybackSnapshot(null);
      setLiveAwaitingNextTrack(false);
    } catch {
      setLivePlaybackSnapshot(null);
      setLiveAwaitingNextTrack(false);
    }
  }

  async function triggerRecentIngestPollForTrackEnd() {
    try {
      await fetch(`${apiBaseUrl}/auth/recent-ingest/poll-now`, {
        method: "POST",
        credentials: "include",
      });
    } catch {
      // Keep track-end UX resilient even if this background ingest trigger fails.
    }
  }

  function startLogin() {
    window.location.href = `${apiBaseUrl}/auth/login`;
  }

  function startRecentIngestLogin() {
    window.location.href = `${apiBaseUrl}/auth/login?mode=recent_ingest`;
  }

  async function loadRecentIngestResult() {
    try {
      const response = await fetch(`${apiBaseUrl}/auth/recent-ingest/result`, {
        credentials: "include",
      });
      if (!response.ok) {
        throw new Error(`Recent ingest result failed (${response.status})`);
      }
      const data = (await response.json()) as RecentIngestResultResponse;
      if (!data.has_result) {
        setRecentIngestResult(null);
        setStatusMessage("Spotify auth succeeded, but no ingest result was returned.");
        return;
      }
      setRecentIngestResult(data);
      if (data.auth_succeeded && data.ingest_succeeded) {
        const earliest = data.earliest_api_played_at ?? "n/a";
        const latest = data.latest_api_played_at ?? "n/a";
        setStatusMessage(
          `Recent ingest succeeded: ${data.row_count ?? 0} rows (${earliest} to ${latest}).`,
        );
      } else {
        setStatusMessage(`Recent ingest failed: ${data.error ?? "unknown error"}`);
      }
    } catch (error) {
      setStatusMessage(
        error instanceof Error ? error.message : "Failed to load recent ingest result.",
      );
    } finally {
      setRecentIngestCallbackPending(false);
    }
  }

  async function runRecentBeforeProbe() {
    try {
      const response = await fetch(`${apiBaseUrl}/auth/recent-ingest/probe-before?days=90&limit=50`, {
        credentials: "include",
      });
      const data = (await response.json()) as RecentBeforeProbeResponse;
      if (!response.ok) {
        throw new Error(data.detail || `Probe failed (${response.status})`);
      }
      setRecentBeforeProbeResult(data);
      setStatusMessage(
        `Before-90d probe: ${data.returned_items ?? 0} rows (${data.earliest_played_at ?? "n/a"} to ${data.latest_played_at ?? "n/a"}).`,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : "Before-90d probe failed.";
      setRecentBeforeProbeResult({ ok: false, detail: message });
      setStatusMessage(`Before-90d probe failed: ${message}`);
    }
  }

  async function runRecentBackfillProbe() {
    try {
      const response = await fetch(`${apiBaseUrl}/auth/recent-ingest/probe-backfill?limit=50&max_pages=10`, {
        credentials: "include",
      });
      const data = (await response.json()) as RecentBackfillProbeResponse;
      if (!response.ok) {
        throw new Error(data.detail || `Backfill probe failed (${response.status})`);
      }
      setRecentBackfillProbeResult(data);
      setStatusMessage(
        `Backfill probe: ${data.total_items ?? 0} items across ${data.pages_fetched ?? 0} pages (${data.earliest_played_at ?? "n/a"} to ${data.latest_played_at ?? "n/a"}).`,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : "Backfill probe failed.";
      setRecentBackfillProbeResult({ ok: false, detail: message });
      setStatusMessage(`Backfill probe failed: ${message}`);
    }
  }

  async function reconnectSpotify() {
    const response = await fetch(`${apiBaseUrl}/cache/rebuild`, {
      method: "POST",
      credentials: "include",
    });
    if (!response.ok) {
      let detail = "Failed to refresh cache before reconnecting Spotify.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // Keep fallback detail.
      }
      throw new Error(detail);
    }
    startLogin();
  }

  function handleAuthAction() {
    if (experienceMode === "local") {
      setProfile(null);
      setProfileLoadAttempted(false);
      setAuthTransitioning(true);
      setStatusMessage("Loading local-only experience...");
      return;
    }
    if (session?.authenticated) {
      void reconnectSpotify();
      return;
    }
    startLogin();
  }

  function toggleSection(section: SectionKey, anchorId?: string) {
    setExperimentalMenuOpen(false);
    const isCurrentlyOpen = openSections[section];
    if (isCurrentlyOpen && anchorId) {
      const element = document.getElementById(anchorId);
      if (element) {
        element.scrollIntoView({ behavior: "smooth", block: "start" });
      }
      window.setTimeout(() => {
        setOpenSections((current) => ({
          ...current,
          [section]: false,
        }));
      }, 180);
      return;
    }

    if (
      section === "playlists"
      && !isCurrentlyOpen
      && experienceMode === "full"
      && profile
      && !profile.extended_loaded
      && !loadingExtendedProfile
    ) {
      void loadExtendedProfile(recentRange, analysisMode);
    }

    setOpenSections((current) => ({
      ...current,
      [section]: !current[section],
    }));
  }

  function openListeningLogPage() {
    setListeningLogTracks([]);
    setListeningLogHasMore(false);
    setListeningLogOffset(0);
    setListeningLogLoaded(false);
    setListeningLogLoading(false);
    setListeningLogError("");
    setListeningLogLastLoadedAt(null);
    setOpenDebugSessions({});
    setOpenDebugTracks({});
    setExperimentalMenuOpen(false);
    setAppPage("recentDebug");
  }

  function openFormulaLabPage() {
    setMergedTracks([]);
    setMergedTracksLoaded(false);
    setMergedTracksLoading(false);
    setMergedTracksError("");
    setMergedTracksExcludedUnknownCount(0);
    setMergedTracksLastLoadedAt(null);
    setExperimentalMenuOpen(false);
    setAppPage("formulaLab");
  }

  function openIdentityAuditPage() {
    setIdentityAudit(null);
    setIdentityAuditLoaded(false);
    setIdentityAuditLoading(false);
    setIdentityAuditError("");
    setIdentityAuditLastLoadedAt(null);
    setIdentityAuditSuggestedGroups(null);
    setIdentityAuditSuggestedLoaded(false);
    setIdentityAuditSuggestedLoading(false);
    setIdentityAuditSuggestedError("");
    setIdentityAuditSuggestedLastLoadedAt(null);
    setIdentityAuditAmbiguous(null);
    setIdentityAuditAmbiguousLoaded(false);
    setIdentityAuditAmbiguousLoading(false);
    setIdentityAuditAmbiguousError("");
    setIdentityAuditAmbiguousLastLoadedAt(null);
    setAlbumDuplicateLookupResult(null);
    setAlbumDuplicateLookupLoading(false);
    setAlbumDuplicateLookupLoaded(false);
    setAlbumDuplicateLookupError("");
    setAlbumDuplicateLookupLastLoadedAt(null);
    setAlbumNameDuplicateLookupResult(null);
    setAlbumNameDuplicateLookupLoading(false);
    setAlbumNameDuplicateLookupLoaded(false);
    setAlbumNameDuplicateLookupError("");
    setAlbumNameDuplicateLookupLastLoadedAt(null);
    setTrackDuplicateLookupResult(null);
    setTrackDuplicateLookupLoading(false);
    setTrackDuplicateLookupLoaded(false);
    setTrackDuplicateLookupError("");
    setTrackDuplicateLookupLastLoadedAt(null);
    setIdentityAuditTab("overview");
    setExperimentalMenuOpen(false);
    setAppPage("identityAudit");
  }

  function openCatalogBackfillPage() {
    setCatalogBackfillCoverage(null);
    setCatalogBackfillCoverageLoading(false);
    setCatalogBackfillCoverageLoaded(false);
    setCatalogBackfillCoverageError("");
    setCatalogBackfillCoverageLastLoadedAt(null);
    setCatalogBackfillRuns(null);
    setCatalogBackfillRunsLoading(false);
    setCatalogBackfillRunsLoaded(false);
    setCatalogBackfillRunsError("");
    setCatalogBackfillRunsLastLoadedAt(null);
    setCatalogBackfillQueue(null);
    setCatalogBackfillQueueLoading(false);
    setCatalogBackfillQueueLoaded(false);
    setCatalogBackfillQueueError("");
    setCatalogBackfillQueueLastLoadedAt(null);
    setCatalogBackfillQueueStatusFilter("all");
    setCatalogBackfillTab("recentRuns");
    setCatalogBackfillQueueRepairLoading(false);
    setCatalogBackfillQueueRepairMessage("");
    setCatalogBackfillRunLoading(false);
    setCatalogBackfillRunError("");
    setCatalogBackfillLatestResult(null);
    setCatalogBackfillLimit(25);
    setCatalogBackfillOffset(0);
    setCatalogBackfillMarket("US");
    setCatalogBackfillIncludeAlbums(true);
    setCatalogBackfillForceRefresh(false);
    setCatalogBackfillRequestDelaySeconds(0.5);
    setCatalogBackfillMaxRuntimeSeconds(60);
    setCatalogBackfillMaxRequests(150);
    setCatalogBackfillMaxErrors(10);
    setCatalogBackfillMaxAlbumTracksPagesPerAlbum(10);
    setCatalogBackfillMax429(3);
    setExperimentalMenuOpen(false);
    setAppPage("catalogBackfill");
  }

  function openSearchLookupPage() {
    setSearchLookupEntityType("albums");
    setSearchLookupQueueStatus("all");
    setSearchLookupSort("default");
    setAlbumCatalogLookupQ("");
    setAlbumCatalogLookupStatus("all");
    setAlbumCatalogLookupResult(null);
    setAlbumCatalogLookupLoading(false);
    setAlbumCatalogLookupLoaded(false);
    setAlbumCatalogLookupError("");
    setAlbumCatalogLookupLastLoadedAt(null);
    setTrackCatalogLookupStatus("all");
    setTrackCatalogLookupResult(null);
    setTrackCatalogLookupLoading(false);
    setTrackCatalogLookupLoaded(false);
    setTrackCatalogLookupError("");
    setTrackCatalogLookupLastLoadedAt(null);
    setAlbumDuplicateLookupResult(null);
    setAlbumDuplicateLookupLoading(false);
    setAlbumDuplicateLookupLoaded(false);
    setAlbumDuplicateLookupError("");
    setAlbumDuplicateLookupLastLoadedAt(null);
    setAlbumCatalogLookupEnqueueLoading(false);
    setAlbumCatalogLookupEnqueueError("");
    setAlbumCatalogLookupEnqueueResult(null);
    setExperimentalMenuOpen(false);
    setAppPage("searchLookup");
  }

  function albumLookupRowIsNotBackfilled(item: AlbumCatalogLookupItem): boolean {
    return item.catalog_fetched_at === null && item.catalog_last_status === null && item.catalog_last_error === null;
  }

  function albumLookupRowHasCatalogError(item: AlbumCatalogLookupItem): boolean {
    return item.catalog_last_status === "error" || Boolean(item.catalog_last_error);
  }

  function albumLookupStatusLabel(item: AlbumCatalogLookupItem): "Complete" | "Missing metadata" | "Tracklist incomplete" | "Error" {
    if (albumLookupRowHasCatalogError(item)) {
      return "Error";
    }
    if (albumLookupRowIsNotBackfilled(item)) {
      return "Missing metadata";
    }
    if (!item.tracklist_complete) {
      return "Tracklist incomplete";
    }
    return "Complete";
  }

  function albumLookupRowIsIncompleteForEnqueue(item: AlbumCatalogLookupItem): boolean {
    if (!item.spotify_album_id) {
      return false;
    }
    return albumLookupRowIsNotBackfilled(item) || !item.tracklist_complete || albumLookupRowHasCatalogError(item);
  }

  function rowIsPendingQueue(queueStatus: string | null | undefined): boolean {
    return String(queueStatus ?? "").trim().toLowerCase() === "pending";
  }

  function queueStatusLabel(queueStatus: string | null | undefined): "Not queued" | "Pending" | "Done" | "Error" {
    const normalized = String(queueStatus ?? "").trim().toLowerCase();
    if (normalized === "pending") {
      return "Pending";
    }
    if (normalized === "done") {
      return "Done";
    }
    if (normalized === "error") {
      return "Error";
    }
    return "Not queued";
  }

  function albumLookupRowCanBulkPrioritize(item: AlbumCatalogLookupItem): boolean {
    return albumLookupRowIsIncompleteForEnqueue(item) && !rowIsPendingQueue(item.queue_status);
  }

  function trackLookupRowHasCatalogError(item: TrackCatalogLookupItem): boolean {
    return item.catalog_last_status === "error" || Boolean(item.catalog_last_error);
  }

  function trackLookupRowIsNotBackfilled(item: TrackCatalogLookupItem): boolean {
    return item.catalog_fetched_at === null && item.catalog_last_status === null && item.catalog_last_error === null;
  }

  function trackLookupStatusLabel(item: TrackCatalogLookupItem): "Complete" | "Missing duration" | "Missing metadata" | "Error" {
    if (trackLookupRowHasCatalogError(item)) {
      return "Error";
    }
    if (trackLookupRowIsNotBackfilled(item)) {
      return "Missing metadata";
    }
    if (item.duration_ms === null) {
      return "Missing duration";
    }
    return "Complete";
  }

  function trackLookupRowIsIncompleteForEnqueue(item: TrackCatalogLookupItem): boolean {
    if (!item.spotify_track_id) {
      return false;
    }
    const statusLabel = trackLookupStatusLabel(item);
    return statusLabel === "Missing metadata" || statusLabel === "Missing duration" || statusLabel === "Error";
  }

  function trackLookupRowCanBulkPrioritize(item: TrackCatalogLookupItem): boolean {
    return trackLookupRowIsIncompleteForEnqueue(item) && !rowIsPendingQueue(item.queue_status);
  }

  function openAlbumLookupPreview(item: AlbumCatalogLookupItem) {
    const spotifyAlbumId = item.spotify_album_id ?? null;
    setSelectedPreview({
      image: null,
      fallbackLabel: "L",
      label: item.release_album_name,
      meta: item.artist_name ?? null,
      detail: item.release_date ?? null,
      kind: "album",
      entityId: spotifyAlbumId,
      trackUri: null,
      url: spotifyAlbumId ? `https://open.spotify.com/album/${spotifyAlbumId}` : "",
      trackId: null,
      albumId: spotifyAlbumId,
      artistName: item.artist_name ?? null,
      sourceTrack: null,
    });
  }

  function normalizedTrackArtistKey(trackName: string | null | undefined, artistName: string | null | undefined) {
    return `${(trackName ?? "").trim().toLowerCase()}::${(artistName ?? "").trim().toLowerCase()}`;
  }

  function formulaTrackKey(track: RecentTrack) {
    return (
      track.track_id
      ?? track.uri
      ?? normalizedTrackArtistKey(track.track_name, track.artist_name)
    );
  }

  function formatFormulaRankDelta(track: RecentTrack): string | null {
    const delta = Number(track.formula_rank_delta ?? 0);
    if (!Number.isFinite(delta) || delta === 0) {
      return null;
    }
    return delta > 0 ? `+${delta}` : String(delta);
  }

  function renderTrackRankingToggle() {
    const showTrackRankingSpinner = trackRankingRefreshPending;
    const selectTrackRankingMode = (nextMode: TrackRankingMode) => {
      if (nextMode === trackRankingMode) {
        return;
      }
      setTrackRankingRefreshPending(true);
      setSectionPages((current) => ({
        ...current,
        tracksAllTime: 0,
      }));
      setTrackRankingMode(nextMode);
    };

    return (
      <div className="track-ranking-toggle" role="group" aria-label="Top track ranking mode">
        {showTrackRankingSpinner ? (
          <span className="recent-range-vinyl-spinner" aria-hidden="true">
            <span className="recent-range-vinyl-center" />
          </span>
        ) : null}
        <button
          className={`track-ranking-chip${trackRankingMode === "plays" ? " track-ranking-chip-active" : ""}`}
          onClick={() => selectTrackRankingMode("plays")}
          type="button"
        >
          Plays
        </button>
        <button
          className={`track-ranking-chip${trackRankingMode === "mix" ? " track-ranking-chip-active" : ""}`}
          onClick={() => selectTrackRankingMode("mix")}
          type="button"
        >
          Mix
        </button>
        <button
          className={`track-ranking-chip${trackRankingMode === "longevity" ? " track-ranking-chip-active" : ""}`}
          onClick={() => selectTrackRankingMode("longevity")}
          type="button"
        >
          Longevity
        </button>
      </div>
    );
  }

  function formatTrackSourceBadge(track: RecentTrack): string | null {
    if (track.source_label === "both") {
      return "Both";
    }
    if (track.source_label === "api") {
      return "API";
    }
    if (track.source_label === "recent") {
      return "Recent";
    }
    if (track.source_label === "history") {
      return "History";
    }
    if (track.has_recent_source && track.has_history_source) {
      return "Both";
    }
    if (track.has_recent_source) {
      return "Recent";
    }
    if (track.has_history_source) {
      return "History";
    }
    return null;
  }

  function renderMergedTrackSourceFilterToggle() {
    return (
      <div className="track-ranking-toggle" role="group" aria-label="Merged track source filter">
        {MERGED_TRACK_SOURCE_FILTER_OPTIONS.map((option) => (
          <button
            className={`track-ranking-chip${mergedTrackSourceFilter === option.value ? " track-ranking-chip-active" : ""}`}
            key={option.value}
            onClick={() => setMergedTrackSourceFilter(option.value)}
            type="button"
          >
            {option.label}
          </button>
        ))}
      </div>
    );
  }

  function renderRecentDebugSourceFilterToggle() {
    return (
      <div className="track-ranking-toggle" role="group" aria-label="Recent debug source filter">
        {RECENT_DEBUG_SOURCE_FILTER_OPTIONS.map((option) => (
          <button
            className={`track-ranking-chip${recentDebugSourceFilter === option.value ? " track-ranking-chip-active" : ""}`}
            key={option.value}
            onClick={() => {
              if (recentDebugSourceFilter === option.value) {
                return;
              }
              setRecentDebugSourceFilter(option.value);
              setListeningLogTracks([]);
              setListeningLogHasMore(false);
              setListeningLogOffset(0);
              setListeningLogLoaded(false);
              setListeningLogLastLoadedAt(null);
              setListeningLogError("");
              setOpenDebugSessions({});
              setOpenDebugTracks({});
            }}
            type="button"
          >
            {option.label}
          </button>
        ))}
      </div>
    );
  }

  function renderRankMovementFilterToggle() {
    return (
      <div className="track-ranking-toggle" role="group" aria-label="Formula rank movement filter">
        {RANK_MOVEMENT_FILTER_OPTIONS.map((option) => (
          <button
            className={`track-ranking-chip${rankMovementFilter === option.value ? " track-ranking-chip-active" : ""}`}
            key={option.value}
            onClick={() => setRankMovementFilter(option.value)}
            type="button"
          >
            {option.label}
          </button>
        ))}
      </div>
    );
  }

  function openAndScrollToSection(section: SectionKey, anchorId: string) {
    setExperimentalMenuOpen(false);
    setAppPage("dashboard");
    setOpenSections((current) => ({
      ...current,
      artists: false,
      tracks: false,
      albums: false,
      playlists: false,
      recent: false,
      [section]: true,
    }));

    window.setTimeout(() => {
      const element = document.getElementById(anchorId);
      if (element) {
        element.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    }, 0);
  }

  function moveSectionPage(section: SectionKey, direction: -1 | 1, itemCount: number, pageSize: number = PAGE_SIZE) {
    const maxPage = Math.max(0, Math.ceil(itemCount / pageSize) - 1);
    setSectionPages((current) => ({
      ...current,
      [section]: Math.min(maxPage, Math.max(0, current[section] + direction)),
    }));
  }

  function visibleItems<T>(section: SectionKey, items: T[]) {
    const start = sectionPages[section] * PAGE_SIZE;
    return items.slice(start, start + PAGE_SIZE);
  }

  function visibleItemsWithPageSize<T>(section: SectionKey, items: T[], pageSize: number) {
    const start = sectionPages[section] * pageSize;
    return items.slice(start, start + pageSize);
  }

  function previewImages(items: Array<{ image_url?: string | null }>) {
    return items
      .map((item) => item.image_url)
      .filter((image): image is string => Boolean(image))
      .slice(0, 5);
  }

  function previewItems(
    items: Array<{
      image_url?: string | null;
      name?: string | null;
      track_name?: string | null;
      track_id?: string | null;
      artist_id?: string | null;
      artist_name?: string | null;
      album_name?: string | null;
      album_id?: string | null;
      release_year?: string | null;
      uri?: string | null;
      playlist_name?: string | null;
      playlist_id?: string | null;
      description?: string | null;
      track_count?: number | null;
      url?: string | null;
      album_url?: string | null;
      playlist_url?: string | null;
    }>,
  ) {
    return items
      .map((item) => {
        const label = item.name ?? item.track_name ?? item.playlist_name ?? "";
        const isTrack = Boolean(item.track_name);
        const isPlaylist = Boolean(item.playlist_name);
        const kind: PreviewItem["kind"] = isTrack
          ? "track"
          : isPlaylist
            ? "playlist"
            : item.release_year
              ? "album"
              : item.artist_name
                ? "album"
                : "artist";
        const meta = isTrack
          ? item.artist_name ?? null
          : isPlaylist
            ? item.track_count != null
              ? `${item.track_count} tracks`
              : "Playlist"
            : item.artist_name ?? null;
        const detail = isTrack
          ? item.album_name ?? null
          : item.release_year
            ? item.release_year
            : isPlaylist
              ? item.description?.trim() || null
              : null;

        return {
          image: item.image_url ?? null,
          label,
          meta,
          detail,
          kind,
          entityId: isTrack
            ? item.track_id ?? null
            : isPlaylist
              ? item.playlist_id ?? null
              : item.album_id ?? item.artist_id ?? null,
          trackUri: item.uri ?? null,
          url: item.url ?? item.album_url ?? item.playlist_url ?? "",
          trackId: isTrack ? item.track_id ?? null : null,
          albumId: isTrack ? item.album_id ?? null : null,
          artistName: isTrack ? item.artist_name ?? null : null,
          sourceTrack: null,
        } satisfies PreviewItem;
      })
      .filter((item) => Boolean(item.label && item.url))
      .slice(0, 5);
  }

  function renderPreviewCard(item: PreviewItem, key: string) {
    return (
      <button
        className="preview-card"
        key={key}
        onClick={() => setSelectedPreview(item)}
        type="button"
      >
        {item.image ? (
          <img alt={item.label} className="preview-thumb" src={item.image} />
        ) : (
          <div className="preview-thumb preview-thumb-fallback" aria-hidden="true">
            {item.fallbackLabel ?? item.label.slice(0, 1).toUpperCase()}
          </div>
        )}
        <span className="preview-overlay">
          <span className="preview-label">{item.label}</span>
          {item.meta ? <span className="preview-meta">{item.meta}</span> : null}
          {item.detail ? <span className="preview-detail">{item.detail}</span> : null}
        </span>
      </button>
    );
  }

  function emptySlots<T>(items: T[]) {
    return Math.max(0, PAGE_SIZE - items.length);
  }

  function formatAlbumSummary(album: TopAlbum) {
    const names = album.represented_track_names.filter(Boolean);
    if (names.length === 0) {
      return `${album.track_representation_count} tracks represented`;
    }
    if (names.length <= 2) {
      return names.join(" | ");
    }
    return `${names.slice(0, 2).join(", ")} +${names.length - 2} more`;
  }

  function formatAlbumBreadth(album: TopAlbum) {
    const count = Math.max(0, album.track_representation_count ?? 0);
    return count === 1 ? "1 track" : `${count} tracks`;
  }

  function formatHistoryDebugLine(item: {
    debug?: {
      source?: string;
      score?: number;
      total_ms?: number;
      play_count?: number;
      distinct_tracks?: number;
    };
  }) {
    if (item.debug?.source !== "history") {
      return null;
    }

    const hours = item.debug.total_ms != null ? `${(item.debug.total_ms / 3_600_000).toFixed(1)}h` : null;
    const plays = item.debug.play_count != null ? `${item.debug.play_count} plays` : null;
    const tracks = item.debug.distinct_tracks != null ? `${item.debug.distinct_tracks} tracks` : null;
    return [hours, plays, tracks].filter(Boolean).join(" | ");
  }

  function formatTrackLongevity(track: RecentTrack) {
    const spanDays = Math.max(0, Math.floor(track.listening_span_days ?? 0));
    if (spanDays <= 0) {
      return null;
    }
    if (spanDays >= 365) {
      const years = spanDays / 365.25;
      return years >= 10 ? `${Math.round(years)}y` : `${years.toFixed(1)}y`;
    }
    if (spanDays >= 30) {
      const months = Math.floor(spanDays / 30);
      return `${months}mo`;
    }
    return `${spanDays}d`;
  }

  function formatTrackLongevityMetric(track: RecentTrack) {
    const longevity = formatTrackLongevity(track);
    const consistency = Math.max(0, Math.min(1, Number(track.consistency_ratio ?? 0)));
    if (!longevity) {
      return null;
    }
    return `${longevity} | ${Math.round(consistency * 100)}%`;
  }

  function getTrackLongevityScore(track: RecentTrack) {
    return Number(track.longevity_score ?? 0);
  }

  function formatTrackLongevitySortMetric(track: RecentTrack) {
    const score = getTrackLongevityScore(track);
    if (score <= 0) {
      return null;
    }
    const longevity = formatTrackLongevity(track) ?? "0d";
    return `${score.toFixed(2)} | ${longevity}`;
  }

  function formatTrackRankingMetric(track: RecentTrack) {
    if (trackRankingMode === "plays") {
      const plays = getTrackPlayCount(track);
      return plays > 0 ? `${plays} plays` : null;
    }
    if (trackRankingMode === "longevity") {
      return formatTrackLongevitySortMetric(track);
    }
    const plays = getTrackPlayCount(track);
    const longevityMetric = formatTrackLongevitySortMetric(track);
    if (plays > 0 && longevityMetric) {
      return `${plays} plays | ${longevityMetric}`;
    }
    if (plays > 0) {
      return `${plays} plays`;
    }
    return longevityMetric;
  }

  function baselineFormulaLabel() {
    if (trackRankingMode === "plays") {
      return "raw plays";
    }
    if (trackRankingMode === "longevity") {
      return "linear longevity";
    }
    return "linear blend";
  }

  function candidateFormulaLabel() {
    if (trackRankingMode === "plays") {
      return "recent-boosted plays";
    }
    if (trackRankingMode === "longevity") {
      return "recent-boosted longevity";
    }
    return "recent-boosted blend";
  }

  function formatTrackLongevityWithConsistency(track: RecentTrack) {
    const longevity = formatTrackLongevity(track);
    const consistency = Math.max(0, Math.min(1, Number(track.consistency_ratio ?? 0)));
    if (!longevity) {
      return null;
    }
    const consistencyPercent = Math.round(consistency * 100);
    return `${longevity} · ${consistencyPercent}%`;
  }
  function getTrackPlayCount(track: RecentTrack): number {
    return Number(track.play_count ?? 0);
  }

  function getNormalizedPlays(track: RecentTrack, maxPlays: number): number {
    return getTrackPlayCount(track) / Math.max(1, maxPlays);
  }

  function getNormalizedLongevity(track: RecentTrack, maxLongevity: number): number {
    return getTrackLongevityScore(track) / Math.max(1, maxLongevity);
  }

  function getOldPlaysScore(track: RecentTrack, maxPlays: number): number {
    return getTrackPlayCount(track) / Math.max(1, maxPlays);
  }

  function getNewPlaysScore(track: RecentTrack, maxPlays: number): number {
    const normalized = getTrackPlayCount(track) / Math.max(1, maxPlays);
    const recentBoost = Number(track.recent_play_count ?? 0) > 0 ? 0.35 : 0;
    return Math.sqrt(normalized) * 0.65 + recentBoost;
  }

  function getOldLongevityScore(track: RecentTrack, maxLongevity: number): number {
    return getTrackLongevityScore(track) / Math.max(1, maxLongevity);
  }

  function getNewLongevityScore(track: RecentTrack, maxLongevity: number): number {
    const normalized = getTrackLongevityScore(track) / Math.max(1, maxLongevity);
    const recentBoost = Number(track.recent_play_count ?? 0) > 0 ? 0.25 : 0;
    return Math.sqrt(normalized) * 0.75 + recentBoost;
  }

  function getOldMixScore(
    track: RecentTrack,
    maxPlays: number,
    maxLongevity: number,
  ): number {
    return getOldPlaysScore(track, maxPlays) * 0.58
      + getOldLongevityScore(track, maxLongevity) * 0.42;
  }

  function getNewMixScore(
    track: RecentTrack,
    maxPlays: number,
    maxLongevity: number,
  ): number {
    return getNewPlaysScore(track, maxPlays) * 0.70
      + getNewLongevityScore(track, maxLongevity) * 0.30;
  }
  function sortedTracksForView(section: SectionKey, tracks: RecentTrack[]) {
    const isCurrentSection = section === "tracksAllTime" || section === "tracksAllTimeCurrent";
    const isNewSection = section === "tracksAllTimeNew";

    if (!isCurrentSection && !isNewSection) {
      return tracks;
    }

    const withMetrics = tracks.some(
      (track) =>
        getTrackPlayCount(track) > 0 ||
        Number(track.listening_span_days ?? 0) > 0 ||
        getTrackLongevityScore(track) > 0,
    );

    if (!withMetrics) {
      return tracks;
    }

    const ranked = [...tracks];
    const maxPlays = Math.max(1, ...ranked.map((track) => getTrackPlayCount(track)));
    const maxLongevity = Math.max(1, ...ranked.map((track) => getTrackLongevityScore(track)));

    ranked.sort((a, b) => {
      let aScore = 0;
      let bScore = 0;

      if (isNewSection) {
        if (trackRankingMode === "plays") {
          aScore = getNewPlaysScore(a, maxPlays);
          bScore = getNewPlaysScore(b, maxPlays);
        } else if (trackRankingMode === "longevity") {
          aScore = getNewLongevityScore(a, maxLongevity);
          bScore = getNewLongevityScore(b, maxLongevity);
        } else {
          aScore = getNewMixScore(a, maxPlays, maxLongevity);
          bScore = getNewMixScore(b, maxPlays, maxLongevity);
        }
      } else {
        if (trackRankingMode === "plays") {
          aScore = getOldPlaysScore(a, maxPlays);
          bScore = getOldPlaysScore(b, maxPlays);
        } else if (trackRankingMode === "longevity") {
          aScore = getOldLongevityScore(a, maxLongevity);
          bScore = getOldLongevityScore(b, maxLongevity);
        } else {
          aScore = getOldMixScore(a, maxPlays, maxLongevity);
          bScore = getOldMixScore(b, maxPlays, maxLongevity);
        }
      }

      const scoreDelta = bScore - aScore;
      if (Math.abs(scoreDelta) > 1e-6) {
        return scoreDelta;
      }

      const playsDelta = getTrackPlayCount(b) - getTrackPlayCount(a);
      if (playsDelta !== 0) {
        return playsDelta;
      }

      const longevityDelta = getTrackLongevityScore(b) - getTrackLongevityScore(a);
      if (Math.abs(longevityDelta) > 1e-6) {
        return longevityDelta;
      }

      const spanDelta = Number(b.listening_span_days ?? 0) - Number(a.listening_span_days ?? 0);
      if (spanDelta !== 0) {
        return spanDelta;
      }

      return 0;
    });

    return ranked;
  }

  function albumGroupingKey(track: RecentTrack) {
    if (track.album_id) {
      return track.album_id;
    }
    const albumName = (track.album_name ?? "").trim().toLowerCase();
    const artistName = (track.artist_name ?? "").trim().toLowerCase();
    if (!albumName) {
      return null;
    }
    return `${albumName}::${artistName}`;
  }

  function capTracksPerAlbum(items: RecentTrack[], maxPerAlbum: number) {
    const albumTotals = new Map<string, number>();
    items.forEach((track) => {
      const key = albumGroupingKey(track);
      if (!key) {
        return;
      }
      albumTotals.set(key, (albumTotals.get(key) ?? 0) + 1);
    });

    const albumSeen = new Map<string, number>();
    const rows: Array<{ track: RecentTrack; hiddenCount: number }> = [];

    items.forEach((track) => {
      const key = albumGroupingKey(track);
      if (!key) {
        rows.push({ track, hiddenCount: 0 });
        return;
      }

      const seen = albumSeen.get(key) ?? 0;
      if (seen >= maxPerAlbum) {
        return;
      }

      const totalInAlbum = albumTotals.get(key) ?? 0;
      const hiddenCount = seen + 1 === maxPerAlbum ? Math.max(0, totalInAlbum - maxPerAlbum) : 0;
      rows.push({ track, hiddenCount });
      albumSeen.set(key, seen + 1);
    });

    return rows;
  }

  function collapseRecentPreviewTracks(items: RecentTrack[]) {
    const collapsed: RecentTrack[] = [];
    let previousAlbumKey: string | null = null;
    for (const track of items) {
      const albumKey = track.album_id
        ?? (track.album_name && track.artist_name ? `${track.album_name}::${track.artist_name}` : null);
      if (albumKey && previousAlbumKey && albumKey === previousAlbumKey) {
        continue;
      }
      collapsed.push(track);
      previousAlbumKey = albumKey;
    }
    return collapsed;
  }

  function collapseTrackPreviewAlbums(items: RecentTrack[]) {
    const collapsed: RecentTrack[] = [];
    const seenAlbumKeys = new Set<string>();
    for (const track of items) {
      const albumKey = albumGroupingKey(track);
      if (albumKey) {
        if (seenAlbumKeys.has(albumKey)) {
          continue;
        }
        seenAlbumKeys.add(albumKey);
      }
      collapsed.push(track);
    }
    return collapsed;
  }

  function formatLoadingStatusDetailed(phase: string | null, elapsedSeconds: number) {
    const elapsed = `${elapsedSeconds.toFixed(1)}s`;
    return phase ? `Working on ${phase}... (${elapsed})` : `Loading your Spotify data... (${elapsed})`;
  }

  function formatLoadingStatusUi(phase: string | null) {
    if (!phase) {
      return "Loading your Spotify data...";
    }
    const normalized = phase.toLowerCase();
    if (normalized.includes("followed artist count")) {
      return "Checking artist count...";
    }
    if (normalized.includes("top artists all time")) {
      return "Loading top artists...";
    }
    if (normalized.includes("top artists recent")) {
      return "Loading recent artists...";
    }
    if (normalized.includes("top tracks all time")) {
      return "Loading top tracks...";
    }
    if (normalized.includes("top tracks recent")) {
      return "Loading recent tracks...";
    }
    if (normalized.includes("recent listening")) {
      return "Loading recent activity...";
    }
    if (normalized.includes("liked tracks")) {
      return "Loading liked tracks...";
    }
    if (normalized.includes("profile")) {
      return "Loading profile...";
    }
    if (normalized.includes("playlist")) {
      return "Loading playlists...";
    }
    if (normalized.includes("albums")) {
      return "Loading albums...";
    }
    if (normalized.includes("analyzing")) {
      return "Analyzing listening history...";
    }
    if (normalized.includes("formula") || normalized.includes("metrics")) {
      return "Analyzing listening patterns...";
    }
    if (normalized.includes("precomputed local insights")) {
      return "Loading local analysis cache...";
    }
    if (normalized.includes("local analysis cache write")) {
      return "Loading local cache updates...";
    }
    if (normalized.includes("loading per-user cached recent sections")) {
      return "Loading cached recent sections...";
    }
    if (normalized.includes("history")) {
      return "Analyzing listening history...";
    }
    if (normalized.includes("finishing") || normalized.includes("complete")) {
      return "Finalizing dashboard...";
    }
    return "Loading your Spotify data...";
  }

  function formatPlaylistSummary(playlist: TopPlaylist, mode: "recent" | "allTime") {
    const matches =
      mode === "recent"
        ? [
            playlist.match_counts.short_term_top > 0
              ? `${playlist.match_counts.short_term_top} top tracks`
              : null,
            playlist.match_counts.recently_played > 0
              ? `${playlist.match_counts.recently_played} recently played`
              : null,
            playlist.match_counts.liked > 0
              ? `${playlist.match_counts.liked} liked tracks`
              : null,
          ]
        : [
            playlist.match_counts.long_term_top > 0
              ? `${playlist.match_counts.long_term_top} top tracks`
              : null,
            playlist.match_counts.liked > 0
              ? `${playlist.match_counts.liked} liked tracks`
              : null,
            playlist.track_count != null ? `${playlist.track_count} total tracks` : null,
          ];

    return matches.filter(Boolean).join(" | ") || `${playlist.track_count ?? 0} tracks`;
  }

  function mergeExtendedProfile(previous: ProfileResponse | null, next: ProfileResponse) {
    if (!previous) {
      return next;
    }

    return {
      ...next,
      recent_tracks: previous.recent_tracks.length > 0 ? previous.recent_tracks : next.recent_tracks,
      recent_tracks_available: previous.recent_tracks_available || next.recent_tracks_available,
      recent_likes_tracks: previous.recent_likes_tracks.length > 0 ? previous.recent_likes_tracks : next.recent_likes_tracks,
      recent_likes_available: previous.recent_likes_available || next.recent_likes_available,
    };
  }

  function recentUnavailableCopy(defaultCopy: string) {
    if (experienceMode === "local") {
      return "Recent sections in restricted local mode come from local history data only.";
    }
    const hasRecentDataLoaded = Boolean(
      profile
      && (
        profile.recent_tracks_available
        || profile.recent_likes_available
        || profile.recent_top_tracks_available
        || profile.recent_top_artists_available
        || profile.recent_top_albums_available
      ),
    );
    if (analysisMode === "quick" && !hasRecentDataLoaded) {
      return "Recent data is off in quick load. Open settings and choose Load full analysis.";
    }
    return defaultCopy;
  }

  function quickUnavailableCopy(defaultCopy: string) {
    if (experienceMode === "local") {
      return "This section is unavailable in restricted local mode until we have a saved Spotify snapshot for it.";
    }
    if (analysisMode === "quick") {
      return "This section is limited in quick load. Open settings and choose Load full analysis.";
    }
    return defaultCopy;
  }

  function hasStaleSection(section: string) {
    return Boolean(profile?.stale_sections?.includes(section));
  }

  function formatRelativeSyncTime(timestampSeconds?: number | null) {
    if (!timestampSeconds || !Number.isFinite(timestampSeconds)) {
      return null;
    }
    const deltaSeconds = Math.max(0, Math.round(Date.now() / 1000 - timestampSeconds));
    if (deltaSeconds < 60) {
      return "just now";
    }
    if (deltaSeconds < 3600) {
      const minutes = Math.floor(deltaSeconds / 60);
      return `${minutes} minute${minutes === 1 ? "" : "s"} ago`;
    }
    if (deltaSeconds < 86_400) {
      const hours = Math.floor(deltaSeconds / 3600);
      return `${hours} hour${hours === 1 ? "" : "s"} ago`;
    }
    const days = Math.floor(deltaSeconds / 86_400);
    return `${days} day${days === 1 ? "" : "s"} ago`;
  }

  function renderSectionTitle(title: string, staleSection?: string) {
    const showStale = Boolean(staleSection && experienceMode === "local" && hasStaleSection(staleSection));
    const syncedLabel = formatRelativeSyncTime(profile?.local_last_synced_at);
    return (
      <span className="section-title-row">
        <span>{title}</span>
        {showStale ? (
          <span
            className="section-stale-badge"
            title={`Cached from Spotify${syncedLabel ? `, last synced ${syncedLabel}` : ""}. Open settings to reload.`}
          >
            Cached
          </span>
        ) : null}
      </span>
    );
  }

  function parseCooldownSeconds(detail: string) {
    const secondMatch = detail.match(/about (\d+) seconds/i);
    if (secondMatch) {
      const parsedSeconds = Number(secondMatch[1]);
      if (Number.isFinite(parsedSeconds)) {
        return Math.min(600, Math.max(1, Math.round(parsedSeconds)));
      }
    }
    const minuteMatch = detail.match(/about (\d+) minutes/i);
    if (minuteMatch) {
      const parsedMinutes = Number(minuteMatch[1]);
      if (Number.isFinite(parsedMinutes)) {
        return Math.min(600, Math.max(60, Math.round(parsedMinutes * 60)));
      }
    }
    return null;
  }

  function formatCooldownCopy(totalSeconds: number) {
    if (totalSeconds >= 120) {
      const minutes = Math.ceil(totalSeconds / 60);
      return `Spotify is rate-limiting requests right now. Try again in about ${minutes} minutes.`;
    }
    return "Spotify is rate-limiting requests right now. Try again in about a minute.";
  }

  function formatCooldownTimerLabel(totalSeconds: number) {
    const safeSeconds = Math.max(0, totalSeconds);
    const minutes = Math.floor(safeSeconds / 60);
    const seconds = safeSeconds % 60;
    return `${minutes}:${seconds.toString().padStart(2, "0")}`;
  }

  function formatUiErrorMessage(error: unknown, fallback: string) {
    const raw = error instanceof Error ? error.message : "";
    const lower = raw.toLowerCase();
    if (
      error instanceof TypeError
      || lower.includes("failed to fetch")
      || lower.includes("networkerror")
      || lower.includes("network request failed")
    ) {
      return "Can’t reach ListenLab API. Start backend on 127.0.0.1:8000 and refresh.";
    }
    if (lower.includes("cors")) {
      return "Backend blocked by CORS. Use localhost/127.0.0.1 defaults.";
    }
    return raw || fallback;
  }

  async function loadProfile() {
    if (experienceMode === "full" && spotifyCooldownActive) {
      setStatusMessage(formatCooldownCopy(reloadSecondsRemaining));
      return;
    }
    if (profileLoadInFlightRef.current) {
      return;
    }
    profileLoadInFlightRef.current = true;
    setLoadingProfile(true);
    setProfileLoadAttempted(true);
    setStatusMessage(experienceMode === "local" ? "Loading local history..." : "Loading your Spotify data...");
    setStatusHistory(["Initial load started."]);
    let pollingActive = true;
    let progressTimer: number | null = null;
    const startedAt = performance.now();

    const updateProgress = async () => {
      const fallbackElapsed = (performance.now() - startedAt) / 1000;
      try {
        const response = await fetch(`${apiBaseUrl}/me/progress`, {
          credentials: "include",
        });
        if (!response.ok) {
          if (pollingActive) {
            setStatusMessage(formatLoadingStatusUi(null));
          }
          return;
        }
        const data = (await response.json()) as ProfileProgressResponse;
        if (!pollingActive) {
          return;
        }
        setStatusMessage(formatLoadingStatusUi(data.active ? data.phase : null));
        if (data.events?.length) {
          setStatusHistory(
            [
              "Initial load started.",
              ...data.events.map((event) => `initial ${event.at_seconds.toFixed(1)}s: ${event.phase}`),
            ],
          );
        } else {
          setStatusHistory(["Initial load started.", `initial ${formatLoadingStatusDetailed(null, fallbackElapsed)}`]);
        }
      } catch {
        if (pollingActive) {
          setStatusMessage(formatLoadingStatusUi(null));
          setStatusHistory(["Initial load started.", `initial ${formatLoadingStatusDetailed(null, fallbackElapsed)}`]);
        }
      }
    };

    await updateProgress();
    progressTimer = window.setInterval(() => {
      void updateProgress();
    }, 500);
    try {
      const endpoint = experienceMode === "local" ? "/me/local" : "/me";
      const response = await fetch(
        `${apiBaseUrl}${endpoint}?recent_range=${encodeURIComponent(recentRange)}&analysis_mode=${encodeURIComponent(analysisMode)}`,
        {
        method: "GET",
        credentials: "include",
        },
      );

      if (!response.ok) {
        let detail = "Failed to load Spotify profile.";
        try {
          const payload = (await response.json()) as { detail?: string };
          if (payload.detail) {
            detail = payload.detail;
          }
        } catch {
          // ignore invalid error payloads
        }
        if (response.status === 403) {
          detail = "Spotify permission missing. Log out and log back in to grant the latest scopes.";
        }
        if (response.status === 429) {
          const cooldownSeconds = parseCooldownSeconds(detail) ?? 60;
          setReloadCooldownDurationMs(cooldownSeconds * 1000);
          setReloadCooldownUntil(Date.now() + cooldownSeconds * 1000);
          detail = formatCooldownCopy(cooldownSeconds);
        }
        throw new Error(detail);
      }

      const data = (await response.json()) as ProfileResponse;
      let hydratedProfile = data;
      if (experienceMode === "local") {
        hydratedProfile = {
          ...hydratedProfile,
          username: session?.spotify_user_id ?? hydratedProfile.username,
          display_name: session?.display_name ?? hydratedProfile.display_name,
          email: session?.email ?? hydratedProfile.email,
        };
      }
      if ((data.analysis_mode ?? analysisMode) === "quick" && experienceMode !== "local") {
        setStatusMessage("Loading recent activity...");
        try {
          const recentData = await fetchRecentSections(data.recent_range ?? recentRange);
          hydratedProfile = {
            ...hydratedProfile,
            recent_range: recentData.recent_range,
            recent_window_days: recentData.recent_window_days,
            recent_top_artists: recentData.recent_top_artists,
            recent_top_artists_available: recentData.recent_top_artists_available,
            recent_top_tracks: recentData.recent_top_tracks,
            recent_top_tracks_available: recentData.recent_top_tracks_available,
            recent_top_albums: recentData.recent_top_albums,
            recent_top_albums_available: recentData.recent_top_albums_available,
            recent_tracks: recentData.recent_tracks,
            recent_tracks_available: recentData.recent_tracks_available,
            recent_likes_tracks: recentData.recent_likes_tracks,
            recent_likes_available: recentData.recent_likes_available,
          };
        } catch (recentError) {
          const message = recentError instanceof Error ? recentError.message : "Recent activity could not be preloaded.";
          setStatusHistory((current) => [...current, `Recent preload warning: ${message}`]);
        }
      }

      setProfile(hydratedProfile);
      setAnalysisMode(hydratedProfile.analysis_mode ?? analysisMode);
      setAuthTransitioning(false);
      setSectionPages(INITIAL_SECTION_PAGES);
      setStatusMessage("");
      setStatusHistory((current) =>
        current.length > 0 ? [...current, "Initial load complete."] : ["Initial load started.", "Initial load complete."],
      );
      if (hydratedProfile.recent_range) {
        setRecentRange(hydratedProfile.recent_range);
      }
    } catch (error) {
      const message = formatUiErrorMessage(error, "Failed to load Spotify profile.");
      setStatusMessage(message);
      setAuthTransitioning(false);
      setStatusHistory((current) => (current.length > 0 ? [...current, `Error: ${message}`] : [message]));
    } finally {
      profileLoadInFlightRef.current = false;
      pollingActive = false;
      if (progressTimer != null) {
        window.clearInterval(progressTimer);
      }
      setLoadingProfile(false);
    }
  }

  async function loadExtendedProfile(targetRange: RecentRange = recentRange, targetAnalysisMode: AnalysisMode = "full") {
    if (experienceMode === "full" && spotifyCooldownActive) {
      setStatusMessage(formatCooldownCopy(reloadSecondsRemaining));
      setStatusHistory((current) => [...current, "Spotify cooldown active. Full analysis paused."]);
      return;
    }
    if (extendedLoadInFlightRef.current) {
      return;
    }
    extendedLoadInFlightRef.current = true;
    setLoadingExtendedProfile(true);
    setStatusMessage("Starting full analysis...");
    setStatusHistory((current) => [...current, "Background expansion started."]);
    let pollingActive = true;
    let progressTimer: number | null = null;
    const startedAt = performance.now();

    const updateProgress = async () => {
      const fallbackElapsed = (performance.now() - startedAt) / 1000;
      try {
        const response = await fetch(`${apiBaseUrl}/me/progress`, {
          credentials: "include",
        });
        if (!response.ok) {
          return;
        }
        const data = (await response.json()) as ProfileProgressResponse;
        if (!pollingActive) {
          return;
        }
        if (data.events?.length) {
          setStatusMessage(
            formatLoadingStatusUi(data.active ? data.phase : "Analyzing your music..."),
          );
          setStatusHistory((current) => {
            const prefix = current.filter((entry) => !entry.startsWith("background "));
            const extensionEvents = (data.events ?? []).map(
              (event) => `background ${event.at_seconds.toFixed(1)}s: ${event.phase}`,
            );
            return [...prefix, ...extensionEvents];
          });
        } else {
          setStatusMessage(formatLoadingStatusUi(null));
          setStatusHistory((current) => {
            const prefix = current.filter((entry) => !entry.startsWith("background "));
            return [...prefix, `background ${formatLoadingStatusDetailed(null, fallbackElapsed)}`];
          });
        }
      } catch {
        // ignore background progress failures
      }
    };

    progressTimer = window.setInterval(() => {
      void updateProgress();
    }, 500);
    try {
      const endpoint = experienceMode === "local" ? "/me/local" : "/me";
      const response = await fetch(
        `${apiBaseUrl}${endpoint}?mode=extended&recent_range=${encodeURIComponent(targetRange)}&analysis_mode=${encodeURIComponent(targetAnalysisMode)}`,
        {
          credentials: "include",
        },
      );

      if (!response.ok) {
        let detail = "Failed to load Spotify profile.";
        try {
          const payload = (await response.json()) as { detail?: string };
          if (payload.detail) {
            detail = payload.detail;
          }
        } catch {
          // ignore invalid error payloads
        }
        if (response.status === 403) {
          detail = "Spotify permission missing. Log out and log back in to grant the latest scopes.";
        }
        if (response.status === 429) {
          const cooldownSeconds = parseCooldownSeconds(detail) ?? 60;
          setReloadCooldownDurationMs(cooldownSeconds * 1000);
          setReloadCooldownUntil(Date.now() + cooldownSeconds * 1000);
          detail = formatCooldownCopy(cooldownSeconds);
        }
        throw new Error(detail);
      }

      const data = (await response.json()) as ProfileResponse;
      setProfile((current) => mergeExtendedProfile(current, data));
      setAnalysisMode(data.analysis_mode ?? targetAnalysisMode);
      if (data.recent_range) {
        setRecentRange(data.recent_range);
      }
      setStatusMessage("");
      setStatusHistory((current) => {
        const filtered = current.filter((entry) => !entry.startsWith("background "));
        return [...filtered, "Background expansion complete."];
      });
    } catch (error) {
      const message = formatUiErrorMessage(error, "Failed to load extended Spotify profile.");
      setStatusMessage(message);
      setStatusHistory((current) => {
        const filtered = current.filter((entry) => !entry.startsWith("background "));
        return [...filtered, `Background expansion error: ${message}`];
      });
    } finally {
      extendedLoadInFlightRef.current = false;
      pollingActive = false;
      if (progressTimer != null) {
        window.clearInterval(progressTimer);
      }
      setLoadingExtendedProfile(false);
    }
  }

  async function logout() {
    await fetch(`${apiBaseUrl}/auth/logout`, {
      method: "POST",
      credentials: "include",
    });
    setSession({
      authenticated: false,
      display_name: null,
      spotify_user_id: null,
      email: null,
    });
    setProfile(null);
    setProfileLoadAttempted(false);
    setOpenSections(INITIAL_OPEN_SECTIONS);
    setSectionPages(INITIAL_SECTION_PAGES);
    setStatusMessage("Signed out.");
    setStatusHistory([]);
    setAuthTransitioning(false);
    setProfileMenuOpen(false);
    setProfileSettingsOpen(false);
    setBrandMenuOpen(false);
    setPlayerMenuOpen(false);
    setCurrentTrack(null);
    setPlayerReady(false);
    setListeningLogTracks([]);
    setListeningLogHasMore(false);
    setListeningLogOffset(0);
    setListeningLogLoading(false);
    setListeningLogLoaded(false);
    setListeningLogError("");
    setListeningLogLastLoadedAt(null);
    setRecentDebugSourceFilter("all");
    setMergedTracks([]);
    setMergedTracksLoaded(false);
    setMergedTracksLoading(false);
    setMergedTracksError("");
    setMergedTracksExcludedUnknownCount(0);
    setMergedTracksLastLoadedAt(null);
    setMergedTrackSourceFilter("all");
    setIdentityAudit(null);
    setIdentityAuditLoading(false);
    setIdentityAuditLoaded(false);
    setIdentityAuditError("");
    setIdentityAuditLastLoadedAt(null);
  }

  function renderPaging(section: SectionKey, itemCount: number) {
    return renderPagingWithPageSize(section, itemCount, PAGE_SIZE);
  }

  function renderPagingWithPageSize(section: SectionKey, itemCount: number, pageSize: number) {
    if (itemCount <= pageSize) {
      return null;
    }

    return (
      <div className="section-nav">
        <button
          className="secondary-button"
          disabled={sectionPages[section] === 0}
          onClick={() => moveSectionPage(section, -1, itemCount, pageSize)}
          type="button"
        >
          {"<"}
        </button>
        <span>
          {sectionPages[section] + 1} / {Math.ceil(itemCount / pageSize)}
        </span>
        <button
          className="secondary-button"
          disabled={(sectionPages[section] + 1) * pageSize >= itemCount}
          onClick={() => moveSectionPage(section, 1, itemCount, pageSize)}
          type="button"
        >
          {">"}
        </button>
      </div>
    );
  }

  function renderDashboardListCard(props: DashboardListCardProps, key: string) {
    const {
      href,
      entityId,
      imageUrl,
      imageAlt,
      fallbackLabel,
    primaryText,
    secondaryText,
    tertiaryText,
    metricText,
    primaryBadgeText,
    secondaryBadgeText,
    trackUri,
    previewTrack,
    primaryClamp = "single-line-ellipsis",
  } = props;
    const previewKind: PreviewItem["kind"] = fallbackLabel === "T"
      ? "track"
      : fallbackLabel === "A"
        ? "artist"
        : fallbackLabel === "P"
          ? "playlist"
          : "album";
    const secondaryValue = secondaryText && secondaryText.trim().length > 0 ? secondaryText : "\u00A0";
    const tertiaryValue = tertiaryText && tertiaryText.trim().length > 0 ? tertiaryText : "\u00A0";
    const secondaryPlaceholder = !(secondaryText && secondaryText.trim().length > 0);
    const tertiaryPlaceholder = !(tertiaryText && tertiaryText.trim().length > 0);

    return (
      <button
        className="list-row list-link dashboard-card-row"
        key={key}
        onClick={() =>
          setSelectedPreview({
            image: imageUrl ?? null,
            fallbackLabel,
            label: primaryText,
            meta: secondaryText ?? null,
            detail: tertiaryText ?? null,
            kind: previewKind,
            entityId: entityId ?? null,
            trackUri: previewKind === "track"
              ? trackUriWithFallback(trackUri, previewTrack?.track_id ?? entityId ?? null)
              : trackUri ?? null,
            url: href ?? "",
            trackId: previewTrack?.track_id ?? null,
            albumId: previewTrack?.album_id ?? null,
            artistName: previewTrack?.artist_name ?? null,
            sourceTrack: previewTrack ?? null,
          })}
        type="button"
      >
        <div className="dashboard-card-layout">
          <div className="list-primary">
            {imageUrl ? (
              <img alt={imageAlt} className="list-art" src={imageUrl} />
            ) : (
              <div className="list-art list-art-fallback" aria-hidden="true">
                {fallbackLabel}
              </div>
            )}
            <div className="card-copy">
              <div className="card-primary-line">
                <strong className={`card-primary ${primaryClamp}`}>{primaryText}</strong>
                {primaryBadgeText ? <span className="card-inline-badge">{primaryBadgeText}</span> : null}
                {secondaryBadgeText ? <span className="card-inline-badge">{secondaryBadgeText}</span> : null}
              </div>
              <p
                aria-hidden={secondaryPlaceholder}
                className={`card-secondary single-line-ellipsis${secondaryPlaceholder ? " card-line-placeholder" : ""}`}
              >
                {secondaryValue}
              </p>
              <p
                aria-hidden={tertiaryPlaceholder}
                className={`card-tertiary single-line-ellipsis${tertiaryPlaceholder ? " card-line-placeholder" : ""}`}
              >
                {tertiaryValue}
              </p>
            </div>
          </div>
          {metricText ? <div className="card-metric">{metricText}</div> : null}
        </div>
      </button>
    );
  }

  function renderArtistColumn(
    section: SectionKey,
    items: FollowedArtist[],
    available: boolean,
    emptyCopy: string,
    unavailableCopy: string,
    unavailableAction?: ReactNode,
  ) {
    if (!available) {
      return (
        <div className="section-unavailable">
          <p className="empty-copy">{unavailableCopy}</p>
          {unavailableAction ? <div className="section-unavailable-action">{unavailableAction}</div> : null}
        </div>
      );
    }
    if (items.length === 0) {
      return <p className="empty-copy">{emptyCopy}</p>;
    }

    const pageItems = visibleItems(section, items);
    return (
      <>
        <div className="item-list">
          {pageItems.map((artist, index) => (
            renderDashboardListCard(
              {
                href: artist.url,
                entityId: artist.artist_id,
                imageUrl: artist.image_url,
                imageAlt: `${artist.name ?? "Artist"} portrait`,
                fallbackLabel: "A",
                primaryText: artist.name ?? "Unknown artist",
                secondaryText:
                  artist.genres.length > 0
                    ? artist.genres.join(", ")
                    : artist.popularity != null
                      ? `Popularity ${artist.popularity}/100`
                      : "Spotify artist",
                tertiaryText: formatHistoryDebugLine(artist),
              },
              artist.artist_id ?? `${artist.name}-${index}`,
            )
          ))}
          {Array.from({ length: emptySlots(pageItems) }).map((_, index) => (
            <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
          ))}
        </div>
        {renderPaging(section, items.length)}
      </>
    );
  }

  function renderTrackColumn(
    section: SectionKey,
    items: RecentTrack[],
    available: boolean,
    emptyCopy: string,
    unavailableCopy: string,
    unavailableAction?: ReactNode,
    paged: boolean = true,
    presorted: boolean = false,
  ) {
    if (!available) {
      return (
        <div className="section-unavailable">
          <p className="empty-copy">{unavailableCopy}</p>
          {unavailableAction ? <div className="section-unavailable-action">{unavailableAction}</div> : null}
        </div>
      );
    }
    if (items.length === 0) {
      return <p className="empty-copy">{emptyCopy}</p>;
    }

    const rankedItems = presorted ? items : sortedTracksForView(section, items);
    const isAllTimeTrackSection =
      section === "tracksAllTime" ||
      section === "tracksAllTimeCurrent" ||
      section === "tracksAllTimeNew";
    const showSourceBadge = section !== "tracksAllTimeCurrent" && section !== "tracksAllTimeNew";
    const formulaRankDeltaText = (track: RecentTrack) =>
      section === "tracksAllTimeCurrent" || section === "tracksAllTimeNew"
        ? formatFormulaRankDelta(track)
        : null;

    const cappedRows = isAllTimeTrackSection || section === "tracksRecent"
      ? capTracksPerAlbum(rankedItems, 1)
      : rankedItems.map((track) => ({ track, hiddenCount: 0 }));
    const pageRows = paged ? visibleItems(section, cappedRows) : cappedRows;
    return (
      <>
        <div className="item-list">
          {pageRows.map((row, index) =>
            renderDashboardListCard(
              {
                href: row.track.url,
                entityId: row.track.track_id,
                imageUrl: row.track.image_url,
                imageAlt: `${row.track.album_name ?? row.track.track_name ?? "Album"} cover`,
                fallbackLabel: "T",
                primaryText: row.track.track_name ?? "Unknown track",
                primaryBadgeText: formulaRankDeltaText(row.track) ?? (showSourceBadge ? formatTrackSourceBadge(row.track) : null),
                secondaryBadgeText: row.hiddenCount > 0 ? `+${row.hiddenCount} more` : null,
                secondaryText: row.track.artist_name ?? "Unknown artist",
                tertiaryText: row.track.album_name ?? "Unknown album",
                metricText: isAllTimeTrackSection
                  ? (
                      section === "tracksAllTimeNew"
                        ? `${row.track.play_count ?? 0} | ${formatTrackLongevity(row.track) ?? "0d"}`
                        : formatTrackRankingMetric(row.track)
                    )
                  : null,
                trackUri: row.track.uri ?? null,
                previewTrack: row.track,
              },
              row.track.track_id ?? `${row.track.track_name}-${index}-${section}`,
            ),
          )}
          {Array.from({ length: emptySlots(pageRows) }).map((_, index) => (
            <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
          ))}
        </div>
        {paged ? renderPaging(section, cappedRows.length) : null}
      </>
    );
  }

  function renderFormulaLabPage() {
    if (!profile) {
      return null;
    }

    const filteredMergedTracks = mergedTracks.filter((track) => {
      if (mergedTrackSourceFilter === "recent") {
        return track.source_label === "recent" || (track.has_recent_source && !track.has_history_source);
      }
      if (mergedTrackSourceFilter === "history") {
        return track.source_label === "history" || (track.has_history_source && !track.has_recent_source);
      }
      if (mergedTrackSourceFilter === "both") {
        return track.source_label === "both" || (track.has_recent_source && track.has_history_source);
      }
      return true;
    });
    const baselineRankedTracks = sortedTracksForView("tracksAllTimeCurrent", filteredMergedTracks);
    const candidateRankedTracks = sortedTracksForView("tracksAllTimeNew", filteredMergedTracks);
    const baselineRankByTrackKey = new Map(
      baselineRankedTracks.map((track, index) => [formulaTrackKey(track), index + 1]),
    );
    const candidateRankByTrackKey = new Map(
      candidateRankedTracks.map((track, index) => [formulaTrackKey(track), index + 1]),
    );
    const annotateRankMovement = (track: RecentTrack): RecentTrack => {
      const key = formulaTrackKey(track);
      const baselineRank = baselineRankByTrackKey.get(key);
      const candidateRank = candidateRankByTrackKey.get(key);
      const delta = baselineRank != null && candidateRank != null ? baselineRank - candidateRank : 0;
      return { ...track, formula_rank_delta: delta };
    };
    const rankMovementMatches = (track: RecentTrack) => {
      const delta = Number(track.formula_rank_delta ?? 0);
      if (rankMovementFilter === "risers") {
        return delta > 0;
      }
      if (rankMovementFilter === "fallers") {
        return delta < 0;
      }
      return true;
    };
    const baselineDisplayTracks = baselineRankedTracks
      .map(annotateRankMovement)
      .filter(rankMovementMatches);
    const candidateDisplayTracks = candidateRankedTracks
      .map(annotateRankMovement)
      .filter(rankMovementMatches);
    const movementFilteredTracks = filteredMergedTracks
      .map(annotateRankMovement)
      .filter((track) => {
        return rankMovementMatches(track);
      });
    const filteredTrackCount = movementFilteredTracks.length;
    const sourceFilterLabel = MERGED_TRACK_SOURCE_FILTER_OPTIONS.find((option) => option.value === mergedTrackSourceFilter)?.label ?? "All plays";
    const rankMovementLabel = RANK_MOVEMENT_FILTER_OPTIONS.find((option) => option.value === rankMovementFilter)?.label ?? "All";
    const mergedTrackEmptyCopy = mergedTracksLoaded
      ? `No ${sourceFilterLabel.toLowerCase()} tracks are available for this comparison.`
      : "Loading track rankings...";
    const mergedTrackUnavailableCopy = mergedTracksLoading
      ? "Loading track rankings..."
      : (mergedTracksError || "Track rankings are not available yet.");
    const formulaModeLabel =
      trackRankingMode === "plays"
        ? "plays"
        : trackRankingMode === "longevity"
          ? "longevity"
          : "mix";

    return (
      <section className="info-card info-card-wide tracks-only-card" id="tracks-page">
        <div className="tracks-only-header">
          <div className="section-column-header tracks-only-header-copy">
            <div>
              <h2>Top Tracks Formula Lab</h2>
              <p className="tracks-only-subtitle">
                Compare canonical track rankings built from merged play history.
              </p>
            </div>
            <div className="section-column-header-actions tracks-only-controls">
              {renderMergedTrackSourceFilterToggle()}
              {renderTrackRankingToggle()}
              {renderRankMovementFilterToggle()}
              <button
                className="secondary-button tracks-page-link-button"
                disabled={mergedTracksLoading}
                onClick={reloadTrackRankings}
                type="button"
              >
                {mergedTracksLoading ? "Reloading..." : "Reload rankings"}
              </button>
            </div>
          </div>
          <button
            className="secondary-button tracks-only-back-button"
            onClick={() => setAppPage("dashboard")}
            type="button"
          >
            Back to dashboard
          </button>
        </div>
        <div className="tracks-only-summary">
          <span>{filteredTrackCount} tracks in comparison</span>
          <span>{formulaModeLabel} mode</span>
          <span>{rankMovementLabel}</span>
          <span>{sourceFilterLabel}</span>
          {mergedTracksLastLoadedAt ? (
            <span>Loaded {new Date(mergedTracksLastLoadedAt).toLocaleTimeString()}</span>
          ) : null}
        </div>
        {mergedTracksError ? (
          <p className="empty-copy">
            {mergedTracksError}
            {" "}
            Refresh this page after confirming the frontend is pointed at the same backend where `/auth/session` is authenticated.
          </p>
        ) : null}
        <div className="artists-grid">
          <div className="artists-column">
            <div className="tracks-formula-heading">
              <h3>Baseline formula</h3>
              <span>{baselineFormulaLabel()}</span>
            </div>
            {renderTrackColumn(
              "tracksAllTimeCurrent",
              baselineDisplayTracks,
              mergedTracksLoaded && !mergedTracksLoading && !mergedTracksError,
              mergedTrackEmptyCopy,
              mergedTrackUnavailableCopy,
              undefined,
              false,
              true,
            )}
          </div>
          <div className="artists-column">
            <div className="tracks-formula-heading">
              <h3>Candidate formula</h3>
              <span>{candidateFormulaLabel()}</span>
            </div>
            {renderTrackColumn(
              "tracksAllTimeNew",
              candidateDisplayTracks,
              mergedTracksLoaded && !mergedTracksLoading && !mergedTracksError,
              mergedTrackEmptyCopy,
              mergedTrackUnavailableCopy,
              undefined,
              false,
              true,
            )}
          </div>
        </div>
      </section>
    );
  }

  function identityAuditTitle(example: TrackIdentityAuditExample): string {
    const candidates = [
      example.track_name,
      example.release_track_name,
      example.analysis_track_name,
      example.artist_name,
      example.grouping_note,
      example.example_type,
    ];
    const title = candidates.find((value) => typeof value === "string" && value.trim().length > 0);
    return typeof title === "string" ? title : "Identity example";
  }

  function identityAuditMeta(example: TrackIdentityAuditExample): string {
    const parts = [
      typeof example.artist_name === "string" ? example.artist_name : null,
      typeof example.listen_count === "number" ? `${example.listen_count} listens` : null,
      typeof example.folded_listen_count === "number" ? `${example.folded_listen_count} folded listens` : null,
      typeof example.spotify_track_id_count === "number" ? `${example.spotify_track_id_count} Spotify IDs` : null,
      typeof example.source_track_count === "number" ? `${example.source_track_count} source tracks` : null,
      typeof example.release_track_count === "number" ? `${example.release_track_count} release tracks` : null,
    ];
    return parts.filter(Boolean).join(" | ");
  }

  function auditString(value: unknown, fallback: string = "Unknown") {
    return typeof value === "string" && value.trim().length > 0 ? value : fallback;
  }

  function auditNumber(value: unknown): number | null {
    return typeof value === "number" && Number.isFinite(value) ? value : null;
  }

  function auditList(value: unknown): TrackIdentityAuditExample[] {
    return Array.isArray(value) ? value.filter((item): item is TrackIdentityAuditExample => Boolean(item) && typeof item === "object" && !Array.isArray(item)) : [];
  }

  function renderAuditStat(label: string, value: unknown) {
    if (value == null || value === "") {
      return null;
    }
    return (
      <span className="identity-audit-stat">
        <span>{label}</span>
        <strong>{String(value)}</strong>
      </span>
    );
  }

  function renderAuditVariantList(items: TrackIdentityAuditExample[], kind: "canonical" | "release" | "composition") {
    if (items.length === 0) {
      return <p className="empty-copy">No variants returned.</p>;
    }
    return (
      <div className="identity-audit-variant-list">
        {items.map((item, index) => {
          const title = kind === "composition"
            ? auditString(item.release_track_name, "Release track")
            : auditString(item.track_name ?? item.source_name_raw, "Variant");
          const subtitle = kind === "canonical"
            ? auditString(item.album_name, "Unknown album")
            : kind === "release"
              ? auditString(item.match_method, "Mapping")
              : auditString(item.status, "Suggestion");
          const listens = auditNumber(item.listen_count);
          const confidence = auditNumber(item.confidence);
          const idText = kind === "composition"
            ? `release ${auditString(item.release_track_id, "n/a")}`
            : auditString(item.spotify_track_id ?? item.external_id, "No Spotify ID");
          return (
            <div className="identity-audit-variant" key={`${idText}-${index}`}>
              <div className="identity-audit-variant-main">
                <strong>{title}</strong>
                <span>{subtitle}</span>
                <code>{idText}</code>
              </div>
              <div className="identity-audit-variant-stats">
                {listens != null ? <span>{listens} listens</span> : null}
                {confidence != null ? <span>{Math.round(confidence * 100)}% confidence</span> : null}
                {typeof item.source_track_count === "number" ? <span>{item.source_track_count} sources</span> : null}
              </div>
            </div>
          );
        })}
      </div>
    );
  }

  function renderIdentityAuditExample(example: TrackIdentityAuditExample, index: number) {
    const exampleType = auditString(example.example_type, "identity");
    const isCanonical = exampleType === "same_name_canonical_split";
    const isRelease = exampleType === "release_track_source_split";
    const isComposition = exampleType === "analysis_track_group";
    const title = identityAuditTitle(example);
    const meta = identityAuditMeta(example);
    const variantItems = isCanonical
      ? auditList(example.variants)
      : isRelease
        ? auditList(example.source_tracks)
        : auditList(example.release_tracks);
    const variantKind = isCanonical ? "canonical" : isRelease ? "release" : "composition";

    return (
      <article className="identity-audit-example" key={`${exampleType}-${title}-${index}`}>
        <div className="identity-audit-example-header">
          <div>
            <h4>{title}</h4>
            {meta ? <p>{meta}</p> : null}
          </div>
          <span className="identity-audit-type-badge">
            {isCanonical ? "Canonical" : isRelease ? "Release" : "Composition"}
          </span>
        </div>
        <div className="identity-audit-stats">
          {renderAuditStat("Spotify IDs", example.spotify_track_id_count)}
          {renderAuditStat("Sources", example.source_track_count)}
          {renderAuditStat("Release tracks", example.release_track_count)}
          {renderAuditStat("Folded listens", example.folded_listen_count)}
          {renderAuditStat("First listened", example.first_listened_at)}
          {renderAuditStat("Last listened", example.last_listened_at)}
        </div>
        {typeof example.grouping_note === "string" ? (
          <p className="identity-audit-note">{example.grouping_note}</p>
        ) : null}
        {renderAuditVariantList(variantItems, variantKind)}
      </article>
    );
  }

  function renderIdentityAuditGroup(title: string, examples: TrackIdentityAuditExample[]) {
    return (
      <div className="identity-audit-group">
        <div className="tracks-formula-heading">
          <h3>{title}</h3>
          <span>{examples.length} examples</span>
        </div>
        {examples.length === 0 ? (
          <p className="empty-copy">No examples returned for this group.</p>
        ) : (
          <div className="identity-audit-examples">
            {examples.map((example, index) => renderIdentityAuditExample(example, index))}
          </div>
        )}
      </div>
    );
  }

  function updateLocalReviewDecision(
    entryId: string,
    patch: Partial<LocalReviewDecision>,
  ) {
    setIdentityAuditLocalDecisions((current) => {
      const existing = current[entryId] ?? {
        verdict: "unsure" as LocalReviewVerdict,
        grouping_target: null,
        note: "",
        updated_at_ms: Date.now(),
      };
      return {
        ...current,
        [entryId]: {
          ...existing,
          ...patch,
          updated_at_ms: Date.now(),
        },
      };
    });
  }

  function renderIdentityAuditOverviewTab() {
    const canonicalCount = identityAudit?.same_name_canonical_splits.length ?? 0;
    const releaseCount = identityAudit?.release_track_source_splits.length ?? 0;
    const compositionCount = identityAudit?.analysis_track_groups.length ?? 0;
    const suggestedCount = identityAuditSuggestedGroups?.summary.total_groups ?? 0;
    const ambiguousCount = identityAuditAmbiguous?.summary.total_review_entries ?? 0;
    return (
      <div className="identity-audit-overview-grid">
        <article className="identity-audit-overview-card">
          <h3>Canonical Splits</h3>
          <p>Same normalized title/artist with multiple Spotify IDs.</p>
          <strong>{canonicalCount}</strong>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Release Track Splits</h3>
          <p>Multiple source tracks folded under a single release track.</p>
          <strong>{releaseCount}</strong>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Composition Groups</h3>
          <p>Release tracks grouped at composition level for analysis.</p>
          <strong>{compositionCount}</strong>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Suggested Groups</h3>
          <p>Suggested composition links from conservative title/artist matching.</p>
          <strong>{suggestedCount}</strong>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Ambiguous Queue</h3>
          <p>Items requiring human judgment across variant-rule families.</p>
          <strong>{ambiguousCount}</strong>
        </article>
      </div>
    );
  }

  function renderIdentityAuditCanonicalTab() {
    if (identityAuditError) {
      return <p className="empty-copy">{identityAuditError}</p>;
    }
    if (!identityAudit) {
      return <p className="empty-copy">{identityAuditLoading ? "Loading canonical splits..." : "Canonical splits are not loaded yet."}</p>;
    }
    return (
      <div className="identity-audit-grid">
        <p className="identity-audit-tab-copy">Canonical checks show where same-name entities still split across Spotify track IDs.</p>
        {renderIdentityAuditGroup("Canonical Splits", identityAudit.same_name_canonical_splits)}
      </div>
    );
  }

  function releaseAlbumMergePreviewKey(prefix: string, releaseAlbums: Array<{ release_album_id: number }>) {
    return `${prefix}:${releaseAlbums.map((item) => item.release_album_id).sort((a, b) => a - b).join(",")}`;
  }

  async function loadReleaseAlbumMergePreview(key: string, releaseAlbumIds: number[]) {
    if (releaseAlbumMergePreviewLoadingKey) {
      return;
    }
    setReleaseAlbumMergePreviewLoadingKey(key);
    setReleaseAlbumMergePreviewErrorByKey((current) => ({ ...current, [key]: "" }));
    try {
      const preview = await postReleaseAlbumMergePreview(releaseAlbumIds);
      setReleaseAlbumMergePreviewByKey((current) => ({ ...current, [key]: preview }));
    } catch (error) {
      setReleaseAlbumMergePreviewErrorByKey((current) => ({
        ...current,
        [key]: formatUiErrorMessage(error, "Failed to preview release album merge."),
      }));
    } finally {
      setReleaseAlbumMergePreviewLoadingKey(null);
    }
  }

  async function loadReleaseAlbumMergeDryRun(key: string, releaseAlbumIds: number[], survivorReleaseAlbumId: number) {
    if (releaseAlbumMergeDryRunLoadingKey) {
      return;
    }
    setReleaseAlbumMergeDryRunLoadingKey(key);
    setReleaseAlbumMergeDryRunErrorByKey((current) => ({ ...current, [key]: "" }));
    try {
      const dryRun = await postReleaseAlbumMergeDryRun(releaseAlbumIds, survivorReleaseAlbumId);
      setReleaseAlbumMergeDryRunByKey((current) => ({ ...current, [key]: dryRun }));
    } catch (error) {
      setReleaseAlbumMergeDryRunErrorByKey((current) => ({
        ...current,
        [key]: formatUiErrorMessage(error, "Failed to dry run release album merge."),
      }));
    } finally {
      setReleaseAlbumMergeDryRunLoadingKey(null);
    }
  }

  function renderReleaseAlbumMergeDryRun(key: string) {
    const dryRun = releaseAlbumMergeDryRunByKey[key];
    const error = releaseAlbumMergeDryRunErrorByKey[key];
    if (error) {
      return <p className="empty-copy">{error}</p>;
    }
    if (!dryRun) {
      return null;
    }
    return (
      <div style={{ marginTop: "12px" }}>
        <div className="tracks-formula-heading">
          <h3>Dry Run Plan</h3>
          <span>{dryRun.blocked ? "blocked" : "ready"}</span>
        </div>
        {dryRun.blocked_reasons.map((reason) => (
          <p className="empty-copy" key={`release-album-dry-run-blocked-${key}-${reason}`}>{reason}</p>
        ))}
        <p className="empty-copy">
          Rows: source maps {dryRun.rows_affected.source_album_map ?? 0}, artist inserts {dryRun.rows_affected.album_artist_insert ?? 0}, artist deletes {dryRun.rows_affected.album_artist_delete ?? 0}, track repoints {dryRun.rows_affected.album_track_repoint ?? 0}, track conflicts {dryRun.rows_affected.album_track_conflict_delete ?? 0}, retired albums {dryRun.rows_affected.release_album_retire ?? 0}
        </p>
        {dryRun.statements.map((statement) => (
          <p className="empty-copy" key={`release-album-dry-run-statement-${key}-${statement}`}>{statement}</p>
        ))}
        {Object.entries(dryRun.plan).map(([planKey, rows]) => (
          rows.length > 0 ? (
            <details key={`release-album-dry-run-plan-${key}-${planKey}`} open={planKey === "album_track_conflicts"}>
              <summary>{planKey.replace(/_/g, " ")} ({rows.length})</summary>
              <pre style={{ whiteSpace: "pre-wrap", overflowX: "auto" }}>{JSON.stringify(rows, null, 2)}</pre>
            </details>
          ) : null
        ))}
      </div>
    );
  }

  function renderReleaseAlbumMergePreview(key: string) {
    const preview = releaseAlbumMergePreviewByKey[key];
    const error = releaseAlbumMergePreviewErrorByKey[key];
    if (error) {
      return <p className="empty-copy">{error}</p>;
    }
    if (!preview) {
      return null;
    }
    const readinessLabel = preview.merge_readiness.replace(/_/g, " ");
    return (
      <div className="identity-audit-group" style={{ marginTop: "12px" }}>
        <div className="tracks-formula-heading">
          <h3>Merge Preview</h3>
          <span>{readinessLabel} · Survivor {preview.survivor_release_album_id ?? "None"}</span>
        </div>
        {preview.readiness_reasons.length > 0 ? (
          <div>
            {preview.readiness_reasons.map((reason) => (
              <p className="empty-copy" key={`release-album-merge-readiness-${key}-${reason}`}>{reason}</p>
            ))}
          </div>
        ) : null}
        {preview.warnings.length > 0 ? (
          <div>
            {preview.warnings.map((warning) => (
              <p className="empty-copy" key={`release-album-merge-warning-${key}-${warning}`}>{warning}</p>
            ))}
          </div>
        ) : null}
        <p className="empty-copy">
          Affected rows: source album maps {preview.affected.source_album_map_rows}, album artists {preview.affected.album_artist_rows}, release tracks {preview.affected.release_track_rows}, album tracks {preview.affected.album_track_rows}, album-track conflicts {preview.affected.album_track_conflicts}, raw listens {preview.affected.raw_play_event_rows}
        </p>
        <ul>
          {preview.proposed_operations.map((operation) => (
            <li key={`release-album-merge-operation-${key}-${operation}`}>{operation}</li>
          ))}
        </ul>
        {preview.survivor_release_album_id !== null ? (() => {
          const survivorReleaseAlbumId = preview.survivor_release_album_id;
          return (
            <button
              className="track-ranking-chip"
              type="button"
              disabled={releaseAlbumMergeDryRunLoadingKey !== null}
              onClick={() => void loadReleaseAlbumMergeDryRun(
                key,
                [survivorReleaseAlbumId, ...preview.merge_release_album_ids],
                survivorReleaseAlbumId,
              )}
            >
              {releaseAlbumMergeDryRunLoadingKey === key ? "Loading..." : "Dry run"}
            </button>
          );
        })() : null}
        {renderReleaseAlbumMergeDryRun(key)}
      </div>
    );
  }

  function renderIdentityAuditReleaseTab() {
    const releaseTrackSplits = identityAudit?.release_track_source_splits ?? [];
    return (
      <div className="identity-audit-grid">
        <p className="identity-audit-tab-copy">
          Release review highlights candidates that likely belong to one release identity but are split today.
        </p>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Duplicate Albums (Same Spotify Album ID)</h3>
            <span>{albumDuplicateLookupResult?.total ?? 0} groups</span>
          </div>
          {albumDuplicateLookupError ? <p className="empty-copy">{albumDuplicateLookupError}</p> : null}
          {!albumDuplicateLookupResult && albumDuplicateLookupLoading ? (
            <p className="empty-copy">Loading duplicate album groups...</p>
          ) : null}
          {!albumDuplicateLookupLoading && (!albumDuplicateLookupResult || albumDuplicateLookupResult.items.length === 0) ? (
            <p className="empty-copy">No duplicate albums found.</p>
          ) : null}
          {albumDuplicateLookupResult && albumDuplicateLookupResult.items.length > 0 ? (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Album</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Name</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Release Album</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Artist</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Dup Count</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Preview</th>
                  </tr>
                </thead>
                <tbody>
                  {albumDuplicateLookupResult.items.map((group) => {
                    const previewKey = releaseAlbumMergePreviewKey(`spotify:${group.spotify_album_id}`, group.release_albums);
                    const releaseAlbumIds = group.release_albums.map((item) => item.release_album_id);
                    return (
                      <Fragment key={`identity-release-dup-album-group-${group.spotify_album_id}`}>
                        {group.release_albums.map((item, index) => (
                          <tr key={`identity-release-dup-album-${group.spotify_album_id}-${item.release_album_id}`}>
                            <td style={{ padding: "8px", verticalAlign: "top", wordBreak: "break-word" }}>{index === 0 ? group.spotify_album_id : ""}</td>
                            <td style={{ padding: "8px", verticalAlign: "top" }}>{index === 0 ? (group.spotify_album_name ?? "Unknown") : ""}</td>
                            <td style={{ padding: "8px", verticalAlign: "top", fontWeight: 600 }}>{item.release_album_name}</td>
                            <td style={{ padding: "8px", verticalAlign: "top" }}>{item.artist_name}</td>
                            <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{index === 0 ? group.duplicate_count : ""}</td>
                            <td style={{ padding: "8px", verticalAlign: "top" }}>
                              {index === 0 ? (
                                <button
                                  className="track-ranking-chip"
                                  type="button"
                                  disabled={releaseAlbumMergePreviewLoadingKey !== null}
                                  onClick={() => void loadReleaseAlbumMergePreview(previewKey, releaseAlbumIds)}
                                >
                                  {releaseAlbumMergePreviewLoadingKey === previewKey ? "Loading..." : "Preview merge"}
                                </button>
                              ) : null}
                            </td>
                          </tr>
                        ))}
                        {releaseAlbumMergePreviewByKey[previewKey] || releaseAlbumMergePreviewErrorByKey[previewKey] ? (
                          <tr>
                            <td colSpan={6} style={{ padding: "8px" }}>{renderReleaseAlbumMergePreview(previewKey)}</td>
                          </tr>
                        ) : null}
                      </Fragment>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : null}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Duplicate Albums (Same Name + Primary Artist)</h3>
            <span>{albumNameDuplicateLookupResult?.total ?? 0} groups</span>
          </div>
          {albumNameDuplicateLookupError ? <p className="empty-copy">{albumNameDuplicateLookupError}</p> : null}
          {!albumNameDuplicateLookupResult && albumNameDuplicateLookupLoading ? (
            <p className="empty-copy">Loading duplicate album name groups...</p>
          ) : null}
          {!albumNameDuplicateLookupLoading && (!albumNameDuplicateLookupResult || albumNameDuplicateLookupResult.items.length === 0) ? (
            <p className="empty-copy">No duplicate album name groups found.</p>
          ) : null}
          {albumNameDuplicateLookupResult && albumNameDuplicateLookupResult.items.length > 0 ? (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Normalized Album</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Normalized Primary Artist</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Album IDs</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Release Album</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Artist</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Album</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Tracklist</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Catalog</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Queue</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Dup Count</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Preview</th>
                  </tr>
                </thead>
                <tbody>
                  {albumNameDuplicateLookupResult.items.map((group) => {
                    const previewKey = releaseAlbumMergePreviewKey(`name:${group.normalized_album_name}:${group.normalized_primary_artist}`, group.release_albums);
                    const releaseAlbumIds = group.release_albums.map((item) => item.release_album_id);
                    return (
                      <Fragment key={`identity-release-dup-name-album-group-${group.normalized_album_name}-${group.normalized_primary_artist}`}>
                        {group.release_albums.map((item, index) => (
                          <tr key={`identity-release-dup-name-album-${group.normalized_album_name}-${group.normalized_primary_artist}-${item.release_album_id}`}>
                            <td style={{ padding: "8px", verticalAlign: "top" }}>{index === 0 ? group.normalized_album_name : ""}</td>
                            <td style={{ padding: "8px", verticalAlign: "top" }}>{index === 0 ? group.normalized_primary_artist : ""}</td>
                            <td style={{ padding: "8px", verticalAlign: "top", wordBreak: "break-word" }}>{index === 0 ? (group.spotify_album_ids.length > 0 ? group.spotify_album_ids.join(", ") : "None") : ""}</td>
                            <td style={{ padding: "8px", verticalAlign: "top", fontWeight: 600 }}>{item.release_album_name}</td>
                            <td style={{ padding: "8px", verticalAlign: "top" }}>{item.artist_name}</td>
                            <td style={{ padding: "8px", verticalAlign: "top", wordBreak: "break-word" }}>{item.spotify_album_id ?? "None"}{item.spotify_album_name ? ` (${item.spotify_album_name})` : ""}</td>
                            <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{item.album_track_rows} / {item.total_tracks ?? "?"}</td>
                            <td style={{ padding: "8px", verticalAlign: "top" }}>{item.catalog_status ?? "unknown"}</td>
                            <td style={{ padding: "8px", verticalAlign: "top" }}>{item.queue_status}</td>
                            <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{index === 0 ? group.duplicate_count : ""}</td>
                            <td style={{ padding: "8px", verticalAlign: "top" }}>
                              {index === 0 ? (
                                <button
                                  className="track-ranking-chip"
                                  type="button"
                                  disabled={releaseAlbumMergePreviewLoadingKey !== null}
                                  onClick={() => void loadReleaseAlbumMergePreview(previewKey, releaseAlbumIds)}
                                >
                                  {releaseAlbumMergePreviewLoadingKey === previewKey ? "Loading..." : "Preview merge"}
                                </button>
                              ) : null}
                            </td>
                          </tr>
                        ))}
                        {releaseAlbumMergePreviewByKey[previewKey] || releaseAlbumMergePreviewErrorByKey[previewKey] ? (
                          <tr>
                            <td colSpan={11} style={{ padding: "8px" }}>{renderReleaseAlbumMergePreview(previewKey)}</td>
                          </tr>
                        ) : null}
                      </Fragment>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : null}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Duplicate Tracks (Same Spotify Track ID)</h3>
            <span>{trackDuplicateLookupResult?.total ?? 0} groups</span>
          </div>
          {trackDuplicateLookupError ? <p className="empty-copy">{trackDuplicateLookupError}</p> : null}
          {!trackDuplicateLookupResult && trackDuplicateLookupLoading ? (
            <p className="empty-copy">Loading duplicate track groups...</p>
          ) : null}
          {!trackDuplicateLookupLoading && (!trackDuplicateLookupResult || trackDuplicateLookupResult.items.length === 0) ? (
            <p className="empty-copy">No duplicate tracks found.</p>
          ) : null}
          {trackDuplicateLookupResult && trackDuplicateLookupResult.items.length > 0 ? (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Track</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Name</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Duration</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Release Track</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Artist</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Release Album</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Album</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Catalog</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Queue</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Dup Count</th>
                  </tr>
                </thead>
                <tbody>
                  {trackDuplicateLookupResult.items.map((group) =>
                    group.release_tracks.map((item, index) => (
                      <tr key={`identity-release-dup-track-${group.spotify_track_id}-${item.release_track_id}`}>
                        <td style={{ padding: "8px", verticalAlign: "top", wordBreak: "break-word" }}>{index === 0 ? group.spotify_track_id : ""}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{index === 0 ? (group.spotify_track_name ?? "Unknown") : ""}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{index === 0 ? (group.duration_display ?? "Unknown") : ""}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", fontWeight: 600 }}>{item.release_track_name}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{item.artist_name}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{item.release_album_name}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", wordBreak: "break-word" }}>{item.spotify_album_id ?? "Unknown"}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{item.catalog_status ?? "unknown"}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{item.queue_status}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{index === 0 ? group.duplicate_count : ""}</td>
                      </tr>
                    )),
                  )}
                </tbody>
              </table>
            </div>
          ) : null}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Release Track Split Signals</h3>
            <span>{releaseTrackSplits.length} examples</span>
          </div>
          <p className="identity-audit-tab-copy">
            These examples show source-track rows collapsing into one release track and can indicate additional release-level cleanup opportunities.
          </p>
          {identityAuditError ? <p className="empty-copy">{identityAuditError}</p> : null}
          {!identityAudit && identityAuditLoading ? <p className="empty-copy">Loading release track splits...</p> : null}
          {identityAudit && releaseTrackSplits.length > 0 ? renderIdentityAuditGroup("Release Track Splits", releaseTrackSplits) : null}
          {identityAudit && releaseTrackSplits.length === 0 ? <p className="empty-copy">No release track split examples returned.</p> : null}
        </div>
      </div>
    );
  }

  function renderIdentityAuditCompositionTab() {
    if (identityAuditError) {
      return <p className="empty-copy">{identityAuditError}</p>;
    }
    if (!identityAudit) {
      return <p className="empty-copy">{identityAuditLoading ? "Loading composition examples..." : "Composition examples are not loaded yet."}</p>;
    }
    return (
      <div className="identity-audit-grid">
        <p className="identity-audit-tab-copy">
          Composition checks show currently grouped release tracks.
        </p>
        {renderIdentityAuditGroup("Current Composition Groups", identityAudit.analysis_track_groups)}
      </div>
    );
  }

  function renderIdentityAuditAmbiguousTab() {
    const familyOptions = identityAuditAmbiguous?.family_counts ?? [];
    const suggestedItems = identityAuditSuggestedGroups?.items ?? [];
    const filteredItems = computeAmbiguousTrackItems();
    const unifiedItems = computeUnifiedReviewItems();
    const visibleItems = filteredItems.slice(0, identityAuditAmbiguousVisibleCount);
    const focusedItem = identityAuditFocusedReviewKey == null
      ? null
      : (unifiedItems.find((item) => item.decision_key === identityAuditFocusedReviewKey) ?? null);
    const focusedDecision = focusedItem ? identityAuditLocalDecisions[focusedItem.decision_key] : undefined;
    const reviewedAmbiguousCount = filteredItems.reduce((count, item) => (
      isReviewedDecision(identityAuditLocalDecisions[trackDecisionKey(item)]) ? count + 1 : count
    ), 0);
    const reviewedSuggestedCount = suggestedItems.reduce((count, group) => {
      const decision = identityAuditLocalDecisions[groupDecisionKey(group)];
      return isReviewedDecision(decision) ? count + 1 : count;
    }, 0);
    const reviewedCount = reviewedAmbiguousCount + reviewedSuggestedCount;
    const totalReviewableCount = filteredItems.length + suggestedItems.length;
    const summaryByFamily = new Map<string, { total: number; approved: number; rejected: number; skipped: number; unreviewed: number }>();
    for (const item of unifiedItems) {
      const current = summaryByFamily.get(item.family_label) ?? { total: 0, approved: 0, rejected: 0, skipped: 0, unreviewed: 0 };
      current.total += 1;
      const verdict = identityAuditLocalDecisions[item.decision_key]?.verdict ?? "unsure";
      if (verdict === "good_to_group") {
        current.approved += 1;
      } else if (verdict === "not_good") {
        current.rejected += 1;
      } else if (verdict === "skipped") {
        current.skipped += 1;
      } else {
        current.unreviewed += 1;
      }
      summaryByFamily.set(item.family_label, current);
    }
    const summaryEntries = Array.from(summaryByFamily.entries())
      .sort((left, right) => right[1].total - left[1].total || left[0].localeCompare(right[0]));
    const visibleSummaryEntries = summaryEntries.slice(0, 8);
    const remainingSummaryCount = Math.max(0, summaryEntries.length - visibleSummaryEntries.length);

    const groupApproved: Array<Record<string, unknown>> = [];
    const groupRejected: Array<Record<string, unknown>> = [];
    const groupSkipped: Array<Record<string, unknown>> = [];
    const trackApproved: Array<Record<string, unknown>> = [];
    const trackRejected: Array<Record<string, unknown>> = [];
    const trackSkipped: Array<Record<string, unknown>> = [];

    for (const item of unifiedItems) {
      const decision = identityAuditLocalDecisions[item.decision_key];
      if (!decision || decision.verdict === "unsure") {
        continue;
      }
      if (item.item_type === "group") {
        const group = item.group;
        const label = group?.analysis_track_name || (group?.analysis_track_id != null ? `analysis_track ${group.analysis_track_id}` : item.decision_key);
        const entry = {
          decision_key: item.decision_key,
          id: group?.analysis_track_id ?? item.decision_key,
          decision: decision.verdict,
          label,
          family: group?.song_family_key ?? item.family_label,
          bucket: item.bucket_label,
          would: decision.verdict === "good_to_group"
            ? `Would group as composition family: ${label}`
            : decision.verdict === "not_good"
              ? `Would keep suggested group separate: ${label}`
              : `Would defer suggested group: ${label}`,
          source: group
            ? {
                analysis_track_id: group.analysis_track_id,
                analysis_track_name: group.analysis_track_name,
                song_family_key: group.song_family_key,
                release_track_count: group.release_track_count,
                confidence: group.confidence,
                match_method: group.match_method,
              }
            : null,
        };
        if (decision.verdict === "good_to_group") {
          groupApproved.push(entry);
        } else if (decision.verdict === "not_good") {
          groupRejected.push(entry);
        } else {
          groupSkipped.push(entry);
        }
      } else {
        const track = item.track;
        const label = track?.release_track_name || (track?.release_track_id != null ? `release_track ${track.release_track_id}` : item.decision_key);
        const entry = {
          decision_key: item.decision_key,
          id: track?.release_track_id ?? item.decision_key,
          decision: decision.verdict,
          label,
          family: track?.dominant_family ?? item.family_label,
          bucket: track?.bucket ?? item.bucket_label,
          would: decision.verdict === "good_to_group"
            ? `Would accept track identity mapping: ${label}`
            : decision.verdict === "not_good"
              ? `Would reject track identity mapping: ${label}`
              : `Would defer track decision: ${label}`,
          source: track
            ? {
                release_track_id: track.release_track_id,
                release_track_name: track.release_track_name,
                artist_name: track.artist_name,
                analysis_name: track.analysis_name,
                bucket: track.bucket,
                dominant_family: track.dominant_family,
                review_families: track.review_families,
                confidence: track.confidence,
              }
            : null,
        };
        if (decision.verdict === "good_to_group") {
          trackApproved.push(entry);
        } else if (decision.verdict === "not_good") {
          trackRejected.push(entry);
        } else {
          trackSkipped.push(entry);
        }
      }
    }

    const totalLocalDecisions = (
      groupApproved.length
      + groupRejected.length
      + groupSkipped.length
      + trackApproved.length
      + trackRejected.length
      + trackSkipped.length
    );
    const previewPayload = {
      generated_at: new Date().toISOString(),
      summary: {
        total_local_decisions: totalLocalDecisions,
        groups: {
          approved: groupApproved.length,
          rejected: groupRejected.length,
          skipped: groupSkipped.length,
        },
        tracks: {
          approved: trackApproved.length,
          rejected: trackRejected.length,
          skipped: trackSkipped.length,
        },
      },
      decisions: {
        groups: {
          approved: groupApproved,
          rejected: groupRejected,
          skipped: groupSkipped,
        },
        tracks: {
          approved: trackApproved,
          rejected: trackRejected,
          skipped: trackSkipped,
        },
      },
    };
    const previewJson = JSON.stringify(previewPayload, null, 2);
    const canSaveSubmission = Boolean(
      totalLocalDecisions > 0
      && identityAuditPreviewValidationResult
      && !identityAuditPreviewValidationLoading,
    );

    const copyPreviewJson = async () => {
      if (!("clipboard" in navigator) || typeof navigator.clipboard?.writeText !== "function") {
        setIdentityAuditPreviewCopyStatus("Clipboard unavailable");
        return;
      }
      try {
        await navigator.clipboard.writeText(previewJson);
        setIdentityAuditPreviewCopyStatus("Copied JSON");
      } catch {
        setIdentityAuditPreviewCopyStatus("Copy failed");
      }
    };

    const downloadPreviewJson = () => {
      try {
        const blob = new Blob([previewJson], { type: "application/json;charset=utf-8" });
        const objectUrl = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = objectUrl;
        link.download = "identity-audit-submission-preview.json";
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(objectUrl);
      } catch {
        // Keep silent; this is a convenience path only.
      }
    };

    const validatePreviewJson = async () => {
      if (identityAuditPreviewValidationLoading) {
        return;
      }
      setIdentityAuditPreviewValidationLoading(true);
      setIdentityAuditPreviewValidationError("");
      try {
        const response = await fetch(
          `${apiBaseUrl}/debug/tracks/identity-audit/submission-preview/validate`,
          {
            method: "POST",
            credentials: "include",
            headers: { "Content-Type": "application/json" },
            body: previewJson,
          },
        );
        if (!response.ok) {
          let detail = "Failed to validate submission preview.";
          try {
            const payload = (await response.json()) as { detail?: string };
            if (payload.detail) {
              detail = payload.detail;
            }
          } catch {
            // keep fallback
          }
          throw new Error(detail);
        }
        const payload = (await response.json()) as SubmissionPreviewValidationResponse;
        setIdentityAuditPreviewValidationResult(payload);
        setIdentityAuditPreviewValidatedAt(Date.now());
      } catch (error) {
        setIdentityAuditPreviewValidationError(formatUiErrorMessage(error, "Failed to validate preview."));
        setIdentityAuditPreviewValidationResult(null);
        setIdentityAuditPreviewValidatedAt(null);
      } finally {
        setIdentityAuditPreviewValidationLoading(false);
      }
    };

    const saveSubmissionPreview = async () => {
      if (identityAuditSubmissionSaveLoading || !canSaveSubmission) {
        return;
      }
      setIdentityAuditSubmissionSaveLoading(true);
      setIdentityAuditSubmissionSaveError("");
      setIdentityAuditSubmissionSaveResult(null);
      try {
        const response = await fetch(
          `${apiBaseUrl}/debug/tracks/identity-audit/submissions`,
          {
            method: "POST",
            credentials: "include",
            headers: { "Content-Type": "application/json" },
            body: previewJson,
          },
        );
        if (!response.ok) {
          let detail = "Failed to save submission.";
          try {
            const payload = (await response.json()) as { detail?: string };
            if (payload.detail) {
              detail = payload.detail;
            }
          } catch {
            // keep fallback
          }
          throw new Error(detail);
        }
        const payload = (await response.json()) as IdentityAuditSubmissionSaveResponse;
        setIdentityAuditSubmissionSaveResult(payload);
        void loadIdentityAuditSavedSubmissions(true);
      } catch (error) {
        setIdentityAuditSubmissionSaveError(formatUiErrorMessage(error, "Failed to save submission."));
      } finally {
        setIdentityAuditSubmissionSaveLoading(false);
      }
    };

    const renderPreviewBucket = (title: string, entries: Array<Record<string, unknown>>) => (
      <div className="identity-audit-group" key={`preview-${title}`}>
        <div className="tracks-formula-heading">
          <h3>{title}</h3>
          <span>{entries.length}</span>
        </div>
        {entries.length === 0 ? (
          <p className="empty-copy">None</p>
        ) : (
          <div className="identity-audit-variant-list">
            {entries.map((entry, index) => (
              <div className="identity-audit-variant" key={`preview-entry-${title}-${String(entry.decision_key)}-${index}`}>
                <div className="identity-audit-variant-main">
                  <strong>{String(entry.label ?? entry.id ?? "Unknown item")}</strong>
                  <span>{String(entry.would ?? "")}</span>
                  <code>{String(entry.decision_key ?? "")}</code>
                </div>
                <div className="identity-audit-variant-stats">
                  {entry.family ? <span>{String(entry.family)}</span> : null}
                  {entry.bucket ? <span>{String(entry.bucket)}</span> : null}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    );

    const applyFocusedAction = (verdict: LocalReviewVerdict) => {
      if (!focusedItem) {
        return;
      }
      const nextDecisions = {
        ...identityAuditLocalDecisions,
        [focusedItem.decision_key]: {
          verdict,
          grouping_target: verdict === "good_to_group"
            ? (identityAuditLocalDecisions[focusedItem.decision_key]?.grouping_target ?? "same_composition")
            : null,
          note: identityAuditLocalDecisions[focusedItem.decision_key]?.note ?? "",
          updated_at_ms: Date.now(),
        },
      };
      updateLocalReviewDecision(focusedItem.decision_key, {
        verdict,
        grouping_target: verdict === "good_to_group"
          ? (identityAuditLocalDecisions[focusedItem.decision_key]?.grouping_target ?? "same_composition")
          : null,
      });
      setIdentityAuditFocusedReviewKey(findNextUnreviewedDecisionKey(unifiedItems, focusedItem.decision_key, nextDecisions));
    };

    return (
      <div className="identity-audit-grid">
        <div className="identity-audit-ambiguous-toolbar">
          <p className="identity-audit-tab-copy">
            Review ambiguous rows and suggested composition groups in one local-only queue.
          </p>
          <div className="identity-audit-ambiguous-summary">
            <span className="identity-audit-pill">Local only (not saved)</span>
            <span className="identity-audit-pill">Reviewed {reviewedCount} / {totalReviewableCount}</span>
            <span className="identity-audit-pill">Shortcuts: A approve, R reject, S skip, N next</span>
            <button
              className="secondary-button"
              onClick={() => {
                setIdentityAuditLocalDecisions({});
                setIdentityAuditPreviewCopyStatus("");
                setIdentityAuditPreviewValidationLoading(false);
                setIdentityAuditPreviewValidationError("");
                setIdentityAuditPreviewValidationResult(null);
                setIdentityAuditPreviewValidatedAt(null);
                setIdentityAuditSubmissionSaveLoading(false);
                setIdentityAuditSubmissionSaveError("");
                setIdentityAuditSubmissionSaveResult(null);
              }}
              type="button"
            >
              Reset local decisions
            </button>
          </div>
        </div>
        <div className="identity-audit-ambiguous-filters">
          <label>
            Family
            <select
              onChange={(event) => setIdentityAuditAmbiguousFamilyFilter(event.target.value)}
              value={identityAuditAmbiguousFamilyFilter}
            >
              <option value="all">All families</option>
              {familyOptions.map((family) => (
                <option key={`family-${family.family}`} value={family.family}>{family.family} ({family.count})</option>
              ))}
            </select>
          </label>
          <label>
            Bucket
            <select
              onChange={(event) => setIdentityAuditAmbiguousBucketFilter(event.target.value as "all" | "grouped" | "ungrouped")}
              value={identityAuditAmbiguousBucketFilter}
            >
              <option value="all">All</option>
              <option value="grouped">Grouped</option>
              <option value="ungrouped">Ungrouped</option>
            </select>
          </label>
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Progress Summary</h3>
            <span>{summaryEntries.length} buckets</span>
          </div>
          {visibleSummaryEntries.length > 0 ? (
            <div className="identity-audit-stats">
              {visibleSummaryEntries.map(([label, counts]) => (
                <span className="identity-audit-stat" key={`summary-${label}`}>
                  <span>{label}</span>
                  <strong>
                    {counts.total} total | {counts.approved} approved | {counts.rejected} rejected | {counts.skipped} skipped | {counts.unreviewed} unreviewed
                  </strong>
                </span>
              ))}
              {remainingSummaryCount > 0 ? (
                <span className="identity-audit-stat"><span>More buckets</span><strong>+{remainingSummaryCount} more</strong></span>
              ) : null}
            </div>
          ) : (
            <p className="empty-copy">No review buckets available yet.</p>
          )}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Next Unreviewed</h3>
            <span>{findNextUnreviewedDecisionKey(unifiedItems) ? "Ready" : "Complete"}</span>
          </div>
          {focusedItem ? (
            <article className="identity-audit-example">
              <div className="identity-audit-example-header">
                <div>
                  <h4>{focusedItem.title}</h4>
                  <p>{focusedItem.subtitle}</p>
                </div>
                <span className="identity-audit-type-badge">{focusedItem.item_type === "group" ? "Suggested group" : "Ambiguous track"}</span>
              </div>
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Bucket</span><strong>{focusedItem.bucket_label}</strong></span>
                <span className="identity-audit-stat"><span>Family</span><strong>{focusedItem.family_label}</strong></span>
                <span className="identity-audit-stat"><span>Current</span><strong>{focusedDecision?.verdict ?? "unreviewed"}</strong></span>
              </div>
              <div className="identity-audit-ambiguous-summary">
                <button className="secondary-button" onClick={() => applyFocusedAction("good_to_group")} type="button">Approve</button>
                <button className="secondary-button" onClick={() => applyFocusedAction("not_good")} type="button">Reject</button>
                <button className="secondary-button" onClick={() => applyFocusedAction("skipped")} type="button">Skip</button>
                <button
                  className="secondary-button"
                  onClick={() => setIdentityAuditFocusedReviewKey(findNextUnreviewedDecisionKey(unifiedItems, focusedItem.decision_key))}
                  type="button"
                >
                  Next unreviewed
                </button>
              </div>
            </article>
          ) : (
            <p className="empty-copy">All items reviewed locally.</p>
          )}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Submission Preview (Local Only)</h3>
            <span>{totalLocalDecisions} decisions</span>
          </div>
          <div className="identity-audit-ambiguous-summary">
            <span className="identity-audit-pill">Groups: {groupApproved.length} approved, {groupRejected.length} rejected, {groupSkipped.length} skipped</span>
            <span className="identity-audit-pill">Tracks: {trackApproved.length} approved, {trackRejected.length} rejected, {trackSkipped.length} skipped</span>
            <button className="secondary-button" onClick={() => void copyPreviewJson()} type="button">Copy JSON</button>
            <button className="secondary-button" onClick={downloadPreviewJson} type="button">Download JSON</button>
            <button
              className="secondary-button"
              disabled={identityAuditPreviewValidationLoading}
              onClick={() => void validatePreviewJson()}
              type="button"
            >
              {identityAuditPreviewValidationLoading
                ? "Validating..."
                : identityAuditPreviewValidationResult
                  ? "Revalidate Preview"
                  : "Validate Preview"}
            </button>
            <button
              className="secondary-button"
              disabled={!canSaveSubmission || identityAuditSubmissionSaveLoading}
              onClick={() => void saveSubmissionPreview()}
              type="button"
            >
              {identityAuditSubmissionSaveLoading ? "Saving..." : "Save Submission"}
            </button>
            {identityAuditPreviewCopyStatus ? <span className="identity-audit-pill">{identityAuditPreviewCopyStatus}</span> : null}
          </div>
          <p className="empty-copy">Saved only. No changes applied.</p>
          {identityAuditPreviewValidationResult
            && (identityAuditPreviewValidationResult.summary.warnings > 0
              || identityAuditPreviewValidationResult.summary.unknown_groups > 0
              || identityAuditPreviewValidationResult.summary.unknown_tracks > 0) ? (
            <p className="empty-copy">Validation has warnings; saved record will include them.</p>
            ) : null}
          {identityAuditPreviewValidationError ? <p className="empty-copy">{identityAuditPreviewValidationError}</p> : null}
          {identityAuditSubmissionSaveError ? <p className="empty-copy">{identityAuditSubmissionSaveError}</p> : null}
          {identityAuditSubmissionSaveResult ? (
            <div className="identity-audit-group">
              <div className="tracks-formula-heading">
                <h3>Saved Submission</h3>
                <span>#{identityAuditSubmissionSaveResult.submission_id}</span>
              </div>
              <p className="empty-copy">
                Saved submission #{identityAuditSubmissionSaveResult.submission_id}
                {" "}
                ({identityAuditSubmissionSaveResult.status}) at {new Date(identityAuditSubmissionSaveResult.created_at).toLocaleString()}.
              </p>
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Warnings</span><strong>{identityAuditSubmissionSaveResult.warnings.length}</strong></span>
                <span className="identity-audit-stat"><span>Unknown groups</span><strong>{identityAuditSubmissionSaveResult.unknown_items.groups.length}</strong></span>
                <span className="identity-audit-stat"><span>Unknown tracks</span><strong>{identityAuditSubmissionSaveResult.unknown_items.tracks.length}</strong></span>
              </div>
            </div>
          ) : null}
          {identityAuditPreviewValidationResult ? (
            <div className="identity-audit-group">
              <div className="tracks-formula-heading">
                <h3>Validation Result</h3>
                <span>{identityAuditPreviewValidationResult.ok ? "ok" : "failed"}</span>
              </div>
              {identityAuditPreviewValidatedAt ? (
                <p className="empty-copy">Validated at {new Date(identityAuditPreviewValidatedAt).toLocaleTimeString()}</p>
              ) : null}
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Total</span><strong>{identityAuditPreviewValidationResult.summary.total_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Groups</span><strong>{identityAuditPreviewValidationResult.summary.group_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Tracks</span><strong>{identityAuditPreviewValidationResult.summary.track_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Warnings</span><strong>{identityAuditPreviewValidationResult.summary.warnings}</strong></span>
                <span className="identity-audit-stat"><span>Unknown groups</span><strong>{identityAuditPreviewValidationResult.summary.unknown_groups}</strong></span>
                <span className="identity-audit-stat"><span>Unknown tracks</span><strong>{identityAuditPreviewValidationResult.summary.unknown_tracks}</strong></span>
              </div>
              {identityAuditPreviewValidationResult.summary.total_decisions === 0 ? (
                <p className="empty-copy">No decisions to validate.</p>
              ) : null}
              {identityAuditPreviewValidationResult.warnings.length > 0 ? (
                <div className="identity-audit-variant-list">
                  {identityAuditPreviewValidationResult.warnings.map((warning, index) => (
                    <div className="identity-audit-variant" key={`validation-warning-${index}`}>
                      <div className="identity-audit-variant-main">
                        <strong>Warning</strong>
                        <span>{warning}</span>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="empty-copy">No validation warnings.</p>
              )}
              <div className="identity-audit-group">
                <div className="tracks-formula-heading">
                  <h3>Unknown Groups</h3>
                  <span>{identityAuditPreviewValidationResult.unknown_items.groups.length}</span>
                </div>
                {identityAuditPreviewValidationResult.unknown_items.groups.length > 0 ? (
                  <div className="identity-audit-variant-list">
                    {identityAuditPreviewValidationResult.unknown_items.groups.map((item, index) => (
                      <div className="identity-audit-variant" key={`unknown-group-${index}`}>
                        <div className="identity-audit-variant-main">
                          <strong>{String(item.label ?? item.id ?? "Unknown group")}</strong>
                          <code>{String(item.decision_key ?? "")}</code>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="empty-copy">None.</p>
                )}
              </div>
              <div className="identity-audit-group">
                <div className="tracks-formula-heading">
                  <h3>Unknown Tracks</h3>
                  <span>{identityAuditPreviewValidationResult.unknown_items.tracks.length}</span>
                </div>
                {identityAuditPreviewValidationResult.unknown_items.tracks.length > 0 ? (
                  <div className="identity-audit-variant-list">
                    {identityAuditPreviewValidationResult.unknown_items.tracks.map((item, index) => (
                      <div className="identity-audit-variant" key={`unknown-track-${index}`}>
                        <div className="identity-audit-variant-main">
                          <strong>{String(item.label ?? item.id ?? "Unknown track")}</strong>
                          <code>{String(item.decision_key ?? "")}</code>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="empty-copy">None.</p>
                )}
              </div>
            </div>
          ) : null}
          {totalLocalDecisions === 0 ? (
            <p className="empty-copy">No local decisions yet.</p>
          ) : (
            <div className="identity-audit-grid">
              <div className="identity-audit-group">
                <div className="tracks-formula-heading">
                  <h3>Group Decisions</h3>
                  <span>{groupApproved.length + groupRejected.length + groupSkipped.length}</span>
                </div>
                {renderPreviewBucket("Approved", groupApproved)}
                {renderPreviewBucket("Rejected", groupRejected)}
                {renderPreviewBucket("Skipped", groupSkipped)}
              </div>
              <div className="identity-audit-group">
                <div className="tracks-formula-heading">
                  <h3>Track Decisions</h3>
                  <span>{trackApproved.length + trackRejected.length + trackSkipped.length}</span>
                </div>
                {renderPreviewBucket("Approved", trackApproved)}
                {renderPreviewBucket("Rejected", trackRejected)}
                {renderPreviewBucket("Skipped", trackSkipped)}
              </div>
            </div>
          )}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Saved Submissions</h3>
            <span>{identityAuditSavedSubmissions?.total ?? 0}</span>
          </div>
          <div className="identity-audit-ambiguous-summary">
            <button
              className="secondary-button"
              disabled={identityAuditSavedSubmissionsLoading}
              onClick={() => void loadIdentityAuditSavedSubmissions(true)}
              type="button"
            >
              {identityAuditSavedSubmissionsLoading ? "Refreshing..." : "Refresh saved submissions"}
            </button>
          </div>
          {identityAuditSavedSubmissionsError ? <p className="empty-copy">{identityAuditSavedSubmissionsError}</p> : null}
          {!identityAuditSavedSubmissions && !identityAuditSavedSubmissionsError ? (
            <p className="empty-copy">{identityAuditSavedSubmissionsLoading ? "Loading saved submissions..." : "Saved submissions are not loaded yet."}</p>
          ) : null}
          {identityAuditSavedSubmissions && identityAuditSavedSubmissions.items.length === 0 ? (
            <p className="empty-copy">No saved submissions yet.</p>
          ) : null}
          {identityAuditSavedSubmissions && identityAuditSavedSubmissions.items.length > 0 ? (
            <div className="identity-audit-variant-list">
              {identityAuditSavedSubmissions.items.map((item) => (
                <div className="identity-audit-variant" key={`saved-submission-${item.id}`}>
                  <div className="identity-audit-variant-main">
                    <strong>#{item.id} • {item.status}</strong>
                    <span>{new Date(item.created_at).toLocaleString()}</span>
                    <span>
                      {Number(item.summary.total_decisions ?? 0)} decisions • {item.warnings_count} warnings • {item.unknown_groups} unknown groups • {item.unknown_tracks} unknown tracks
                    </span>
                  </div>
                  <div className="identity-audit-variant-stats">
                    <button className="secondary-button" onClick={() => void viewIdentityAuditSavedSubmission(item.id)} type="button">View</button>
                  </div>
                </div>
              ))}
            </div>
          ) : null}
          {identityAuditSavedSubmissionDetailError ? <p className="empty-copy">{identityAuditSavedSubmissionDetailError}</p> : null}
          {identityAuditSavedSubmissionDetailLoading ? <p className="empty-copy">Loading saved submission...</p> : null}
          {identityAuditSavedSubmissionDetail ? (
            <div>
              <div className="tracks-formula-heading">
                <h3>Saved Submission Detail</h3>
                <span>#{identityAuditSavedSubmissionDetail.item.id}</span>
              </div>
              <div className="identity-audit-ambiguous-summary">
                <button
                  className="secondary-button"
                  disabled={identityAuditSavedSubmissionDryRunLoading}
                  onClick={() => void dryRunIdentityAuditSavedSubmission(identityAuditSavedSubmissionDetail.item.id)}
                  type="button"
                >
                  {identityAuditSavedSubmissionDryRunLoading
                    ? "Running dry run..."
                    : identityAuditSavedSubmissionDryRun
                      ? "Re-run Dry Run"
                      : "Dry Run"}
                </button>
              </div>
              <p className="empty-copy">Dry run only. No changes applied.</p>
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Status</span><strong>{identityAuditSavedSubmissionDetail.item.status}</strong></span>
                <span className="identity-audit-stat"><span>Created</span><strong>{new Date(identityAuditSavedSubmissionDetail.item.created_at).toLocaleString()}</strong></span>
                <span className="identity-audit-stat"><span>Total</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.total_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Groups</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.group_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Tracks</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.track_decisions}</strong></span>
              </div>
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Approved</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.approved}</strong></span>
                <span className="identity-audit-stat"><span>Rejected</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.rejected}</strong></span>
                <span className="identity-audit-stat"><span>Skipped</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.skipped}</strong></span>
                <span className="identity-audit-stat"><span>Warnings</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.warnings}</strong></span>
              </div>
              {identityAuditSavedSubmissionDryRunError ? <p className="empty-copy">{identityAuditSavedSubmissionDryRunError}</p> : null}
              {identityAuditSavedSubmissionDryRun ? (
                <div className="identity-audit-group">
                  <div className="tracks-formula-heading">
                    <h3>Dry Run Result</h3>
                    <span>#{identityAuditSavedSubmissionDryRun.submission_id} • {identityAuditSavedSubmissionDryRun.status}</span>
                  </div>
                  {identityAuditSavedSubmissionDryRunAt ? (
                    <p className="empty-copy">Dry run at {new Date(identityAuditSavedSubmissionDryRunAt).toLocaleTimeString()}</p>
                  ) : null}
                  <div className="identity-audit-stats">
                    <span className="identity-audit-stat"><span>Would apply</span><strong>{identityAuditSavedSubmissionDryRun.summary.would_apply}</strong></span>
                    <span className="identity-audit-stat"><span>Approved groups</span><strong>{identityAuditSavedSubmissionDryRun.summary.approved_groups}</strong></span>
                    <span className="identity-audit-stat"><span>Approved tracks</span><strong>{identityAuditSavedSubmissionDryRun.summary.approved_tracks}</strong></span>
                    <span className="identity-audit-stat"><span>Rejected no-ops</span><strong>{identityAuditSavedSubmissionDryRun.summary.rejected}</strong></span>
                    <span className="identity-audit-stat"><span>Skipped no-ops</span><strong>{identityAuditSavedSubmissionDryRun.summary.skipped}</strong></span>
                  </div>
                  <div className="identity-audit-stats">
                    <span className="identity-audit-stat"><span>Warnings</span><strong>{identityAuditSavedSubmissionDryRun.summary.warnings}</strong></span>
                    <span className="identity-audit-stat"><span>Unknown groups</span><strong>{identityAuditSavedSubmissionDryRun.summary.unknown_groups}</strong></span>
                    <span className="identity-audit-stat"><span>Unknown tracks</span><strong>{identityAuditSavedSubmissionDryRun.summary.unknown_tracks}</strong></span>
                  </div>
                  {identityAuditSavedSubmissionDryRun.warnings.length > 0 ? (
                    <div className="identity-audit-variant-list">
                      {identityAuditSavedSubmissionDryRun.warnings.map((warning, index) => (
                        <div className="identity-audit-variant" key={`dry-run-warning-${index}`}>
                          <div className="identity-audit-variant-main">
                            <strong>Warning</strong>
                            <span>{warning}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : null}
                  <div className="identity-audit-group">
                    <div className="tracks-formula-heading">
                      <h3>Plan</h3>
                      <span>{identityAuditSavedSubmissionDryRun.plan.groups.length + identityAuditSavedSubmissionDryRun.plan.tracks.length} items</span>
                    </div>
                    {identityAuditSavedSubmissionDryRun.plan.groups.length === 0 && identityAuditSavedSubmissionDryRun.plan.tracks.length === 0 ? (
                      <p className="empty-copy">No plan items.</p>
                    ) : (
                      <div className="identity-audit-variant-list">
                        {identityAuditSavedSubmissionDryRun.plan.groups.map((item, index) => (
                          <div className="identity-audit-variant" key={`dry-run-group-${index}`}>
                            <div className="identity-audit-variant-main">
                              <strong>{String(item.label ?? item.id ?? "Group item")}</strong>
                              <span>{String(item.action ?? "would_accept_group")}</span>
                              <code>{String(item.decision_key ?? "")}</code>
                            </div>
                          </div>
                        ))}
                        {identityAuditSavedSubmissionDryRun.plan.tracks.map((item, index) => (
                          <div className="identity-audit-variant" key={`dry-run-track-${index}`}>
                            <div className="identity-audit-variant-main">
                              <strong>{String(item.label ?? item.id ?? "Track item")}</strong>
                              <span>{String(item.action ?? "would_accept_track_mapping")}</span>
                              <code>{String(item.decision_key ?? "")}</code>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Suggested Group Queue</h3>
            <span>{suggestedItems.length} groups</span>
          </div>
          {identityAuditSuggestedError ? <p className="empty-copy">{identityAuditSuggestedError}</p> : null}
          {!identityAuditSuggestedGroups && !identityAuditSuggestedError ? (
            <p className="empty-copy">{identityAuditSuggestedLoading ? "Loading suggested groups..." : "Suggested groups are not loaded yet."}</p>
          ) : null}
          {suggestedItems.length > 0 ? (
            <div className="identity-audit-examples">
              {suggestedItems.map((group) => {
                const decisionKey = groupDecisionKey(group);
                const decision = identityAuditLocalDecisions[decisionKey] ?? {
                  verdict: "unsure" as LocalReviewVerdict,
                  grouping_target: null,
                  note: "",
                  updated_at_ms: 0,
                };
                return (
                  <article className="identity-audit-example" key={`suggested-${group.analysis_track_id}`}>
                    <div className="identity-audit-example-header">
                      <div>
                        <h4>{group.analysis_track_name || `Analysis Track ${group.analysis_track_id}`}</h4>
                        <p>{group.match_method || "suggested"} | {Math.round(group.confidence * 100)}% confidence</p>
                      </div>
                      <span className="identity-audit-type-badge">Suggested group</span>
                    </div>
                    <div className="identity-audit-stats">
                      <span className="identity-audit-stat"><span>Release tracks</span><strong>{group.release_track_count}</strong></span>
                      {group.song_family_key ? <span className="identity-audit-stat"><span>Family key</span><strong>{group.song_family_key}</strong></span> : null}
                    </div>
                    <div className="identity-audit-review-controls">
                      <label>
                        Decision
                        <select
                          onChange={(event) => {
                            const nextVerdict = event.target.value as LocalReviewVerdict;
                            updateLocalReviewDecision(decisionKey, {
                              verdict: nextVerdict,
                              grouping_target: nextVerdict === "good_to_group" ? (decision.grouping_target ?? "same_composition") : null,
                            });
                          }}
                          value={decision.verdict}
                        >
                          <option value="unsure">Unreviewed</option>
                          <option value="good_to_group">Good to group</option>
                          <option value="not_good">Not good</option>
                          <option value="skipped">Skipped</option>
                        </select>
                      </label>
                      <label>
                        Grouping target
                        <select
                          disabled={decision.verdict !== "good_to_group"}
                          onChange={(event) =>
                            updateLocalReviewDecision(decisionKey, {
                              grouping_target: event.target.value as Exclude<LocalGroupingTarget, null>,
                            })}
                          value={decision.grouping_target ?? "same_composition"}
                        >
                          <option value="same_composition">Needs composition-level grouping</option>
                          <option value="same_release_track_only">Same release track only</option>
                        </select>
                      </label>
                    </div>
                    <label className="identity-audit-review-note">
                      Note
                      <textarea
                        onChange={(event) => updateLocalReviewDecision(decisionKey, { note: event.target.value })}
                        placeholder="Optional review context"
                        rows={2}
                        value={decision.note}
                      />
                    </label>
                    <div className="identity-audit-variant-list">
                      {group.release_tracks.map((releaseTrack) => (
                        <div className="identity-audit-variant" key={`group-${group.analysis_track_id}-${releaseTrack.release_track_id}`}>
                          <div className="identity-audit-variant-main">
                            <strong>{releaseTrack.release_track_name}</strong>
                            <span>{releaseTrack.primary_artists || "Unknown artists"}</span>
                            <code>release {releaseTrack.release_track_id}</code>
                          </div>
                          <div className="identity-audit-variant-stats">
                            {releaseTrack.album_names ? <span>{releaseTrack.album_names}</span> : null}
                          </div>
                        </div>
                      ))}
                    </div>
                  </article>
                );
              })}
            </div>
          ) : identityAuditSuggestedGroups ? (
            <p className="empty-copy">No suggested groups returned.</p>
          ) : null}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Ambiguous Track Queue</h3>
            <span>{filteredItems.length} rows</span>
          </div>
          {identityAuditAmbiguousError ? <p className="empty-copy">{identityAuditAmbiguousError}</p> : null}
          {!identityAuditAmbiguous && !identityAuditAmbiguousError ? (
            <p className="empty-copy">{identityAuditAmbiguousLoading ? "Loading ambiguous queue..." : "Ambiguous queue is not loaded yet."}</p>
          ) : null}
          {identityAuditAmbiguous?.parse_warning ? (
            <p className="empty-copy">Parser warning: {identityAuditAmbiguous.parse_warning}</p>
          ) : null}
          {visibleItems.length > 0 ? (
            <div className="identity-audit-examples">
              {visibleItems.map((item) => {
                const decision = identityAuditLocalDecisions[trackDecisionKey(item)] ?? {
                  verdict: "unsure" as LocalReviewVerdict,
                  grouping_target: null,
                  note: "",
                  updated_at_ms: 0,
                };
                return (
                  <article className="identity-audit-example" key={`ambiguous-${item.entry_id}`}>
                  <div className="identity-audit-example-header">
                    <div>
                      <h4>{item.release_track_name}</h4>
                      <p>{item.artist_name} | {item.bucket} | {item.analysis_name ?? "no analysis mapping"}</p>
                    </div>
                    <span className="identity-audit-type-badge">{item.dominant_family ?? "ambiguous"}</span>
                  </div>
                  <div className="identity-audit-stats">
                    <span className="identity-audit-stat"><span>release</span><strong>{item.release_track_id}</strong></span>
                    {item.confidence != null ? <span className="identity-audit-stat"><span>confidence</span><strong>{Math.round(item.confidence * 100)}%</strong></span> : null}
                    {item.song_family_key ? <span className="identity-audit-stat"><span>family key</span><strong>{item.song_family_key}</strong></span> : null}
                    {item.review_families.map((family) => (
                      <span className="identity-audit-stat" key={`${item.entry_id}-${family}`}><span>rule</span><strong>{family}</strong></span>
                    ))}
                  </div>
                  <div className="identity-audit-review-controls">
                    <label>
                      Decision
                      <select
                        onChange={(event) => {
                          const nextVerdict = event.target.value as LocalReviewVerdict;
                          updateLocalReviewDecision(trackDecisionKey(item), {
                            verdict: nextVerdict,
                            grouping_target: nextVerdict === "good_to_group" ? (decision.grouping_target ?? "same_composition") : null,
                          });
                        }}
                        value={decision.verdict}
                      >
                        <option value="unsure">Unreviewed</option>
                        <option value="good_to_group">Good to group</option>
                        <option value="not_good">Not good</option>
                        <option value="skipped">Skipped</option>
                      </select>
                    </label>
                    <label>
                      Grouping target
                      <select
                        disabled={decision.verdict !== "good_to_group"}
                        onChange={(event) =>
                          updateLocalReviewDecision(trackDecisionKey(item), {
                            grouping_target: event.target.value as Exclude<LocalGroupingTarget, null>,
                          })}
                        value={decision.grouping_target ?? "same_composition"}
                      >
                        <option value="same_composition">Needs composition-level grouping</option>
                        <option value="same_release_track_only">Same release track only</option>
                      </select>
                    </label>
                  </div>
                  <label className="identity-audit-review-note">
                    Note
                    <textarea
                      onChange={(event) => updateLocalReviewDecision(trackDecisionKey(item), { note: event.target.value })}
                      placeholder="Optional review context"
                      rows={2}
                      value={decision.note}
                    />
                  </label>
                </article>
                );
              })}
            </div>
          ) : identityAuditAmbiguous ? (
            <p className="empty-copy">No ambiguous rows match the current filters.</p>
          ) : null}
          {filteredItems.length > visibleItems.length ? (
            <div className="identity-audit-load-more-row">
              <button
                className="secondary-button"
                onClick={() => setIdentityAuditAmbiguousVisibleCount((current) => current + IDENTITY_AUDIT_AMBIGUOUS_VISIBLE_STEP)}
                type="button"
              >
                Show more ({filteredItems.length - visibleItems.length} remaining)
              </button>
            </div>
          ) : null}
        </div>
      </div>
    );
  }

  function renderIdentityAuditPage() {
    if (!profile) {
      return null;
    }
    const tabs: Array<{ value: IdentityAuditTab; label: string }> = [
      { value: "overview", label: "Overview" },
      { value: "canonical", label: "Canonical Splits" },
      { value: "release", label: "Release" },
      { value: "composition", label: "Composition Groups" },
      { value: "family", label: "Family" },
    ];

    return (
      <section className="info-card info-card-wide tracks-only-card" id="identity-audit-page">
        <div className="tracks-only-header">
          <div>
            <h2>Identity Audit</h2>
            <p className="tracks-only-subtitle">
              Review split-track examples and ambiguous rule families before any grouping behavior is promoted.
            </p>
          </div>
          <div className="section-column-header-actions">
            <button
              className="secondary-button tracks-page-link-button"
              disabled={identityAuditLoading || identityAuditSuggestedLoading || identityAuditAmbiguousLoading}
              onClick={() => {
                void loadIdentityAudit(true);
                void loadIdentityAuditSuggestedGroups(true);
                void loadIdentityAuditAmbiguousReview(true);
              }}
              type="button"
            >
              {(identityAuditLoading || identityAuditSuggestedLoading || identityAuditAmbiguousLoading) ? "Reloading..." : "Reload all"}
            </button>
            <button
              className="secondary-button tracks-only-back-button"
              onClick={() => setAppPage("dashboard")}
              type="button"
            >
              Back to dashboard
            </button>
          </div>
        </div>
        <div className="tracks-only-summary">
          <span>Identity samples: {identityAudit ? `${identityAudit.limit} per group` : "not loaded"}</span>
          <span>Suggested groups: {identityAuditSuggestedGroups?.summary.total_groups ?? 0}</span>
          <span>Ambiguous queue: {identityAuditAmbiguous?.summary.total_review_entries ?? 0}</span>
          {identityAuditLastLoadedAt ? <span>Identity loaded {new Date(identityAuditLastLoadedAt).toLocaleTimeString()}</span> : null}
          {identityAuditSuggestedLastLoadedAt ? <span>Suggested loaded {new Date(identityAuditSuggestedLastLoadedAt).toLocaleTimeString()}</span> : null}
          {identityAuditAmbiguousLastLoadedAt ? <span>Ambiguous loaded {new Date(identityAuditAmbiguousLastLoadedAt).toLocaleTimeString()}</span> : null}
        </div>
        <div className="track-ranking-toggle identity-audit-tabs" role="group" aria-label="Identity audit sections">
          {tabs.map((tab) => (
            <button
              className={`track-ranking-chip${identityAuditTab === tab.value ? " track-ranking-chip-active" : ""}`}
              key={`identity-tab-${tab.value}`}
              onClick={() => setIdentityAuditTab(tab.value)}
              type="button"
            >
              {tab.label}
            </button>
          ))}
        </div>
        {identityAuditTab === "overview" ? renderIdentityAuditOverviewTab() : null}
        {identityAuditTab === "canonical" ? renderIdentityAuditCanonicalTab() : null}
        {identityAuditTab === "release" ? renderIdentityAuditReleaseTab() : null}
        {identityAuditTab === "composition" ? renderIdentityAuditCompositionTab() : null}
        {identityAuditTab === "family" ? renderIdentityAuditAmbiguousTab() : null}
      </section>
    );
  }

  function renderCatalogBackfillPage() {
    if (!profile) {
      return null;
    }

    const latestDisplayRun = catalogBackfillLatestResult ?? catalogBackfillCoverage?.latest_run ?? null;
    const coveragePercent = typeof catalogBackfillCoverage?.track_duration_coverage_percent === "number"
      ? `${catalogBackfillCoverage.track_duration_coverage_percent.toFixed(2)}%`
      : "0.00%";
    const latestWarnings = Array.isArray((latestDisplayRun as { warnings?: unknown[] } | null)?.warnings)
      ? ((latestDisplayRun as { warnings?: string[] }).warnings ?? [])
      : [];
    const latestWarningsCount = latestWarnings.length > 0
      ? latestWarnings.length
      : (latestDisplayRun?.warnings_count ?? 0);
    const showLatestLastError = Boolean(latestDisplayRun?.last_error) && (latestDisplayRun?.status ?? "unknown") !== "ok";

    return (
      <section className="info-card info-card-wide tracks-only-card" id="catalog-backfill-page">
        <div className="tracks-only-header">
          <div>
            <h2>Catalog Backfill</h2>
            <p className="tracks-only-subtitle">Run and monitor Spotify catalog enrichment for static track and album metadata.</p>
            <p className="empty-copy">Catalog enrichment only. No identity mappings are changed.</p>
          </div>
          <div className="section-column-header-actions">
            <button
              className="secondary-button"
              disabled={catalogBackfillCoverageLoading || catalogBackfillRunsLoading || catalogBackfillQueueLoading}
              onClick={() => {
                void loadCatalogBackfillCoverage(true);
                void loadCatalogBackfillRuns(true);
                void loadCatalogBackfillQueue(true);
              }}
              type="button"
            >
              {(catalogBackfillCoverageLoading || catalogBackfillRunsLoading || catalogBackfillQueueLoading) ? "Refreshing..." : "Refresh all"}
            </button>
            <button
              className="secondary-button tracks-only-back-button"
              onClick={() => setAppPage("dashboard")}
              type="button"
            >
              Back to dashboard
            </button>
          </div>
        </div>

        <div className="tracks-only-summary">
          <span>Known release tracks: {catalogBackfillCoverage?.known_release_tracks ?? 0}</span>
          <span>Track catalog rows: {catalogBackfillCoverage?.track_catalog_rows ?? 0}</span>
          <span>Duration coverage: {catalogBackfillCoverage?.track_duration_coverage_count ?? 0} ({coveragePercent})</span>
          <span>Known release albums: {catalogBackfillCoverage?.known_release_albums ?? 0}</span>
          <span>Album catalog rows: {catalogBackfillCoverage?.album_catalog_rows ?? 0}</span>
          <span>Album track rows: {catalogBackfillCoverage?.album_track_rows ?? 0}</span>
          <span>Recent run errors: {catalogBackfillCoverage?.recent_errors_count ?? 0}</span>
          {catalogBackfillCoverageLastLoadedAt ? <span>Coverage loaded {new Date(catalogBackfillCoverageLastLoadedAt).toLocaleTimeString()}</span> : null}
        </div>

        <div className="info-card-body">
          <h3>Coverage</h3>
          {catalogBackfillCoverageError ? <p className="empty-copy">{catalogBackfillCoverageError}</p> : null}
          {!catalogBackfillCoverage && catalogBackfillCoverageLoading ? <p className="empty-copy">Loading coverage...</p> : null}
          {catalogBackfillCoverage?.latest_run ? (
            <p className="empty-copy">
              Latest run {catalogBackfillCoverage.latest_run.id}: {catalogBackfillCoverage.latest_run.status ?? "unknown"} | started{" "}
              {formatDebugTimestamp(catalogBackfillCoverage.latest_run.started_at)}
            </p>
          ) : (
            <p className="empty-copy">No catalog backfill runs recorded yet.</p>
          )}
        </div>

        <div className="info-card-body">
          <h3>Run Backfill</h3>
          <div className="identity-audit-ambiguous-toolbar">
            <label>
              Limit
              <input
                min={1}
                onChange={(event) => setCatalogBackfillLimit(Math.max(1, Number(event.target.value) || 1))}
                type="number"
                value={catalogBackfillLimit}
              />
            </label>
            <label>
              Offset
              <input
                min={0}
                onChange={(event) => setCatalogBackfillOffset(Math.max(0, Number(event.target.value) || 0))}
                type="number"
                value={catalogBackfillOffset}
              />
            </label>
            <label>
              Market
              <input
                onChange={(event) => setCatalogBackfillMarket(event.target.value.toUpperCase())}
                type="text"
                value={catalogBackfillMarket}
              />
            </label>
            <label>
              Request delay (s)
              <input
                min={0.2}
                onChange={(event) => setCatalogBackfillRequestDelaySeconds(Math.max(0.2, Number(event.target.value) || 0.2))}
                step={0.1}
                type="number"
                value={catalogBackfillRequestDelaySeconds}
              />
            </label>
            <label>
              Max runtime (s)
              <input
                max={300}
                min={5}
                onChange={(event) => setCatalogBackfillMaxRuntimeSeconds(Math.min(300, Math.max(5, Number(event.target.value) || 5)))}
                type="number"
                value={catalogBackfillMaxRuntimeSeconds}
              />
            </label>
            <label>
              Max requests
              <input
                max={1000}
                min={1}
                onChange={(event) => setCatalogBackfillMaxRequests(Math.min(1000, Math.max(1, Number(event.target.value) || 1)))}
                type="number"
                value={catalogBackfillMaxRequests}
              />
            </label>
            <label>
              Max errors
              <input
                max={100}
                min={1}
                onChange={(event) => setCatalogBackfillMaxErrors(Math.min(100, Math.max(1, Number(event.target.value) || 1)))}
                type="number"
                value={catalogBackfillMaxErrors}
              />
            </label>
            <label>
              Max album pages
              <input
                max={50}
                min={1}
                onChange={(event) => setCatalogBackfillMaxAlbumTracksPagesPerAlbum(Math.min(50, Math.max(1, Number(event.target.value) || 1)))}
                type="number"
                value={catalogBackfillMaxAlbumTracksPagesPerAlbum}
              />
            </label>
            <label>
              Album tracklist policy
              <select
                onChange={(event) =>
                  setCatalogBackfillAlbumTracklistPolicy(
                    (event.target.value as "all" | "priority_only" | "relevant_albums" | "none") ?? "relevant_albums",
                  )
                }
                value={catalogBackfillAlbumTracklistPolicy}
              >
                <option value="all">All</option>
                <option value="relevant_albums">Relevant albums</option>
                <option value="priority_only">Prioritized only</option>
                <option value="none">None</option>
              </select>
            </label>
            <label>
              Max 429 responses
              <input
                max={20}
                min={1}
                onChange={(event) => setCatalogBackfillMax429(Math.min(20, Math.max(1, Number(event.target.value) || 1)))}
                type="number"
                value={catalogBackfillMax429}
              />
            </label>
            <label className="recent-debug-filter">
              <input
                checked={catalogBackfillIncludeAlbums}
                onChange={(event) => setCatalogBackfillIncludeAlbums(event.currentTarget.checked)}
                type="checkbox"
              />
              Include albums
            </label>
            <label className="recent-debug-filter">
              <input
                checked={catalogBackfillForceRefresh}
                onChange={(event) => setCatalogBackfillForceRefresh(event.currentTarget.checked)}
                type="checkbox"
              />
              Force refresh
            </label>
            <button
              className="primary-button"
              disabled={catalogBackfillRunLoading}
              onClick={() => {
                void runCatalogBackfill();
              }}
              type="button"
            >
              {catalogBackfillRunLoading ? "Running..." : "Run Backfill"}
            </button>
          </div>
          <p className="empty-copy">
            Runs are bounded. If a stop limit is reached, partial catalog rows are kept and you can continue with the next
            offset.
          </p>
          {catalogBackfillRunError ? <p className="empty-copy">{catalogBackfillRunError}</p> : null}
        </div>

        <div className="info-card-body">
          <h3>Latest Run Result</h3>
          {!latestDisplayRun ? (
            <p className="empty-copy">Run backfill to see latest results.</p>
          ) : (
            <div className="tracks-only-summary">
              <span>Status: {latestDisplayRun.status ?? "unknown"}</span>
              <span>Partial: {latestDisplayRun.partial ? "yes" : "no"}</span>
              <span>Stop reason: {latestDisplayRun.stop_reason ?? "none"}</span>
              <span>Tracks seen/fetched/upserted: {latestDisplayRun.tracks_seen} / {latestDisplayRun.tracks_fetched} / {latestDisplayRun.tracks_upserted}</span>
              <span>Albums seen/fetched: {latestDisplayRun.albums_seen} / {latestDisplayRun.albums_fetched}</span>
              <span>Album tracks upserted: {latestDisplayRun.album_tracks_upserted}</span>
              <span>
                Album tracklists seen/skipped/fetched: {latestDisplayRun.album_tracklists_seen ?? 0} /{" "}
                {latestDisplayRun.album_tracklists_skipped_by_policy ?? 0} / {latestDisplayRun.album_tracklists_fetched ?? 0}
              </span>
              <span>Album tracklist policy: {latestDisplayRun.album_tracklist_policy ?? "all"}</span>
              <span>Errors: {latestDisplayRun.errors}</span>
              <span>Requests total: {latestDisplayRun.requests_total}</span>
              <span>Requests 429: {latestDisplayRun.requests_429}</span>
              <span>Max Retry-After: {latestDisplayRun.max_retry_after_seconds}</span>
              {"last_retry_after_seconds" in latestDisplayRun ? (
                <span>Last Retry-After: {latestDisplayRun.last_retry_after_seconds ?? 0}</span>
              ) : null}
              <span>Final delay: {latestDisplayRun.final_request_delay_seconds}</span>
              <span>Has more: {latestDisplayRun.has_more ? "yes" : "no"}</span>
              <span>Warnings: {latestWarningsCount}</span>
              {latestWarnings.length > 0 ? <span>Warning details: {latestWarnings.join(" | ")}</span> : null}
              {showLatestLastError ? <span>Last error: {latestDisplayRun.last_error}</span> : null}
            </div>
          )}
        </div>

        <div className="track-ranking-toggle" role="group" aria-label="Catalog backfill sections">
          <button
            className={`track-ranking-chip${catalogBackfillTab === "recentRuns" ? " track-ranking-chip-active" : ""}`}
            onClick={() => setCatalogBackfillTab("recentRuns")}
            type="button"
          >
            Recent Runs
          </button>
          <button
            className={`track-ranking-chip${catalogBackfillTab === "queue" ? " track-ranking-chip-active" : ""}`}
            onClick={() => setCatalogBackfillTab("queue")}
            type="button"
          >
            Queue
          </button>
        </div>

        {catalogBackfillTab === "recentRuns" ? (
          <div className="info-card-body">
            <div className="section-column-header">
              <h3>Recent Runs</h3>
              <button
                className="secondary-button"
                disabled={catalogBackfillRunsLoading}
                onClick={() => void loadCatalogBackfillRuns(true)}
                type="button"
              >
                {catalogBackfillRunsLoading ? "Refreshing..." : "Refresh"}
              </button>
            </div>
            {catalogBackfillRunsError ? <p className="empty-copy">{catalogBackfillRunsError}</p> : null}
            {!catalogBackfillRuns && catalogBackfillRunsLoading ? <p className="empty-copy">Loading recent runs...</p> : null}
            {catalogBackfillRunsLastLoadedAt ? (
              <p className="empty-copy">Runs loaded {new Date(catalogBackfillRunsLastLoadedAt).toLocaleTimeString()}</p>
            ) : null}
            {!catalogBackfillRuns || catalogBackfillRuns.items.length === 0 ? (
              <p className="empty-copy">No runs available.</p>
            ) : (
              <div className="recent-debug-grid">
                {catalogBackfillRuns.items.map((run) => (
                  <div className="recent-debug-row" key={`catalog-run-${run.id}`}>
                    {(() => {
                      const runWarnings = Array.isArray(run.warnings) ? run.warnings : [];
                      const runWarningsCount = runWarnings.length > 0 ? runWarnings.length : (run.warnings_count ?? 0);
                      const runLastError = (run.status ?? "unknown") === "ok" ? "none" : (run.last_error ?? "none");
                      return (
                        <>
                    <span className="recent-debug-key">
                      Run {run.id} | {formatDebugTimestamp(run.started_at)} {"->"} {formatDebugTimestamp(run.completed_at)}
                    </span>
                    <span className="recent-debug-value">
                      status={run.status ?? "unknown"}{run.status === "partial" ? " [PARTIAL/STOPPED]" : ""} | tracks={run.tracks_seen}/{run.tracks_fetched}/{run.tracks_upserted} | albums={run.albums_seen}/{run.albums_fetched} | album_tracks={run.album_tracks_upserted} | errors={run.errors} | requests_429={run.requests_429} | has_more={run.has_more ? "yes" : "no"} | warnings={runWarningsCount} | last_error={runLastError}
                    </span>
                    {runWarnings.length > 0 ? (
                      <span className="recent-debug-value">warning_details={runWarnings.join(" | ")}</span>
                    ) : null}
                        </>
                      );
                    })()}
                  </div>
                ))}
              </div>
            )}
          </div>
        ) : null}

        {catalogBackfillTab === "queue" ? (
          <div className="info-card-body">
            <div className="section-column-header">
              <h3>Queue</h3>
              <div className="section-column-header-actions">
                <label>
                  Status
                  <select
                    onChange={(event) => {
                      const nextFilter = (event.target.value as "all" | "pending" | "done" | "error");
                      setCatalogBackfillQueueStatusFilter(nextFilter);
                      void loadCatalogBackfillQueue(true, nextFilter);
                    }}
                    value={catalogBackfillQueueStatusFilter}
                  >
                    <option value="all">All</option>
                    <option value="pending">Pending</option>
                    <option value="done">Done</option>
                    <option value="error">Error</option>
                  </select>
                </label>
                <button
                  className="secondary-button"
                  disabled={catalogBackfillQueueRepairLoading}
                  onClick={() => {
                    void repairCatalogBackfillQueueStatuses();
                  }}
                  type="button"
                >
                  {catalogBackfillQueueRepairLoading ? "Repairing..." : "Repair queue statuses"}
                </button>
                <button
                  className="secondary-button"
                  disabled={catalogBackfillQueueLoading}
                  onClick={() => void loadCatalogBackfillQueue(true)}
                  type="button"
                >
                  {catalogBackfillQueueLoading ? "Refreshing..." : "Refresh"}
                </button>
              </div>
            </div>
            <div className="tracks-only-summary">
              <span>Pending: {catalogBackfillQueue?.counts.pending ?? 0}</span>
              <span>Done: {catalogBackfillQueue?.counts.done ?? 0}</span>
              <span>Error: {catalogBackfillQueue?.counts.error ?? 0}</span>
              <span>Total: {catalogBackfillQueue?.total ?? 0}</span>
            </div>
            {catalogBackfillQueueRepairMessage ? <p className="empty-copy">{catalogBackfillQueueRepairMessage}</p> : null}
            {catalogBackfillQueueError ? <p className="empty-copy">{catalogBackfillQueueError}</p> : null}
            {!catalogBackfillQueue && catalogBackfillQueueLoading ? <p className="empty-copy">Loading queue...</p> : null}
            {catalogBackfillQueueLastLoadedAt ? (
              <p className="empty-copy">Queue loaded {new Date(catalogBackfillQueueLastLoadedAt).toLocaleTimeString()}</p>
            ) : null}
            {!catalogBackfillQueue || catalogBackfillQueue.items.length === 0 ? (
              <p className="empty-copy">No queue items.</p>
            ) : (
              <div className="recent-debug-grid">
                {catalogBackfillQueue.items.map((item) => (
                  <div className="recent-debug-row" key={`catalog-queue-${item.id}`}>
                    <span className="recent-debug-key">
                      {item.entity_type}:{item.spotify_id} | status={item.status} | priority={item.priority} | attempts={item.attempts}
                    </span>
                    <span className="recent-debug-value">
                      requested={formatDebugTimestamp(item.requested_at)} | last_attempted={formatDebugTimestamp(item.last_attempted_at)} | reason={item.reason ?? "none"} | last_error={item.last_error ?? "none"}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        ) : null}
      </section>
    );
  }

  function renderSearchLookupPage() {
    if (!profile) {
      return null;
    }

    const isAlbumsLookup = searchLookupEntityType === "albums";
    const isDuplicateAlbumsLookup = searchLookupEntityType === "duplicate_albums";
    const visibleAlbumItems = albumCatalogLookupResult?.items ?? [];
    const visibleTrackItems = trackCatalogLookupResult?.items ?? [];
    const visibleIncompleteAlbumIds = Array.from(
      new Set(
        visibleAlbumItems
          .filter((item) => albumLookupRowCanBulkPrioritize(item))
          .map((item) => item.spotify_album_id)
          .filter((spotifyAlbumId): spotifyAlbumId is string => Boolean(spotifyAlbumId)),
      ),
    );
    const visibleIncompleteTrackIds = Array.from(
      new Set(
        visibleTrackItems
          .filter((item) => trackLookupRowCanBulkPrioritize(item))
          .map((item) => item.spotify_track_id)
          .filter((spotifyTrackId): spotifyTrackId is string => Boolean(spotifyTrackId)),
      ),
    );
    const statusBadgeColors: Record<string, { background: string; color: string; border: string }> = {
      Complete: { background: "#e8f7ee", color: "#1c6b3d", border: "#bfe7cf" },
      "Missing metadata": { background: "#fff7e6", color: "#8a5b00", border: "#f1ddb0" },
      "Missing duration": { background: "#fff7e6", color: "#8a5b00", border: "#f1ddb0" },
      "Tracklist incomplete": { background: "#fff3e8", color: "#8a4a1f", border: "#efd0b9" },
      "Not queued": { background: "#f2f3f5", color: "#4f5663", border: "#d8dbe1" },
      Pending: { background: "#e7f0ff", color: "#2252a3", border: "#c1d5ff" },
      Done: { background: "#e9f7ee", color: "#1f6f40", border: "#c4e9d2" },
      Error: { background: "#fdecec", color: "#9a1f1f", border: "#f2c3c3" },
    };
    const renderLookupStatusBadge = (label: string) => {
      const colors = statusBadgeColors[label] ?? { background: "#f2f2f2", color: "#3a3a3a", border: "#d9d9d9" };
      return (
        <span
          style={{
            display: "inline-block",
            padding: "2px 8px",
            borderRadius: "999px",
            fontSize: "12px",
            fontWeight: 600,
            whiteSpace: "nowrap",
            background: colors.background,
            color: colors.color,
            border: `1px solid ${colors.border}`,
          }}
        >
          {label}
        </span>
      );
    };

    return (
      <section className="info-card info-card-wide tracks-only-card" id="search-lookup-page">
        <div className="tracks-only-header">
          <div>
            <h2>Search / Lookup</h2>
            <p className="tracks-only-subtitle">Read-only lookup tools for catalog and enrichment status.</p>
          </div>
          <div className="section-column-header-actions">
            <button className="secondary-button tracks-only-back-button" onClick={() => setAppPage("dashboard")} type="button">
              Back to dashboard
            </button>
          </div>
        </div>

        <div className="info-card-body">
          <h3>{searchLookupEntityType === "albums" ? "Album Catalog Lookup" : searchLookupEntityType === "tracks" ? "Track Catalog Lookup" : "Duplicate Albums"}</h3>
          <div className="identity-audit-ambiguous-toolbar">
            <div className="track-ranking-toggle" role="group" aria-label="Lookup type">
              <button
                className={`track-ranking-chip${isAlbumsLookup ? " track-ranking-chip-active" : ""}`}
                onClick={() => {
                  setSearchLookupEntityType("albums");
                  setAlbumCatalogLookupEnqueueError("");
                  setAlbumCatalogLookupEnqueueResult(null);
                }}
                type="button"
              >
                Albums
              </button>
              <button
                className={`track-ranking-chip${searchLookupEntityType === "tracks" ? " track-ranking-chip-active" : ""}`}
                onClick={() => {
                  setSearchLookupEntityType("tracks");
                  setAlbumCatalogLookupEnqueueError("");
                  setAlbumCatalogLookupEnqueueResult(null);
                }}
                type="button"
              >
                Tracks
              </button>
              <button
                className={`track-ranking-chip${isDuplicateAlbumsLookup ? " track-ranking-chip-active" : ""}`}
                onClick={() => {
                  setSearchLookupEntityType("duplicate_albums");
                  setAlbumCatalogLookupEnqueueError("");
                  setAlbumCatalogLookupEnqueueResult(null);
                }}
                type="button"
              >
                Duplicate Albums
              </button>
            </div>
            <label>
              Query
              <input
                onChange={(event) => setAlbumCatalogLookupQ(event.target.value)}
                placeholder={isAlbumsLookup ? "Album, artist, or Spotify album id" : isDuplicateAlbumsLookup ? "Not used for duplicate view" : "Track, artist, album, or Spotify track id"}
                type="text"
                value={albumCatalogLookupQ}
                disabled={isDuplicateAlbumsLookup}
              />
            </label>
            <label>
              Catalog status
              {isDuplicateAlbumsLookup ? (
                <select disabled value="all">
                  <option value="all">all</option>
                </select>
              ) : isAlbumsLookup ? (
                <select
                  onChange={(event) => setAlbumCatalogLookupStatus(event.target.value as "all" | "backfilled" | "not_backfilled" | "tracklist_complete" | "tracklist_incomplete" | "error")}
                  value={albumCatalogLookupStatus}
                >
                  <option value="all">all</option>
                  <option value="backfilled">backfilled</option>
                  <option value="not_backfilled">not_backfilled</option>
                  <option value="tracklist_complete">tracklist_complete</option>
                  <option value="tracklist_incomplete">tracklist_incomplete</option>
                  <option value="error">error</option>
                </select>
              ) : (
                <select
                  onChange={(event) => setTrackCatalogLookupStatus(event.target.value as "all" | "backfilled" | "not_backfilled" | "duration_missing" | "error")}
                  value={trackCatalogLookupStatus}
                >
                  <option value="all">all</option>
                  <option value="backfilled">backfilled</option>
                  <option value="not_backfilled">not_backfilled</option>
                  <option value="duration_missing">duration_missing</option>
                  <option value="error">error</option>
                </select>
              )}
            </label>
            <label>
              Queue status
              <select
                onChange={(event) => setSearchLookupQueueStatus(event.target.value as "all" | "not_queued" | "pending" | "done" | "error")}
                value={searchLookupQueueStatus}
                disabled={isDuplicateAlbumsLookup}
              >
                <option value="all">All queue states</option>
                <option value="not_queued">Not queued</option>
                <option value="pending">Pending</option>
                <option value="done">Done</option>
                <option value="error">Error</option>
              </select>
            </label>
            <label>
              Sort
              <select
                onChange={(event) => setSearchLookupSort(event.target.value as "default" | "recently_backfilled" | "name" | "incomplete_first")}
                value={searchLookupSort}
                disabled={isDuplicateAlbumsLookup}
              >
                <option value="default">Default</option>
                <option value="recently_backfilled">Recently backfilled</option>
                <option value="name">Name</option>
                <option value="incomplete_first">Incomplete first</option>
              </select>
            </label>
            <button
              className="primary-button"
              disabled={isDuplicateAlbumsLookup ? albumDuplicateLookupLoading : isAlbumsLookup ? albumCatalogLookupLoading : trackCatalogLookupLoading}
              onClick={() => {
                void loadActiveSearchLookup(true);
              }}
              type="button"
            >
              {isDuplicateAlbumsLookup ? (albumDuplicateLookupLoading ? "Loading..." : "Refresh") : isAlbumsLookup ? (albumCatalogLookupLoading ? "Searching..." : "Search") : (trackCatalogLookupLoading ? "Searching..." : "Search")}
            </button>
            <button
              className="secondary-button"
              disabled={isDuplicateAlbumsLookup || albumCatalogLookupEnqueueLoading || (isAlbumsLookup ? visibleIncompleteAlbumIds.length === 0 : visibleIncompleteTrackIds.length === 0)}
              onClick={() => {
                if (isAlbumsLookup) {
                  void enqueueVisibleIncompleteLookupAlbums();
                } else {
                  void enqueueVisibleIncompleteLookupTracks();
                }
              }}
              type="button"
            >
              {albumCatalogLookupEnqueueLoading
                ? "Prioritizing..."
                : isAlbumsLookup
                  ? "Prioritize visible incomplete albums"
                  : "Prioritize visible incomplete tracks"}
            </button>
          </div>
          {isAlbumsLookup && albumCatalogLookupResult ? (
            <p className="empty-copy">Visible incomplete albums with Spotify IDs: {visibleIncompleteAlbumIds.length}</p>
          ) : null}
          {isDuplicateAlbumsLookup && albumDuplicateLookupResult ? (
            <p className="empty-copy">Duplicate Spotify album groups: {albumDuplicateLookupResult.total}</p>
          ) : null}
          {searchLookupEntityType === "tracks" && trackCatalogLookupResult ? (
            <p className="empty-copy">Visible incomplete tracks with Spotify IDs: {visibleIncompleteTrackIds.length}</p>
          ) : null}
          <p className="empty-copy">Catalog status shows what data exists. Queue status shows whether backfill work is scheduled.</p>
          <p className="empty-copy">Prioritized items are added to the catalog backfill queue. They are not fetched immediately.</p>
          {albumCatalogLookupError ? <p className="empty-copy">{albumCatalogLookupError}</p> : null}
          {trackCatalogLookupError ? <p className="empty-copy">{trackCatalogLookupError}</p> : null}
          {albumDuplicateLookupError ? <p className="empty-copy">{albumDuplicateLookupError}</p> : null}
          {albumCatalogLookupEnqueueError ? <p className="empty-copy">{albumCatalogLookupEnqueueError}</p> : null}
          {albumCatalogLookupEnqueueResult ? (
            <p className="empty-copy">
              Added {albumCatalogLookupEnqueueResult.enqueued}, updated {albumCatalogLookupEnqueueResult.updated}, already complete {albumCatalogLookupEnqueueResult.already_complete}, invalid {albumCatalogLookupEnqueueResult.invalid}.
            </p>
          ) : null}
          {isAlbumsLookup && !albumCatalogLookupResult && albumCatalogLookupLoading ? <p className="empty-copy">Loading albums...</p> : null}
          {searchLookupEntityType === "tracks" && !trackCatalogLookupResult && trackCatalogLookupLoading ? <p className="empty-copy">Loading tracks...</p> : null}
          {isDuplicateAlbumsLookup && !albumDuplicateLookupResult && albumDuplicateLookupLoading ? <p className="empty-copy">Loading duplicate album groups...</p> : null}
          {isAlbumsLookup && albumCatalogLookupLastLoadedAt ? (
            <p className="empty-copy">Albums loaded {new Date(albumCatalogLookupLastLoadedAt).toLocaleTimeString()}</p>
          ) : null}
          {searchLookupEntityType === "tracks" && trackCatalogLookupLastLoadedAt ? (
            <p className="empty-copy">Tracks loaded {new Date(trackCatalogLookupLastLoadedAt).toLocaleTimeString()}</p>
          ) : null}
          {isDuplicateAlbumsLookup && albumDuplicateLookupLastLoadedAt ? (
            <p className="empty-copy">Duplicate groups loaded {new Date(albumDuplicateLookupLastLoadedAt).toLocaleTimeString()}</p>
          ) : null}
          {isAlbumsLookup && (!albumCatalogLookupResult || albumCatalogLookupResult.items.length === 0) ? (
            <p className="empty-copy">No matching albums.</p>
          ) : null}
          {isAlbumsLookup && albumCatalogLookupResult && albumCatalogLookupResult.items.length > 0 ? (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Album</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Artist</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Album</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Tracklist</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Status</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Queue</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Last Updated</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {albumCatalogLookupResult.items.map((item) => {
                    const tracklistText = `${item.album_track_rows} / ${item.total_tracks ?? "?"} tracks`;
                    const statusLabel = albumLookupStatusLabel(item);
                    const queueLabel = queueStatusLabel(item.queue_status);
                    const canPrioritize = albumLookupRowIsIncompleteForEnqueue(item);
                    const isPendingQueue = rowIsPendingQueue(item.queue_status);
                    const actionLabel = isPendingQueue ? "Prioritized" : (String(item.queue_status).toLowerCase() === "error" ? "Retry priority" : "Prioritize");
                    return (
                      <>
                        <tr key={`album-lookup-${item.release_album_id}`}>
                          <td style={{ padding: "8px", verticalAlign: "top", fontWeight: 600 }}>
                            {item.spotify_album_id ? (
                              <button
                                className="jump-link"
                                onClick={() => openAlbumLookupPreview(item)}
                                type="button"
                              >
                                {item.release_album_name}
                              </button>
                            ) : (
                              item.release_album_name
                            )}
                          </td>
                          <td style={{ padding: "8px", verticalAlign: "top" }}>{item.artist_name}</td>
                          <td style={{ padding: "8px", verticalAlign: "top", wordBreak: "break-word" }}>{item.spotify_album_id ?? "None"}</td>
                          <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{tracklistText}</td>
                          <td style={{ padding: "8px", verticalAlign: "top" }}>{renderLookupStatusBadge(statusLabel)}</td>
                          <td style={{ padding: "8px", verticalAlign: "top" }}>{renderLookupStatusBadge(queueLabel)}</td>
                          <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{formatDebugTimestamp(item.catalog_fetched_at)}</td>
                          <td style={{ padding: "8px", verticalAlign: "top" }}>
                            {canPrioritize ? (
                              <button
                                className="secondary-button"
                                disabled={albumCatalogLookupEnqueueLoading || isPendingQueue}
                                onClick={() => {
                                  void enqueueVisibleIncompleteLookupAlbums([item]);
                                }}
                                type="button"
                              >
                                {albumCatalogLookupEnqueueLoading ? "Prioritizing..." : actionLabel}
                              </button>
                            ) : null}
                          </td>
                        </tr>
                        {item.catalog_last_error ? (
                          <tr key={`album-lookup-error-${item.release_album_id}`}>
                            <td colSpan={8} style={{ padding: "0 8px 8px 8px", color: "rgba(0, 0, 0, 0.65)", fontSize: "12px", wordBreak: "break-word" }}>
                              Error detail: {item.catalog_last_error}
                            </td>
                          </tr>
                        ) : null}
                      </>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : null}
          {searchLookupEntityType === "tracks" && (!trackCatalogLookupResult || trackCatalogLookupResult.items.length === 0) ? (
            <p className="empty-copy">No matching tracks.</p>
          ) : null}
          {isDuplicateAlbumsLookup && (!albumDuplicateLookupResult || albumDuplicateLookupResult.items.length === 0) ? (
            <p className="empty-copy">No duplicate albums found.</p>
          ) : null}
          {searchLookupEntityType === "tracks" && trackCatalogLookupResult && trackCatalogLookupResult.items.length > 0 ? (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Track</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Artist</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Album</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Track</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Duration</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Status</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Queue</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Last Updated</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {trackCatalogLookupResult.items.map((item) => {
                    const statusLabel = trackLookupStatusLabel(item);
                    const queueLabel = queueStatusLabel(item.queue_status);
                    const canPrioritize = trackLookupRowIsIncompleteForEnqueue(item);
                    const isPendingQueue = rowIsPendingQueue(item.queue_status);
                    const actionLabel = isPendingQueue ? "Prioritized" : (String(item.queue_status).toLowerCase() === "error" ? "Retry priority" : "Prioritize");
                    return (
                      <>
                        <tr key={`track-lookup-${item.release_track_id}`}>
                          <td style={{ padding: "8px", verticalAlign: "top", fontWeight: 600 }}>{item.release_track_name}</td>
                          <td style={{ padding: "8px", verticalAlign: "top" }}>{item.artist_name}</td>
                          <td style={{ padding: "8px", verticalAlign: "top" }}>{item.release_album_name}</td>
                          <td style={{ padding: "8px", verticalAlign: "top", wordBreak: "break-word" }}>{item.spotify_track_id ?? "None"}</td>
                          <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{item.duration_display ?? "Unknown"}</td>
                          <td style={{ padding: "8px", verticalAlign: "top" }}>{renderLookupStatusBadge(statusLabel)}</td>
                          <td style={{ padding: "8px", verticalAlign: "top" }}>{renderLookupStatusBadge(queueLabel)}</td>
                          <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{formatDebugTimestamp(item.catalog_fetched_at)}</td>
                          <td style={{ padding: "8px", verticalAlign: "top" }}>
                            {canPrioritize ? (
                              <button
                                className="secondary-button"
                                disabled={albumCatalogLookupEnqueueLoading || isPendingQueue}
                                onClick={() => {
                                  void enqueueVisibleIncompleteLookupTracks([item]);
                                }}
                                type="button"
                              >
                                {albumCatalogLookupEnqueueLoading ? "Prioritizing..." : actionLabel}
                              </button>
                            ) : null}
                          </td>
                        </tr>
                        {item.catalog_last_error ? (
                          <tr key={`track-lookup-error-${item.release_track_id}`}>
                            <td colSpan={9} style={{ padding: "0 8px 8px 8px", color: "rgba(0, 0, 0, 0.65)", fontSize: "12px", wordBreak: "break-word" }}>
                              Error detail: {item.catalog_last_error}
                            </td>
                          </tr>
                        ) : null}
                      </>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : null}
          {isDuplicateAlbumsLookup && albumDuplicateLookupResult && albumDuplicateLookupResult.items.length > 0 ? (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Album</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Name</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Duplicate Count</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Release Album</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Artist</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Tracklist</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Catalog</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Queue</th>
                  </tr>
                </thead>
                <tbody>
                  {albumDuplicateLookupResult.items.map((group) =>
                    group.release_albums.map((item, index) => (
                      <tr key={`duplicate-album-${group.spotify_album_id}-${item.release_album_id}`}>
                        <td style={{ padding: "8px", verticalAlign: "top", wordBreak: "break-word" }}>{index === 0 ? group.spotify_album_id : ""}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{index === 0 ? (group.spotify_album_name ?? "Unknown") : ""}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{index === 0 ? group.duplicate_count : ""}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", fontWeight: 600 }}>{item.release_album_name}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{item.artist_name}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{item.album_track_rows} / {item.total_tracks ?? "?"} tracks</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{item.catalog_status ?? "unknown"}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{item.queue_status}</td>
                      </tr>
                    )),
                  )}
                </tbody>
              </table>
            </div>
          ) : null}
        </div>
      </section>
    );
  }

  function formatDurationMs(value: unknown): string {
    if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
      return "Unknown";
    }
    const totalSeconds = Math.round(value / 1000);
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return `${minutes}:${String(seconds).padStart(2, "0")}`;
  }

  function formatDebugTimestamp(value: unknown): string {
    if (typeof value !== "string" || !value) {
      return "Unknown";
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      return value;
    }
    return parsed.toLocaleString();
  }

  function formatTimeOnly(value: unknown): string {
    if (typeof value !== "string" || !value) {
      return "Unknown";
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      return value;
    }
    return parsed.toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" });
  }

  function trackEstimatedMs(track: RecentTrack): number | null {
    if (typeof track.estimated_played_ms === "number" && Number.isFinite(track.estimated_played_ms)) {
      return Math.max(0, track.estimated_played_ms);
    }
    if (typeof track.duration_ms === "number" && Number.isFinite(track.duration_ms)) {
      return Math.max(0, track.duration_ms);
    }
    return null;
  }

  function formatDebugLabel(key: string): string {
    if (key === "spotify_played_at") {
      return "Played at";
    }
    if (key === "played_at_gap_ms") {
      return "Gap to previous play";
    }
    return key
      .replace(/_/g, " ")
      .replace(/\b\w/g, (letter) => letter.toUpperCase());
  }

  function isLinkOrUriField(key: string, value: unknown): boolean {
    if (key.toLowerCase().includes("url") || key.toLowerCase().includes("uri") || key.toLowerCase().includes("href")) {
      return true;
    }
    if (typeof value === "string" && /^https?:\/\//i.test(value)) {
      return true;
    }
    return false;
  }

  function isComputedField(key: string): boolean {
    if (key.startsWith("estimated_")) {
      return true;
    }
    return [
      "played_at_gap_ms",
      "duration_seconds",
      "spotify_played_at_unix_ms",
      "play_count",
      "all_time_play_count",
      "recent_play_count",
      "first_played_at",
      "last_played_at",
      "listening_span_days",
      "listening_span_years",
      "active_months_count",
      "span_months_count",
      "consistency_ratio",
      "longevity_score",
      "estimated_completion_ratio",
    ].includes(key);
  }

  function formatDebugValue(key: string, value: unknown): string {
    if (value === null) {
      return "null";
    }
    if (value === undefined) {
      return "undefined";
    }
    if (key === "artists" && Array.isArray(value)) {
      const names = value
        .map((artist) => (artist && typeof artist === "object" ? (artist as { name?: unknown }).name : null))
        .filter((name): name is string => typeof name === "string" && name.length > 0);
      return names.length > 0 ? names.join(", ") : "[]";
    }
    if (key.endsWith("_at")) {
      return formatDebugTimestamp(value);
    }
    if (key === "duration_ms" || key === "estimated_played_ms") {
      return formatDurationMs(value);
    }
    if (key === "played_at_gap_ms") {
      return formatDurationMs(value);
    }
    if (key === "duration_seconds" || key === "estimated_played_seconds") {
      return typeof value === "number" ? formatDurationMs(value * 1000) : String(value);
    }
    if (key === "estimated_completion_ratio" || key === "consistency_ratio") {
      return typeof value === "number" ? `${(value * 100).toFixed(1)}%` : String(value);
    }
    if (typeof value === "string") {
      return value.length > 0 ? value : '""';
    }
    if (typeof value === "object") {
      try {
        return JSON.stringify(value);
      } catch {
        return "[unserializable value]";
      }
    }
    return String(value);
  }

  function trackPlayedAtMs(track: RecentTrack): number | null {
    if (!track.spotify_played_at) {
      return null;
    }
    const parsed = new Date(track.spotify_played_at);
    if (Number.isNaN(parsed.getTime())) {
      return null;
    }
    return parsed.getTime();
  }

  type DebugSession = {
    id: string;
    tracks: RecentTrack[];
    startedAt: number | null;
    endedAt: number | null;
  };

  function buildDebugSessions(tracks: RecentTrack[]): DebugSession[] {
    const sortedTracks = [...tracks].sort((a, b) => {
      const aMs = trackPlayedAtMs(a) ?? -1;
      const bMs = trackPlayedAtMs(b) ?? -1;
      return bMs - aMs;
    });
    const sessions: DebugSession[] = [];
    let currentTracks: RecentTrack[] = [];
    let previousTrackMs: number | null = null;

    for (const track of sortedTracks) {
      const currentTrackMs = trackPlayedAtMs(track);
      const startsNewSession = (
        currentTracks.length > 0
        && (
          currentTrackMs == null
          || previousTrackMs == null
          || previousTrackMs - currentTrackMs > DEBUG_SESSION_BREAK_MS
          || previousTrackMs < currentTrackMs
        )
      );

      if (startsNewSession) {
        const allTimes = currentTracks
          .map((sessionTrack) => trackPlayedAtMs(sessionTrack))
          .filter((value): value is number => value != null);
        const startedAt = allTimes.length > 0 ? Math.max(...allTimes) : null;
        const endedAt = allTimes.length > 0 ? Math.min(...allTimes) : null;
        sessions.push({
          id: `session-${sessions.length + 1}-${startedAt ?? "na"}-${endedAt ?? "na"}`,
          tracks: currentTracks,
          startedAt,
          endedAt,
        });
        currentTracks = [];
      }

      currentTracks.push(track);
      previousTrackMs = currentTrackMs;
    }

    if (currentTracks.length > 0) {
      const allTimes = currentTracks
        .map((sessionTrack) => trackPlayedAtMs(sessionTrack))
        .filter((value): value is number => value != null);
      const startedAt = allTimes.length > 0 ? Math.max(...allTimes) : null;
      const endedAt = allTimes.length > 0 ? Math.min(...allTimes) : null;
      sessions.push({
        id: `session-${sessions.length + 1}-${startedAt ?? "na"}-${endedAt ?? "na"}`,
        tracks: currentTracks,
        startedAt,
        endedAt,
      });
    }

    return sessions;
  }

  function formatSessionRange(session: DebugSession): string {
    if (session.startedAt == null || session.endedAt == null) {
      return "Time range unavailable";
    }
    const earlierMs = Math.min(session.startedAt, session.endedAt);
    const laterMs = Math.max(session.startedAt, session.endedAt);
    const earlier = new Date(earlierMs);
    const later = new Date(laterMs);
    const sameLocalDate = (
      earlier.getFullYear() === later.getFullYear()
      && earlier.getMonth() === later.getMonth()
      && earlier.getDate() === later.getDate()
    );
    if (sameLocalDate) {
      const dateText = earlier.toLocaleDateString();
      const startTime = earlier.toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" });
      const endTime = later.toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" });
      return `${dateText} ${startTime} - ${endTime}`;
    }
    const startText = earlier.toLocaleString();
    const endText = later.toLocaleString();
    return `${startText} - ${endText}`;
  }

  function debugTrackKey(sessionId: string, track: RecentTrack, index: number): string {
    return `${sessionId}-${track.spotify_played_at ?? "na"}-${track.track_id ?? "no-id"}-${index}`;
  }

  function renderRecentDebugPage() {
    if (!profile) {
      return null;
    }

    const trackKey = (track: RecentTrack): string => [
      track.event_id ?? "no-event-id",
      track.spotify_played_at ?? "na",
      track.track_id ?? "no-id",
      track.uri ?? "no-uri",
      track.track_name ?? "no-track",
      track.artist_name ?? "no-artist",
    ].join("||");

    const allSortedTracks = [...listeningLogTracks].sort((a, b) => {
      const aMs = trackPlayedAtMs(a) ?? -1;
      const bMs = trackPlayedAtMs(b) ?? -1;
      return bMs - aMs;
    });
    const visibleTracks = allSortedTracks;
    const sessions = buildDebugSessions(visibleTracks);
    const canTryLoadMore = listeningLogOffset === 0 || listeningLogHasMore;
    const buildSpotifyUrl = (kind: "track" | "artist" | "album", id: string | null): string =>
      id ? `https://open.spotify.com/${kind}/${id}` : "";
    const firstArtist = (track: RecentTrack) => track.artists?.find((artist) => Boolean(artist?.name || artist?.id || artist?.artist_id)) ?? null;
    const openDebugPreview = (track: RecentTrack, kind: PreviewItem["kind"]) => {
      const artist = firstArtist(track);
      const artistLabel = artist?.name ?? track.artist_name ?? "Unknown artist";
      const albumLabel = track.album_name ?? "Unknown album";
      const releaseYear = track.album_release_year ?? null;

      if (kind === "track") {
        const fallbackTrackUrl = buildSpotifyUrl("track", track.track_id ?? null);
        setSelectedPreview({
          image: track.image_url ?? null,
          fallbackLabel: "T",
          label: track.track_name ?? "Unknown track",
          meta: track.artist_name ?? null,
          detail: track.album_name ?? null,
          kind: "track",
          entityId: track.track_id ?? null,
          trackUri: trackUriWithFallback(track.uri, track.track_id),
          url: track.url ?? fallbackTrackUrl,
          trackId: track.track_id ?? null,
          albumId: track.album_id ?? null,
          artistName: track.artist_name ?? null,
          sourceTrack: track,
        });
        return;
      }

      if (kind === "artist") {
        const artistId = artist?.artist_id ?? artist?.id ?? null;
        const fallbackArtistUrl = buildSpotifyUrl("artist", artistId);
        setSelectedPreview({
          image: track.image_url ?? null,
          fallbackLabel: "A",
          label: artistLabel,
          meta: null,
          detail: null,
          kind: "artist",
          entityId: artistId,
          trackUri: null,
          url: artist?.url ?? fallbackArtistUrl,
          trackId: null,
          albumId: null,
          artistName: artistLabel,
          sourceTrack: track,
        });
        return;
      }

      const fallbackAlbumUrl = buildSpotifyUrl("album", track.album_id ?? null);
      setSelectedPreview({
        image: track.image_url ?? null,
        fallbackLabel: "L",
        label: albumLabel,
        meta: track.artist_name ?? null,
        detail: releaseYear,
        kind: "album",
        entityId: track.album_id ?? null,
        trackUri: null,
        url: track.album_url ?? fallbackAlbumUrl,
        trackId: null,
        albumId: track.album_id ?? null,
        artistName: track.artist_name ?? null,
        sourceTrack: track,
      });
    };

    const debugFieldOrder = [
      "event_id",
      "source_label",
      "raw_spotify_recent_id",
      "raw_spotify_history_id",
      "timing_source",
      "matched_state",
      "track_id",
      "track_name",
      "artist_name",
      "album_name",
      "album_release_year",
      "album_id",
      "artists",
      "spotify_played_at",
      "duration_ms",
      "estimated_played_ms",
      "estimated_completion_ratio",
      "spotify_context_type",
      "spotify_context_uri",
      "spotify_context_url",
      "spotify_context_href",
    ];

    return (
      <section className="info-card info-card-wide tracks-only-card" id="recent-debug-page">
        <div className="tracks-only-header">
          <div className="section-column-header">
            <div>
              <h2>Listening Log</h2>
              <p className="tracks-only-subtitle">
                Canonical play events from the merged fact layer.
              </p>
            </div>
            <div className="recent-debug-controls">
              {renderRecentDebugSourceFilterToggle()}
              <button
                className="secondary-button"
                disabled={listeningLogLoading}
                onClick={() => void loadListeningLogBatch(true)}
                type="button"
              >
                {listeningLogLoading ? "Loading..." : "Reload log"}
              </button>
              <label className="recent-debug-filter">
                <input
                  checked={showDebugLinkFields}
                  onChange={(event) => setShowDebugLinkFields(event.currentTarget.checked)}
                  type="checkbox"
                />
                Show raw data
              </label>
            </div>
          </div>
          <button
            className="secondary-button tracks-only-back-button"
            onClick={() => setAppPage("dashboard")}
            type="button"
          >
            Back to activity
          </button>
        </div>
        <div className="tracks-only-diagnostics">
          <span>{visibleTracks.length} visible play events</span>
          {listeningLogLastLoadedAt ? (
            <span>Loaded {new Date(listeningLogLastLoadedAt).toLocaleTimeString()}</span>
          ) : null}
        </div>
        {listeningLogError ? (
          <p className="empty-copy">
            {listeningLogError}
            {" "}
            Refresh this page after confirming the frontend is using the same backend where `/auth/session` is authenticated.
          </p>
        ) : null}
        {visibleTracks.length === 0 && listeningLogLoading ? (
          <p className="empty-copy">Loading listening log...</p>
        ) : null}
        {visibleTracks.length === 0 && !listeningLogLoading && !listeningLogError ? (
          <p className="empty-copy">No play events are currently available.</p>
        ) : (
          <div className="recent-debug-list">
            {sessions.map((session, sessionIndex) => {
              const isSessionOpen = showDebugLinkFields || (openDebugSessions[session.id] ?? sessionIndex === 0);
              return (
                <section className="recent-debug-session" key={session.id}>
                  <button
                    className="recent-debug-session-toggle"
                    onClick={() =>
                      setOpenDebugSessions((current) => ({
                        ...current,
                        [session.id]: !isSessionOpen,
                      }))
                    }
                    type="button"
                  >
                    <span className="recent-debug-session-title">
                      Session {sessionIndex + 1}: {formatSessionRange(session)} ({session.tracks.length} {session.tracks.length === 1 ? "play" : "plays"})
                    </span>
                    <span>{isSessionOpen ? "^" : "v"}</span>
                  </button>
                  {isSessionOpen ? (
                    <div className="recent-debug-session-list">
                      {session.tracks.map((track, index) => {
                        const trackKey = debugTrackKey(session.id, track, index);
                        const isTrackOpen = showDebugLinkFields || Boolean(openDebugTracks[trackKey]);
                        const albumSummary = track.album_name ?? "Unknown album";
                        const albumWithYear = track.album_release_year
                          ? `${track.album_release_year} - ${albumSummary}`
                          : albumSummary;
                        const playedAtSummary = formatDebugTimestamp(track.spotify_played_at ?? null);
                        const durationSummary = formatDurationMs(track.duration_ms ?? null);
                        const estimatedSummary = formatDurationMs(track.estimated_played_ms ?? null);
                        const endMs = trackPlayedAtMs(track);
                        const estimatedMs = trackEstimatedMs(track);
                        const startMs = endMs != null && estimatedMs != null ? Math.max(0, endMs - estimatedMs) : null;
                        const playedGapMsValue = typeof track.played_at_gap_ms === "number"
                          ? Math.max(0, Math.round(track.played_at_gap_ms))
                          : null;
                        const timeRangeSummary =
                          startMs != null && endMs != null
                            ? `${new Date(startMs).toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" })} - ${new Date(endMs).toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" })}`
                            : formatTimeOnly(track.spotify_played_at ?? null);
                        const completionRatio =
                          typeof track.estimated_completion_ratio === "number"
                            ? Math.max(0, Math.min(1, track.estimated_completion_ratio))
                            : typeof track.duration_ms === "number" && track.duration_ms > 0 && typeof track.estimated_played_ms === "number"
                              ? Math.max(0, Math.min(1, track.estimated_played_ms / track.duration_ms))
                              : 0;
                        const nextOlderTrack = index + 1 < session.tracks.length ? session.tracks[index + 1] : null;
                        const nextOlderEndMs = nextOlderTrack ? trackPlayedAtMs(nextOlderTrack) : null;
                        const interTrackGapMs =
                          startMs != null && nextOlderEndMs != null
                            ? Math.max(0, startMs - nextOlderEndMs)
                            : null;
                        const showGapMarker = Boolean(
                          interTrackGapMs != null
                          && interTrackGapMs >= DEBUG_GAP_MARKER_MIN_MS
                          && interTrackGapMs <= DEBUG_GAP_MARKER_MAX_MS,
                        );
                        const rowEntries = Object.entries(track)
                          .filter(([key, value]) => {
                            if (!showDebugLinkFields && isLinkOrUriField(key, value)) {
                              return false;
                            }
                            if (key === "duration_seconds" || key === "estimated_played_seconds") {
                              return false;
                            }
                            if (key === "duration_ms" || key === "estimated_played_ms") {
                              return false;
                            }
                            return true;
                          })
                          .sort(([keyA], [keyB]) => {
                            const indexA = debugFieldOrder.indexOf(keyA);
                            const indexB = debugFieldOrder.indexOf(keyB);
                            const rankA = indexA === -1 ? 10_000 : indexA;
                            const rankB = indexB === -1 ? 10_000 : indexB;
                            if (rankA !== rankB) {
                              return rankA - rankB;
                            }
                            return keyA.localeCompare(keyB);
                          });

                        return (
                          <div className="recent-debug-item-wrap" key={trackKey}>
                            <article className="recent-debug-item">
                            <div className="recent-debug-item-top">
                              <div className="recent-debug-item-summary">
                                <p className="recent-debug-item-time" title={playedAtSummary}>
                                  {timeRangeSummary}
                                </p>
                                <div className="recent-debug-title-row">
                                  <button
                                    className="recent-debug-link recent-debug-item-title"
                                    onClick={() => openDebugPreview(track, "track")}
                                    title={track.track_name ?? "Unknown track"}
                                    type="button"
                                  >
                                    {track.track_name ?? "Unknown track"}
                                  </button>
                                  <span className="card-inline-badge">
                                    {track.source_label === "both"
                                      ? "Both"
                                      : track.source_label === "history"
                                        ? "History"
                                        : "API"}
                                  </span>
                                </div>
                                <button
                                  className="empty-copy recent-debug-link recent-debug-item-meta"
                                  onClick={() => openDebugPreview(track, "artist")}
                                  title={track.artist_name ?? "Unknown artist"}
                                  type="button"
                                >
                                  {track.artist_name ?? "Unknown artist"}
                                </button>
                                <button
                                  className="empty-copy recent-debug-link recent-debug-item-album"
                                  onClick={() => openDebugPreview(track, "album")}
                                  title={albumWithYear}
                                  type="button"
                                >
                                  {albumWithYear}
                                </button>
                                <div
                                  className="recent-debug-completion"
                                  title={`Estimated completion*: ${(completionRatio * 100).toFixed(1)}%`}
                                >
                                  <div
                                    className="recent-debug-completion-fill"
                                    style={{ width: `${completionRatio * 100}%` }}
                                  />
                                </div>
                                <div className="recent-debug-times">
                                  <span className="recent-debug-time-chip">
                                    Gap to previous play*: {formatDurationMs(playedGapMsValue)}
                                  </span>
                                  <span className="recent-debug-time-chip">Length: {durationSummary}</span>
                                  <span className="recent-debug-time-chip">Estimated played*: {estimatedSummary}</span>
                                </div>
                              </div>
                              {!showDebugLinkFields ? (
                                <button
                                  className="secondary-button recent-debug-expand-button"
                                  onClick={() =>
                                    setOpenDebugTracks((current) => ({
                                      ...current,
                                      [trackKey]: !isTrackOpen,
                                    }))
                                  }
                                  type="button"
                                >
                                  {isTrackOpen ? "Hide data" : "Show data"}
                                </button>
                              ) : null}
                            </div>
                            {isTrackOpen ? (
                              <div className="recent-debug-grid">
                                {rowEntries.map(([key, value]) => (
                                  <div className="recent-debug-row" key={`${trackKey}-${key}`}>
                                    <span className="recent-debug-key">
                                      {formatDebugLabel(key)}
                                      {isComputedField(key) ? "*" : ""}
                                    </span>
                                    <span className="recent-debug-value">{formatDebugValue(key, value)}</span>
                                  </div>
                                ))}
                              </div>
                            ) : null}
                            </article>
                            {showGapMarker ? (
                              <div
                                className="recent-debug-gap"
                                title={`Gap of ${formatDurationMs(interTrackGapMs ?? null)} before this play`}
                              >
                                <span className="recent-debug-gap-line" />
                                <span className="recent-debug-gap-text">
                                  {interTrackGapMs != null ? `${Math.max(0, Math.round(interTrackGapMs / 1000))}s gap` : "gap"}
                                </span>
                                <span className="recent-debug-gap-line" />
                              </div>
                            ) : null}
                          </div>
                        );
                      })}
                    </div>
                  ) : null}
                </section>
              );
            })}
            <div className="recent-debug-footer">
              <button
                className="secondary-button"
                disabled={!canTryLoadMore || listeningLogLoading}
                onClick={() => void loadListeningLogBatch(false)}
                title={
                  listeningLogLoading
                    ? "Loading older play events..."
                    : canTryLoadMore
                      ? "Load 50 more play events from the listening log"
                      : "No additional play events in the listening log"
                }
                type="button"
              >
                {listeningLogLoading ? "Loading..." : canTryLoadMore ? "Show 50 more" : "No more yet"}
              </button>
            </div>
          </div>
        )}
      </section>
    );
  }

  function renderAlbumColumn(
    section: SectionKey,
    items: TopAlbum[],
    available: boolean,
    emptyCopy: string,
    unavailableCopy: string,
    unavailableAction?: ReactNode,
  ) {
    if (!available) {
      return (
        <div className="section-unavailable">
          <p className="empty-copy">{unavailableCopy}</p>
          {unavailableAction ? <div className="section-unavailable-action">{unavailableAction}</div> : null}
        </div>
      );
    }
    if (items.length === 0) {
      return <p className="empty-copy">{emptyCopy}</p>;
    }

    const pageItems = visibleItems(section, items);
    return (
      <>
        <div className="item-list">
          {pageItems.map((album, index) =>
            renderDashboardListCard(
              {
                href: album.url,
                entityId: album.album_id,
                imageUrl: album.image_url,
                imageAlt: `${album.name ?? "Album"} cover`,
                fallbackLabel: "A",
                primaryText: album.name ?? "Unknown album",
                secondaryText: album.artist_name ?? "Unknown artist",
                tertiaryText:
                  formatHistoryDebugLine(album) ??
                  formatAlbumSummary(album),
                metricText: formatAlbumBreadth(album),
              },
              album.album_id ?? `${album.name}-${index}-${section}`,
            ),
          )}
          {Array.from({ length: emptySlots(pageItems) }).map((_, index) => (
            <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
          ))}
        </div>
        {renderPaging(section, items.length)}
      </>
    );
  }

  function renderPlaylistColumn(
    section: SectionKey,
    items: OwnedPlaylist[],
    available: boolean,
    emptyCopy: string,
    unavailableCopy: string,
    paged: boolean = true,
  ) {
    if (!available) {
      return <p className="empty-copy">{unavailableCopy}</p>;
    }
    if (items.length === 0) {
      return <p className="empty-copy">{emptyCopy}</p>;
    }

    const pageItems = paged ? visibleItems(section, items) : items;
    return (
      <>
        <div className="item-list">
          {pageItems.map((playlist, index) =>
            renderDashboardListCard(
              {
                href: playlist.url,
                entityId: playlist.playlist_id,
                imageUrl: playlist.image_url,
                imageAlt: `${playlist.name ?? "Playlist"} cover`,
                fallbackLabel: "P",
                primaryText: playlist.name ?? "Untitled playlist",
                primaryClamp: "two-line-clamp",
                secondaryText: playlist.description?.trim() || null,
                tertiaryText:
                  playlist.track_count != null ? `${playlist.track_count} tracks` : "Playlist",
              },
              playlist.playlist_id ?? `${playlist.name}-${index}-${section}`,
            ),
          )}
          {Array.from({ length: emptySlots(pageItems) }).map((_, index) => (
            <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
          ))}
        </div>
        {paged ? renderPaging(section, items.length) : null}
      </>
    );
  }

  function splitItems<T>(items: T[]) {
    const midpoint = Math.ceil(items.length / 2);
    return {
      left: items.slice(0, midpoint),
      right: items.slice(midpoint),
    };
  }

  function renderDualSectionCard(props: {
    title: ReactNode;
    section: SectionKey;
    anchorId: string;
    leftTitle: ReactNode;
    rightTitle: ReactNode;
    leftContent: ReactNode;
    rightContent: ReactNode;
    previewItemsLeft: PreviewItem[];
    previewItemsRight: PreviewItem[];
    collapsedPreviewItems?: PreviewItem[];
  }) {
    const {
      title,
      section,
      anchorId,
      leftTitle,
      rightTitle,
      leftContent,
      rightContent,
      previewItemsLeft,
      previewItemsRight,
      collapsedPreviewItems,
    } = props;

    return (
      <section className="info-card info-card-wide" id={anchorId}>
        <button className="section-toggle section-toggle-header" onClick={() => toggleSection(section, anchorId)} type="button">
          <h2>{title}</h2>
        </button>
        {openSections[section] ? (
          <div className="artists-grid">
            <div className="artists-column">
              {typeof leftTitle === "string" ? <h3>{leftTitle}</h3> : leftTitle}
              {leftContent}
            </div>
            <div className="artists-column">
              {typeof rightTitle === "string" ? <h3>{rightTitle}</h3> : rightTitle}
              {rightContent}
            </div>
          </div>
        ) : (
          <div className="preview-strip">
            {(collapsedPreviewItems ?? previewItemsLeft.concat(previewItemsRight)).slice(0, 5).map((item, index) =>
              renderPreviewCard(item, `${String(typeof title === "string" ? title : "section")}-${item.image}-${index}`),
            )}
          </div>
        )}
        <button className="section-toggle section-toggle-footer" onClick={() => toggleSection(section, anchorId)} type="button">
          <span>{openSections[section] ? "^" : "v"}</span>
        </button>
      </section>
    );
  }

  function renderPlaylistsSection() {
    if (!profile) {
      return null;
    }

    const visiblePlaylists = visibleItemsWithPageSize(
      "playlists",
      profile.owned_playlists,
      PLAYLISTS_PAGE_SIZE,
    );
    const playlistColumns = splitItems(visiblePlaylists);

    return (
      <section className="info-card info-card-wide" id="playlists">
        <button className="section-toggle section-toggle-header" onClick={() => toggleSection("playlists", "playlists")} type="button">
          <h2>{renderSectionTitle("Playlists", "playlists")}</h2>
        </button>
        {openSections.playlists ? (
          profile.owned_playlists_available ? (
            profile.owned_playlists.length > 0 ? (
              <div className="artists-grid">
                <div className="artists-column">
                  {renderPlaylistColumn(
                    "playlists",
                    playlistColumns.left,
                    true,
                    "No playlists were returned by Spotify for this account.",
                    "",
                    false,
                  )}
                </div>
                <div className="artists-column">
                  {playlistColumns.right.length > 0
                    ? renderPlaylistColumn(
                        "playlists",
                        playlistColumns.right,
                        true,
                        "No playlists were returned by Spotify for this account.",
                        "",
                        false,
                      )
                    : <p className="empty-copy">No more playlists in this column yet.</p>}
                </div>
              </div>
            ) : (
              <p className="empty-copy">No playlists were returned by Spotify for this account.</p>
            )
          ) : (
            <p className="empty-copy">
              {quickUnavailableCopy("Playlist access is not available for this session yet. Log out and log back in to grant access.")}
            </p>
          )
        ) : (
          <div className="preview-strip">
            {previewItems(profile.owned_playlists).map((item, index) =>
              renderPreviewCard(item, `playlists-${item.image}-${index}`),
            )}
          </div>
        )}
        {openSections.playlists && profile.owned_playlists.length > PLAYLISTS_PAGE_SIZE
          ? renderPagingWithPageSize("playlists", profile.owned_playlists.length, PLAYLISTS_PAGE_SIZE)
          : null}
        <button className="section-toggle section-toggle-footer" onClick={() => toggleSection("playlists", "playlists")} type="button">
          <span>{openSections.playlists ? "^" : "v"}</span>
        </button>
      </section>
    );
  }

  function renderLoadingScreen() {
    const latestHistory = statusHistory.length > 0 ? statusHistory[statusHistory.length - 1] : null;
    const loadingLabel =
      statusMessage && !statusMessage.startsWith("Spotify login succeeded")
        ? statusMessage
        : latestHistory ?? "Analyzing your music...";
    const analyzingStage = loadingLabel.toLowerCase().startsWith("analyzing");
    const quickLoadMode = analysisMode === "quick" && !analyzingStage;

    return (
      <main className="app-shell">
        <section className={`loading-screen${showRateLimitReload && !reloadReady ? " loading-screen-error" : ""}`}>
          <div className="loading-graphic" aria-hidden="true">
            <div className={`loading-headphones${showRateLimitReload && !reloadReady ? " loading-headphones-error" : ""}`}>
              <div className="loading-headphones-band" />
              <div className="loading-headphones-cup loading-headphones-cup-left" />
              <div className="loading-headphones-cup loading-headphones-cup-right" />
            </div>
          </div>
          <p className="eyebrow">ListenLab</p>
          <h1>{quickLoadMode ? "Loading your Spotify profile" : "Your music is being analyzed"}</h1>
          <p className="loading-copy two-line-clamp">
            {quickLoadMode
              ? "We're starting with a lighter profile view so you can get in quickly."
              : "We're pulling together your recent activity, favorites, and history-backed listening patterns."}
          </p>
          <p className="loading-phase single-line-ellipsis">{loadingLabel}</p>
          {renderCooldownRetryControl()}
        </section>
      </main>
    );
  }

  function renderCooldownRetryControl() {
    if (!showRateLimitReload) {
      return null;
    }

    return (
      <div className="loading-retry-row">
        <button
          className={`secondary-button loading-retry-button${reloadReady ? " loading-retry-button-ready" : ""}`}
          aria-label={reloadReady ? "Reload" : "Waiting for Spotify cooldown"}
          disabled={!reloadReady || loadingProfile || loadingRecentSection || loadingExtendedProfile}
          onClick={() => {
            setReloadCooldownUntil(null);
            setReloadCooldownDurationMs(60_000);
            if (!profile) {
              void loadProfile();
              return;
            }
            void refreshRecentSection(recentRange);
          }}
          style={{ opacity: 0.35 + reloadProgress * 0.65 }}
          type="button"
        >
          <span
            className="loading-retry-clock"
            aria-hidden="true"
            style={{ ["--reload-progress" as string]: `${reloadProgress * 360}deg` }}
          >
            <span className="loading-retry-clock-face">
              <span className="loading-retry-clock-groove loading-retry-clock-groove-outer" />
              <span className="loading-retry-clock-groove loading-retry-clock-groove-inner" />
              <span className="loading-retry-clock-pie" />
              <span className="loading-retry-clock-center-ring" />
              <span className="loading-retry-clock-center">
                {reloadReady ? "\u21bb" : ""}
              </span>
            </span>
          </span>
        </button>
      </div>
    );
  }

  function renderFullAnalysisOverlay() {
    const latestHistory = statusHistory.length > 0 ? statusHistory[statusHistory.length - 1] : null;
    const loadingLabel =
      statusMessage && !statusMessage.startsWith("Spotify login succeeded")
        ? statusMessage
        : latestHistory ?? "Analyzing your music...";
    const analyzingStage = loadingLabel.toLowerCase().startsWith("analyzing");
    const quickLoadMode = analysisMode === "quick" && !analyzingStage;

    return (
      <div className="loading-overlay-backdrop" role="status" aria-live="polite">
        <section className={`loading-screen${showRateLimitReload ? " loading-screen-error" : ""}`}>
          <div className="loading-graphic" aria-hidden="true">
            <div className={`loading-headphones${showRateLimitReload ? " loading-headphones-error" : ""}`}>
              <div className="loading-headphones-band" />
              <div className="loading-headphones-cup loading-headphones-cup-left" />
              <div className="loading-headphones-cup loading-headphones-cup-right" />
            </div>
          </div>
          <p className="eyebrow">ListenLab</p>
          <h1>{quickLoadMode ? "Loading your Spotify profile" : "Your music is being analyzed"}</h1>
          <p className="loading-copy two-line-clamp">
            {quickLoadMode
              ? "We're starting with a lighter profile view so you can get in quickly."
              : "We're pulling together your recent activity, favorites, and history-backed listening patterns."}
          </p>
          <p className="loading-phase single-line-ellipsis">{loadingLabel}</p>
          {renderCooldownRetryControl()}
        </section>
      </div>
    );
  }

  async function loadFullAnalysis() {
    setAnalysisMode("full");
    await loadExtendedProfile(recentRange, "full");
  }

  async function recomputeHistoryFromLocal() {
    if (loadingHistoryRecompute || loadingExtendedProfile) {
      return;
    }
    setLoadingHistoryRecompute(true);
    setStatusMessage("Recomputing from full history...");
    try {
      const response = await fetch(`${apiBaseUrl}/cache/rebuild`, {
        method: "POST",
        credentials: "include",
      });
      if (!response.ok) {
        let detail = "Failed to recompute history.";
        try {
          const payload = (await response.json()) as { detail?: string };
          if (payload.detail) {
            detail = payload.detail;
          }
        } catch {
          // Keep fallback detail.
        }
        throw new Error(detail);
      }
      setAnalysisMode("full");
      await loadExtendedProfile(recentRange, "full");
      setStatusHistory((current) => [...current, "History recompute complete."]);
    } catch (error) {
      const message = formatUiErrorMessage(error, "Failed to recompute history.");
      setStatusMessage(message);
      setStatusHistory((current) => [...current, `History recompute error: ${message}`]);
    } finally {
      setLoadingHistoryRecompute(false);
    }
  }

  async function fetchRecentSections(targetRange: RecentRange): Promise<RecentSectionResponse> {
    const endpoint = experienceMode === "local" ? "/me/local/recent" : "/me/recent";
    const response = await fetch(
      `${apiBaseUrl}${endpoint}?recent_range=${encodeURIComponent(targetRange)}&limit=${encodeURIComponent(String(RECENT_SECTION_FETCH_LIMIT))}`,
      {
        credentials: "include",
      },
    );
    if (!response.ok) {
      let detail = "Failed to refresh recent sections.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 429 && experienceMode === "full") {
        const cooldownSeconds = parseCooldownSeconds(detail) ?? 60;
        setReloadCooldownDurationMs(cooldownSeconds * 1000);
        setReloadCooldownUntil(Date.now() + cooldownSeconds * 1000);
        detail = formatCooldownCopy(cooldownSeconds);
      }
      throw new Error(detail);
    }
    return (await response.json()) as RecentSectionResponse;
  }

  async function fetchListeningLog(
    limit: number,
    offset: number,
    sourceFilter: RecentDebugSourceFilter,
  ): Promise<ListeningLogResponse> {
    const endpoint = "/debug/listening-log";
    const response = await fetch(
      `${apiBaseUrl}${endpoint}?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}&source_filter=${encodeURIComponent(sourceFilter)}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load listening log.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Listening Log (${response.status}): ${detail}`);
    }
    return (await response.json()) as ListeningLogResponse;
  }

  async function fetchCatalogBackfillCoverage(): Promise<CatalogBackfillCoverageResponse> {
    const response = await fetch(`${apiBaseUrl}/debug/spotify/catalog-backfill/coverage`, {
      credentials: "include",
    });
    if (!response.ok) {
      let detail = "Failed to load catalog backfill coverage.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Catalog Backfill Coverage (${response.status}): ${detail}`);
    }
    return (await response.json()) as CatalogBackfillCoverageResponse;
  }

  async function fetchCatalogBackfillRuns(limit: number = 20, offset: number = 0): Promise<CatalogBackfillRunsResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/spotify/catalog-backfill/runs?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`,
      {
        credentials: "include",
      },
    );
    if (!response.ok) {
      let detail = "Failed to load catalog backfill runs.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Catalog Backfill Runs (${response.status}): ${detail}`);
    }
    return (await response.json()) as CatalogBackfillRunsResponse;
  }

  async function fetchCatalogBackfillQueue(
    statusFilter: "all" | "pending" | "done" | "error" = "all",
    limit: number = 50,
    offset: number = 0
  ): Promise<CatalogBackfillQueueResponse> {
    const statusQuery = statusFilter === "all" ? "" : `&status=${encodeURIComponent(statusFilter)}`;
    const response = await fetch(
      `${apiBaseUrl}/debug/spotify/catalog-backfill/queue?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}${statusQuery}`,
      {
        credentials: "include",
      },
    );
    if (!response.ok) {
      let detail = "Failed to load catalog backfill queue.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Catalog Backfill Queue (${response.status}): ${detail}`);
    }
    return (await response.json()) as CatalogBackfillQueueResponse;
  }

  async function postCatalogBackfillQueueRepair(): Promise<CatalogBackfillQueueRepairResponse> {
    const response = await fetch(`${apiBaseUrl}/debug/spotify/catalog-backfill/queue/repair`, {
      method: "POST",
      credentials: "include",
    });
    const payload = (await response.json()) as CatalogBackfillQueueRepairResponse | { detail?: string; error?: { message?: string } };
    if (!response.ok || !("ok" in payload && payload.ok)) {
      let detail = "Failed to repair queue statuses.";
      if ("error" in payload && payload.error?.message) {
        detail = payload.error.message;
      } else if ("detail" in payload && payload.detail) {
        detail = payload.detail;
      }
      throw new Error(`Catalog Backfill Queue Repair (${response.status}): ${detail}`);
    }
    return payload as CatalogBackfillQueueRepairResponse;
  }

  async function fetchAlbumCatalogLookup(
    q: string,
    catalogStatus: "all" | "backfilled" | "not_backfilled" | "tracklist_complete" | "tracklist_incomplete" | "error",
    queueStatus: "all" | "not_queued" | "pending" | "done" | "error",
    sort: "default" | "recently_backfilled" | "name" | "incomplete_first",
    limit: number = 50,
    offset: number = 0
  ): Promise<AlbumCatalogLookupResponse> {
    const qQuery = q.trim() ? `&q=${encodeURIComponent(q.trim())}` : "";
    const response = await fetch(
      `${apiBaseUrl}/debug/search/albums?catalog_status=${encodeURIComponent(catalogStatus)}&queue_status=${encodeURIComponent(queueStatus)}&sort=${encodeURIComponent(sort)}&limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}${qQuery}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to search albums.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Album Catalog Lookup (${response.status}): ${detail}`);
    }
    return (await response.json()) as AlbumCatalogLookupResponse;
  }

  async function fetchTrackCatalogLookup(
    q: string,
    catalogStatus: "all" | "backfilled" | "not_backfilled" | "duration_missing" | "error",
    queueStatus: "all" | "not_queued" | "pending" | "done" | "error",
    sort: "default" | "recently_backfilled" | "name" | "incomplete_first",
    limit: number = 50,
    offset: number = 0
  ): Promise<TrackCatalogLookupResponse> {
    const qQuery = q.trim() ? `&q=${encodeURIComponent(q.trim())}` : "";
    const response = await fetch(
      `${apiBaseUrl}/debug/search/tracks?catalog_status=${encodeURIComponent(catalogStatus)}&queue_status=${encodeURIComponent(queueStatus)}&sort=${encodeURIComponent(sort)}&limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}${qQuery}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to search tracks.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Track Catalog Lookup (${response.status}): ${detail}`);
    }
    return (await response.json()) as TrackCatalogLookupResponse;
  }

  async function fetchAlbumDuplicateLookup(
    limit: number = 200,
    offset: number = 0
  ): Promise<AlbumDuplicateLookupResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/search/albums/duplicates?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load duplicate albums.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Album Duplicate Lookup (${response.status}): ${detail}`);
    }
    return (await response.json()) as AlbumDuplicateLookupResponse;
  }

  async function fetchTrackDuplicateLookup(
    limit: number = 200,
    offset: number = 0
  ): Promise<TrackDuplicateLookupResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/search/tracks/duplicates?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load duplicate tracks.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Track Duplicate Lookup (${response.status}): ${detail}`);
    }
    return (await response.json()) as TrackDuplicateLookupResponse;
  }

  async function fetchAlbumNameDuplicateLookup(
    limit: number = 200,
    offset: number = 0
  ): Promise<AlbumNameDuplicateLookupResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/search/albums/duplicates-by-name?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load duplicate albums by name.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Album Name Duplicate Lookup (${response.status}): ${detail}`);
    }
    return (await response.json()) as AlbumNameDuplicateLookupResponse;
  }

  async function postReleaseAlbumMergePreview(releaseAlbumIds: number[]): Promise<ReleaseAlbumMergePreviewResponse> {
    const response = await fetch(`${apiBaseUrl}/debug/identity/release-albums/merge-preview`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ release_album_ids: releaseAlbumIds }),
    });
    const payload = (await response.json()) as ReleaseAlbumMergePreviewResponse | { detail?: string };
    if (!response.ok) {
      let detail = "Failed to preview release album merge.";
      if ("detail" in payload && payload.detail) {
        detail = payload.detail;
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Release Album Merge Preview (${response.status}): ${detail}`);
    }
    return payload as ReleaseAlbumMergePreviewResponse;
  }

  async function postReleaseAlbumMergeDryRun(releaseAlbumIds: number[], survivorReleaseAlbumId: number): Promise<ReleaseAlbumMergeDryRunResponse> {
    const response = await fetch(`${apiBaseUrl}/debug/identity/release-albums/merge-dry-run`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        release_album_ids: releaseAlbumIds,
        survivor_release_album_id: survivorReleaseAlbumId,
      }),
    });
    const payload = (await response.json()) as ReleaseAlbumMergeDryRunResponse | { detail?: string };
    if (!response.ok) {
      let detail = "Failed to dry run release album merge.";
      if ("detail" in payload && payload.detail) {
        detail = payload.detail;
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Release Album Merge Dry Run (${response.status}): ${detail}`);
    }
    return payload as ReleaseAlbumMergeDryRunResponse;
  }

  async function postCatalogBackfillRun(): Promise<CatalogBackfillRunResponse> {
    const response = await fetch(`${apiBaseUrl}/debug/spotify/catalog-backfill`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        limit: Math.max(1, Math.round(catalogBackfillLimit)),
        offset: Math.max(0, Math.round(catalogBackfillOffset)),
        market: catalogBackfillMarket.trim() || "US",
        include_albums: catalogBackfillIncludeAlbums,
        force_refresh: catalogBackfillForceRefresh,
        request_delay_seconds: Math.max(0.2, catalogBackfillRequestDelaySeconds),
        max_runtime_seconds: Math.min(300, Math.max(5, Math.round(catalogBackfillMaxRuntimeSeconds))),
        max_requests: Math.min(1000, Math.max(1, Math.round(catalogBackfillMaxRequests))),
        max_errors: Math.min(100, Math.max(1, Math.round(catalogBackfillMaxErrors))),
        max_album_tracks_pages_per_album: Math.min(50, Math.max(1, Math.round(catalogBackfillMaxAlbumTracksPagesPerAlbum))),
        max_429: Math.min(20, Math.max(1, Math.round(catalogBackfillMax429))),
        album_tracklist_policy: catalogBackfillAlbumTracklistPolicy,
      }),
    });
    const payload = (await response.json()) as
      | CatalogBackfillRunResponse
      | { detail?: string; error?: { message?: string }; status?: string; last_error?: string | null };
    if (!response.ok || !("ok" in payload && payload.ok)) {
      let detail = "Catalog backfill failed.";
      if ("error" in payload && payload.error?.message) {
        detail = payload.error.message;
      } else if ("detail" in payload && payload.detail) {
        detail = payload.detail;
      } else if ("last_error" in payload && payload.last_error) {
        detail = payload.last_error;
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Catalog Backfill Run (${response.status}): ${detail}`);
    }
    return payload as CatalogBackfillRunResponse;
  }

  async function enqueueCatalogBackfillItems(
    items: Array<{ entity_type: "track" | "album"; spotify_id: string; reason?: string; priority?: number }>
  ): Promise<CatalogBackfillEnqueueResponse> {
    const response = await fetch(`${apiBaseUrl}/debug/spotify/catalog-backfill/enqueue`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ items }),
    });
    const payload = (await response.json()) as CatalogBackfillEnqueueResponse | { detail?: string; error?: { message?: string } };
    if (!response.ok || !("ok" in payload && payload.ok)) {
      let detail = "Catalog enqueue failed.";
      if ("error" in payload && payload.error?.message) {
        detail = payload.error.message;
      } else if ("detail" in payload && payload.detail) {
        detail = payload.detail;
      }
      throw new Error(`Catalog Backfill Enqueue (${response.status}): ${detail}`);
    }
    return payload as CatalogBackfillEnqueueResponse;
  }

  async function fetchMergedTrackAggregate(): Promise<MergedTrackAggregateResponse> {
    const response = await fetch(
      `${apiBaseUrl}/tracks/merged-aggregate?limit=${TRACKS_FORMULA_FETCH_LIMIT}&recent_window_days=28&source_filter=all`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load merged track aggregate.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Merged Tracks (${response.status}): ${detail}`);
    }
    return (await response.json()) as MergedTrackAggregateResponse;
  }

  async function fetchIdentityAudit(): Promise<TrackIdentityAuditResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/identity-audit?limit=5`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load identity audit.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Identity Audit (${response.status}): ${detail}`);
    }
    return (await response.json()) as TrackIdentityAuditResponse;
  }

  async function fetchIdentityAuditSuggestedGroups(): Promise<SuggestedGroupsResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/identity-audit/suggested-groups?limit=50&offset=0&status_filter=suggested`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load suggested composition groups.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Identity Audit Suggested Groups (${response.status}): ${detail}`);
    }
    return (await response.json()) as SuggestedGroupsResponse;
  }

  async function fetchIdentityAuditAmbiguousReview(): Promise<AmbiguousReviewResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/identity-audit/ambiguous-review?limit=500&offset=0`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load ambiguous review queue.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Identity Audit Ambiguous Review (${response.status}): ${detail}`);
    }
    return (await response.json()) as AmbiguousReviewResponse;
  }

  async function fetchIdentityAuditSavedSubmissions(limit: number = 20, offset: number = 0): Promise<IdentityAuditSavedSubmissionListResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/identity-audit/submissions?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load saved submissions.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Identity Audit Saved Submissions (${response.status}): ${detail}`);
    }
    return (await response.json()) as IdentityAuditSavedSubmissionListResponse;
  }

  async function fetchIdentityAuditSavedSubmissionById(submissionId: number): Promise<IdentityAuditSavedSubmissionReadResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/identity-audit/submissions/${encodeURIComponent(String(submissionId))}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load saved submission.";
      try {
        const payload = (await response.json()) as { detail?: string; error?: { message?: string } };
        if (payload.error?.message) {
          detail = payload.error.message;
        } else if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Identity Audit Saved Submission (${response.status}): ${detail}`);
    }
    return (await response.json()) as IdentityAuditSavedSubmissionReadResponse;
  }

  async function fetchIdentityAuditSavedSubmissionDryRun(submissionId: number): Promise<IdentityAuditSubmissionDryRunResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/identity-audit/submissions/${encodeURIComponent(String(submissionId))}/dry-run`,
      {
        method: "POST",
        credentials: "include",
      },
    );
    if (!response.ok) {
      let detail = "Failed to run dry run.";
      try {
        const payload = (await response.json()) as { detail?: string; error?: { message?: string } };
        if (payload.error?.message) {
          detail = payload.error.message;
        } else if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Identity Audit Submission Dry Run (${response.status}): ${detail}`);
    }
    return (await response.json()) as IdentityAuditSubmissionDryRunResponse;
  }

  async function loadMergedTrackRankings(reset: boolean = false) {
    if (mergedTracksLoading) {
      return;
    }
    if (reset) {
      setMergedTracks([]);
      setMergedTracksExcludedUnknownCount(0);
      setMergedTracksLoaded(false);
      setMergedTracksLastLoadedAt(null);
    }
    setMergedTracksLoading(true);
    setMergedTracksError("");
    try {
      const data = await fetchMergedTrackAggregate();
      setMergedTracks(data.items);
      setMergedTracksExcludedUnknownCount(Math.max(0, data.excluded_unknown_identity_count ?? 0));
      setMergedTracksLoaded(true);
      setMergedTracksLastLoadedAt(Date.now());
    } catch (error) {
      setMergedTracksError(formatUiErrorMessage(error, "Failed to load merged tracks."));
    } finally {
      setMergedTracksLoading(false);
    }
  }

  function reloadTrackRankings() {
    void loadMergedTrackRankings(true);
  }

  async function loadIdentityAudit(reset: boolean = false) {
    if (identityAuditLoading) {
      return;
    }
    if (reset) {
      setIdentityAudit(null);
      setIdentityAuditLoaded(false);
      setIdentityAuditLastLoadedAt(null);
    }
    setIdentityAuditLoading(true);
    setIdentityAuditError("");
    try {
      const data = await fetchIdentityAudit();
      setIdentityAudit(data);
      setIdentityAuditLoaded(true);
      setIdentityAuditLastLoadedAt(Date.now());
    } catch (error) {
      setIdentityAuditError(formatUiErrorMessage(error, "Failed to load identity audit."));
    } finally {
      setIdentityAuditLoading(false);
    }
  }

  async function loadIdentityAuditSuggestedGroups(reset: boolean = false) {
    if (identityAuditSuggestedLoading) {
      return;
    }
    if (reset) {
      setIdentityAuditSuggestedGroups(null);
      setIdentityAuditSuggestedLoaded(false);
      setIdentityAuditSuggestedLastLoadedAt(null);
    }
    setIdentityAuditSuggestedLoading(true);
    setIdentityAuditSuggestedError("");
    try {
      const data = await fetchIdentityAuditSuggestedGroups();
      setIdentityAuditSuggestedGroups(data);
      setIdentityAuditSuggestedLoaded(true);
      setIdentityAuditSuggestedLastLoadedAt(Date.now());
    } catch (error) {
      setIdentityAuditSuggestedError(formatUiErrorMessage(error, "Failed to load suggested composition groups."));
    } finally {
      setIdentityAuditSuggestedLoading(false);
    }
  }

  async function loadIdentityAuditAmbiguousReview(reset: boolean = false) {
    if (identityAuditAmbiguousLoading) {
      return;
    }
    if (reset) {
      setIdentityAuditAmbiguous(null);
      setIdentityAuditAmbiguousLoaded(false);
      setIdentityAuditAmbiguousLastLoadedAt(null);
    }
    setIdentityAuditAmbiguousLoading(true);
    setIdentityAuditAmbiguousError("");
    try {
      const data = await fetchIdentityAuditAmbiguousReview();
      setIdentityAuditAmbiguous(data);
      setIdentityAuditAmbiguousLoaded(true);
      setIdentityAuditAmbiguousLastLoadedAt(Date.now());
    } catch (error) {
      setIdentityAuditAmbiguousError(formatUiErrorMessage(error, "Failed to load ambiguous review queue."));
    } finally {
      setIdentityAuditAmbiguousLoading(false);
    }
  }

  async function loadIdentityAuditSavedSubmissions(reset: boolean = false) {
    if (identityAuditSavedSubmissionsLoading) {
      return;
    }
    if (reset) {
      setIdentityAuditSavedSubmissions(null);
      setIdentityAuditSavedSubmissionDetail(null);
      setIdentityAuditSavedSubmissionDetailError("");
      setIdentityAuditSavedSubmissionDryRun(null);
      setIdentityAuditSavedSubmissionDryRunError("");
      setIdentityAuditSavedSubmissionDryRunLoading(false);
      setIdentityAuditSavedSubmissionDryRunAt(null);
    }
    setIdentityAuditSavedSubmissionsLoading(true);
    setIdentityAuditSavedSubmissionsError("");
    try {
      const data = await fetchIdentityAuditSavedSubmissions(20, 0);
      setIdentityAuditSavedSubmissions(data);
    } catch (error) {
      setIdentityAuditSavedSubmissionsError(formatUiErrorMessage(error, "Failed to load saved submissions."));
    } finally {
      setIdentityAuditSavedSubmissionsLoading(false);
    }
  }

  async function viewIdentityAuditSavedSubmission(submissionId: number) {
    if (identityAuditSavedSubmissionDetailLoading) {
      return;
    }
    setIdentityAuditSavedSubmissionDetailLoading(true);
    setIdentityAuditSavedSubmissionDetailError("");
    setIdentityAuditSavedSubmissionDryRun(null);
    setIdentityAuditSavedSubmissionDryRunError("");
    setIdentityAuditSavedSubmissionDryRunLoading(false);
    setIdentityAuditSavedSubmissionDryRunAt(null);
    try {
      const payload = await fetchIdentityAuditSavedSubmissionById(submissionId);
      setIdentityAuditSavedSubmissionDetail(payload);
    } catch (error) {
      setIdentityAuditSavedSubmissionDetailError(formatUiErrorMessage(error, "Failed to load saved submission details."));
      setIdentityAuditSavedSubmissionDetail(null);
    } finally {
      setIdentityAuditSavedSubmissionDetailLoading(false);
    }
  }

  async function dryRunIdentityAuditSavedSubmission(submissionId: number) {
    if (identityAuditSavedSubmissionDryRunLoading) {
      return;
    }
    setIdentityAuditSavedSubmissionDryRunLoading(true);
    setIdentityAuditSavedSubmissionDryRunError("");
    try {
      const payload = await fetchIdentityAuditSavedSubmissionDryRun(submissionId);
      setIdentityAuditSavedSubmissionDryRun(payload);
      setIdentityAuditSavedSubmissionDryRunAt(Date.now());
    } catch (error) {
      setIdentityAuditSavedSubmissionDryRunError(formatUiErrorMessage(error, "Failed to run dry run."));
      setIdentityAuditSavedSubmissionDryRun(null);
      setIdentityAuditSavedSubmissionDryRunAt(null);
    } finally {
      setIdentityAuditSavedSubmissionDryRunLoading(false);
    }
  }

  async function loadListeningLogBatch(reset: boolean = false) {
    if (listeningLogLoading) {
      return;
    }
    setListeningLogLoading(true);
    setListeningLogError("");
    try {
      const targetOffset = reset ? 0 : listeningLogOffset;
      const payload = await fetchListeningLog(50, targetOffset, recentDebugSourceFilter);
      setListeningLogTracks((current) => (reset ? payload.items : [...current, ...payload.items]));
      setListeningLogOffset(targetOffset + payload.items.length);
      setListeningLogHasMore(Boolean(payload.has_more));
      setListeningLogLoaded(true);
      setListeningLogLastLoadedAt(Date.now());
    } catch (error) {
      const message = formatUiErrorMessage(error, "Failed to load listening log.");
      setStatusHistory((current) => [...current, `Listening log error: ${message}`]);
      setListeningLogError(message);
      if (reset) {
        setListeningLogTracks([]);
        setListeningLogOffset(0);
        setListeningLogHasMore(false);
      }
    } finally {
      setListeningLogLoading(false);
    }
  }

  async function loadCatalogBackfillCoverage(reset: boolean = false) {
    if (catalogBackfillCoverageLoading) {
      return;
    }
    if (reset) {
      setCatalogBackfillCoverage(null);
      setCatalogBackfillCoverageLoaded(false);
      setCatalogBackfillCoverageLastLoadedAt(null);
    }
    setCatalogBackfillCoverageLoading(true);
    setCatalogBackfillCoverageError("");
    try {
      const payload = await fetchCatalogBackfillCoverage();
      setCatalogBackfillCoverage(payload);
      setCatalogBackfillCoverageLoaded(true);
      setCatalogBackfillCoverageLastLoadedAt(Date.now());
    } catch (error) {
      setCatalogBackfillCoverageError(formatUiErrorMessage(error, "Failed to load catalog coverage."));
    } finally {
      setCatalogBackfillCoverageLoading(false);
    }
  }

  async function loadCatalogBackfillRuns(reset: boolean = false) {
    if (catalogBackfillRunsLoading) {
      return;
    }
    if (reset) {
      setCatalogBackfillRuns(null);
      setCatalogBackfillRunsLoaded(false);
      setCatalogBackfillRunsLastLoadedAt(null);
    }
    setCatalogBackfillRunsLoading(true);
    setCatalogBackfillRunsError("");
    try {
      const payload = await fetchCatalogBackfillRuns(20, 0);
      setCatalogBackfillRuns(payload);
      setCatalogBackfillRunsLoaded(true);
      setCatalogBackfillRunsLastLoadedAt(Date.now());
    } catch (error) {
      setCatalogBackfillRunsError(formatUiErrorMessage(error, "Failed to load backfill runs."));
    } finally {
      setCatalogBackfillRunsLoading(false);
    }
  }

  async function loadCatalogBackfillQueue(
    reset: boolean = false,
    explicitFilter?: "all" | "pending" | "done" | "error"
  ) {
    if (catalogBackfillQueueLoading) {
      return;
    }
    if (reset) {
      setCatalogBackfillQueue(null);
      setCatalogBackfillQueueLoaded(false);
      setCatalogBackfillQueueLastLoadedAt(null);
    }
    const activeFilter = explicitFilter ?? catalogBackfillQueueStatusFilter;
    if (explicitFilter && explicitFilter !== catalogBackfillQueueStatusFilter) {
      setCatalogBackfillQueueStatusFilter(explicitFilter);
    }
    setCatalogBackfillQueueLoading(true);
    setCatalogBackfillQueueError("");
    try {
      const payload = await fetchCatalogBackfillQueue(activeFilter, 50, 0);
      setCatalogBackfillQueue(payload);
      setCatalogBackfillQueueLoaded(true);
      setCatalogBackfillQueueLastLoadedAt(Date.now());
    } catch (error) {
      setCatalogBackfillQueueError(formatUiErrorMessage(error, "Failed to load backfill queue."));
    } finally {
      setCatalogBackfillQueueLoading(false);
    }
  }

  async function repairCatalogBackfillQueueStatuses() {
    if (catalogBackfillQueueRepairLoading) {
      return;
    }
    setCatalogBackfillQueueRepairLoading(true);
    setCatalogBackfillQueueRepairMessage("");
    try {
      const payload = await postCatalogBackfillQueueRepair();
      setCatalogBackfillQueueRepairMessage(`Repaired ${payload.repaired} queue item status values.`);
      await loadCatalogBackfillQueue(true);
    } catch (error) {
      setCatalogBackfillQueueRepairMessage(formatUiErrorMessage(error, "Failed to repair queue statuses."));
    } finally {
      setCatalogBackfillQueueRepairLoading(false);
    }
  }

  async function loadAlbumCatalogLookup(reset: boolean = false) {
    if (albumCatalogLookupLoading) {
      return;
    }
    if (reset) {
      setAlbumCatalogLookupResult(null);
      setAlbumCatalogLookupLoaded(false);
      setAlbumCatalogLookupLastLoadedAt(null);
    }
    setAlbumCatalogLookupLoading(true);
    setAlbumCatalogLookupError("");
    try {
      const payload = await fetchAlbumCatalogLookup(
        albumCatalogLookupQ,
        albumCatalogLookupStatus,
        searchLookupQueueStatus,
        searchLookupSort,
        50,
        0,
      );
      setAlbumCatalogLookupResult(payload);
      setAlbumCatalogLookupLoaded(true);
      setAlbumCatalogLookupLastLoadedAt(Date.now());
    } catch (error) {
      setAlbumCatalogLookupError(formatUiErrorMessage(error, "Failed to search albums."));
    } finally {
      setAlbumCatalogLookupLoading(false);
    }
  }

  async function loadTrackCatalogLookup(reset: boolean = false) {
    if (trackCatalogLookupLoading) {
      return;
    }
    if (reset) {
      setTrackCatalogLookupResult(null);
      setTrackCatalogLookupLoaded(false);
      setTrackCatalogLookupLastLoadedAt(null);
    }
    setTrackCatalogLookupLoading(true);
    setTrackCatalogLookupError("");
    try {
      const payload = await fetchTrackCatalogLookup(
        albumCatalogLookupQ,
        trackCatalogLookupStatus,
        searchLookupQueueStatus,
        searchLookupSort,
        50,
        0,
      );
      setTrackCatalogLookupResult(payload);
      setTrackCatalogLookupLoaded(true);
      setTrackCatalogLookupLastLoadedAt(Date.now());
    } catch (error) {
      setTrackCatalogLookupError(formatUiErrorMessage(error, "Failed to search tracks."));
    } finally {
      setTrackCatalogLookupLoading(false);
    }
  }

  async function loadAlbumDuplicateLookup(reset: boolean = false) {
    if (albumDuplicateLookupLoading) {
      return;
    }
    if (reset) {
      setAlbumDuplicateLookupResult(null);
      setAlbumDuplicateLookupLoaded(false);
      setAlbumDuplicateLookupLastLoadedAt(null);
    }
    setAlbumDuplicateLookupLoading(true);
    setAlbumDuplicateLookupError("");
    try {
      const payload = await fetchAlbumDuplicateLookup(200, 0);
      setAlbumDuplicateLookupResult(payload);
      setAlbumDuplicateLookupLoaded(true);
      setAlbumDuplicateLookupLastLoadedAt(Date.now());
    } catch (error) {
      setAlbumDuplicateLookupError(formatUiErrorMessage(error, "Failed to load duplicate albums."));
    } finally {
      setAlbumDuplicateLookupLoading(false);
    }
  }

  async function loadTrackDuplicateLookup(reset: boolean = false) {
    if (trackDuplicateLookupLoading) {
      return;
    }
    if (reset) {
      setTrackDuplicateLookupResult(null);
      setTrackDuplicateLookupLoaded(false);
      setTrackDuplicateLookupLastLoadedAt(null);
    }
    setTrackDuplicateLookupLoading(true);
    setTrackDuplicateLookupError("");
    try {
      const payload = await fetchTrackDuplicateLookup(200, 0);
      setTrackDuplicateLookupResult(payload);
      setTrackDuplicateLookupLoaded(true);
      setTrackDuplicateLookupLastLoadedAt(Date.now());
    } catch (error) {
      setTrackDuplicateLookupError(formatUiErrorMessage(error, "Failed to load duplicate tracks."));
    } finally {
      setTrackDuplicateLookupLoading(false);
    }
  }

  async function loadAlbumNameDuplicateLookup(reset: boolean = false) {
    if (albumNameDuplicateLookupLoading) {
      return;
    }
    if (reset) {
      setAlbumNameDuplicateLookupResult(null);
      setAlbumNameDuplicateLookupLoaded(false);
      setAlbumNameDuplicateLookupLastLoadedAt(null);
    }
    setAlbumNameDuplicateLookupLoading(true);
    setAlbumNameDuplicateLookupError("");
    try {
      const payload = await fetchAlbumNameDuplicateLookup(200, 0);
      setAlbumNameDuplicateLookupResult(payload);
      setAlbumNameDuplicateLookupLoaded(true);
      setAlbumNameDuplicateLookupLastLoadedAt(Date.now());
    } catch (error) {
      setAlbumNameDuplicateLookupError(formatUiErrorMessage(error, "Failed to load duplicate albums by name."));
    } finally {
      setAlbumNameDuplicateLookupLoading(false);
    }
  }

  async function loadActiveSearchLookup(reset: boolean = false) {
    if (searchLookupEntityType === "duplicate_albums") {
      await loadAlbumDuplicateLookup(reset);
      return;
    }
    if (searchLookupEntityType === "tracks") {
      await loadTrackCatalogLookup(reset);
      return;
    }
    await loadAlbumCatalogLookup(reset);
  }

  async function enqueueVisibleIncompleteLookupAlbums(items?: AlbumCatalogLookupItem[]) {
    if (albumCatalogLookupEnqueueLoading) {
      return;
    }
    const sourceItems = items ?? (albumCatalogLookupResult?.items ?? []);
    const spotifyAlbumIds = Array.from(
      new Set(
        sourceItems
          .filter((item) => albumLookupRowCanBulkPrioritize(item))
          .map((item) => item.spotify_album_id)
          .filter((spotifyAlbumId): spotifyAlbumId is string => Boolean(spotifyAlbumId)),
      ),
    );
    if (spotifyAlbumIds.length === 0) {
      setAlbumCatalogLookupEnqueueResult(null);
      setAlbumCatalogLookupEnqueueError("No visible incomplete albums with Spotify IDs to enqueue.");
      return;
    }

    setAlbumCatalogLookupEnqueueLoading(true);
    setAlbumCatalogLookupEnqueueError("");
    setAlbumCatalogLookupEnqueueResult(null);
    try {
      const payload = await enqueueCatalogBackfillItems(
        spotifyAlbumIds.map((spotifyId) => ({
          entity_type: "album",
          spotify_id: spotifyId,
          reason: "album_lookup_visible_incomplete",
          priority: 80,
        })),
      );
      setAlbumCatalogLookupEnqueueResult(payload);
      await loadCatalogBackfillQueue(true);
    } catch (error) {
      setAlbumCatalogLookupEnqueueError(formatUiErrorMessage(error, "Failed to enqueue albums."));
    } finally {
      setAlbumCatalogLookupEnqueueLoading(false);
    }
  }

  async function enqueueVisibleIncompleteLookupTracks(items?: TrackCatalogLookupItem[]) {
    if (albumCatalogLookupEnqueueLoading) {
      return;
    }
    const sourceItems = items ?? (trackCatalogLookupResult?.items ?? []);
    const spotifyTrackIds = Array.from(
      new Set(
        sourceItems
          .filter((item) => trackLookupRowCanBulkPrioritize(item))
          .map((item) => item.spotify_track_id)
          .filter((spotifyTrackId): spotifyTrackId is string => Boolean(spotifyTrackId)),
      ),
    );
    if (spotifyTrackIds.length === 0) {
      setAlbumCatalogLookupEnqueueResult(null);
      setAlbumCatalogLookupEnqueueError("No visible incomplete tracks with Spotify IDs to prioritize.");
      return;
    }

    setAlbumCatalogLookupEnqueueLoading(true);
    setAlbumCatalogLookupEnqueueError("");
    setAlbumCatalogLookupEnqueueResult(null);
    try {
      const payload = await enqueueCatalogBackfillItems(
        spotifyTrackIds.map((spotifyId) => ({
          entity_type: "track",
          spotify_id: spotifyId,
          reason: "track_lookup_visible_incomplete",
          priority: 80,
        })),
      );
      setAlbumCatalogLookupEnqueueResult(payload);
      await loadCatalogBackfillQueue(true);
    } catch (error) {
      setAlbumCatalogLookupEnqueueError(formatUiErrorMessage(error, "Failed to prioritize tracks."));
    } finally {
      setAlbumCatalogLookupEnqueueLoading(false);
    }
  }

  async function runCatalogBackfill() {
    if (catalogBackfillRunLoading) {
      return;
    }
    setCatalogBackfillRunLoading(true);
    setCatalogBackfillRunError("");
    try {
      const result = await postCatalogBackfillRun();
      setCatalogBackfillLatestResult(result);
      await Promise.all([loadCatalogBackfillCoverage(true), loadCatalogBackfillRuns(true), loadCatalogBackfillQueue(true)]);
    } catch (error) {
      setCatalogBackfillRunError(formatUiErrorMessage(error, "Catalog backfill failed."));
    } finally {
      setCatalogBackfillRunLoading(false);
    }
  }

  async function refreshRecentSection(targetRange: RecentRange = recentRange) {
    if (experienceMode === "full" && spotifyCooldownActive) {
      setStatusMessage(formatCooldownCopy(reloadSecondsRemaining));
      setStatusHistory((current) => [...current, "Spotify cooldown active. Recent refresh paused."]);
      return;
    }
    if (loadingRecentSection) {
      return;
    }
    setLoadingRecentSection(true);
    setStatusMessage("Refreshing recent sections...");
    try {
      const data = await fetchRecentSections(targetRange);
      setProfile((current) =>
        current
          ? {
              ...current,
              recent_range: data.recent_range,
              recent_window_days: data.recent_window_days,
              recent_top_artists: data.recent_top_artists,
              recent_top_artists_available: data.recent_top_artists_available,
              recent_top_tracks: data.recent_top_tracks,
              recent_top_tracks_available: data.recent_top_tracks_available,
              recent_top_albums: data.recent_top_albums,
              recent_top_albums_available: data.recent_top_albums_available,
              recent_tracks: data.recent_tracks,
              recent_tracks_available: data.recent_tracks_available,
              recent_likes_tracks: data.recent_likes_tracks,
              recent_likes_available: data.recent_likes_available,
            }
          : current,
      );
      if (targetRange !== recentRange) {
        setRecentRange(targetRange);
      }
      setStatusMessage("");
      setStatusHistory((current) => [...current, "Recent sections refreshed."]);
    } catch (error) {
      const message = formatUiErrorMessage(error, "Failed to refresh recent sections.");
      setStatusMessage(message);
      setStatusHistory((current) => [...current, `Recent refresh error: ${message}`]);
    } finally {
      setLoadingRecentSection(false);
    }
  }

  const showLoadingScreen = (authTransitioning || session?.authenticated || experienceMode === "local") && !profile;
  const heroTitle = "ListenLab";
  const heroCopy =
    "Connect your account and browse the listening, library, and profile details Spotify already makes available to ListenLab.";

  if (showLoadingScreen) {
    return renderLoadingScreen();
  }

  return (
    <>
      {profile && loadingExtendedProfile ? renderFullAnalysisOverlay() : null}
      <main className="app-shell">
        <section className="hero-card">
        {!profile ? (
          <div className="top-bar">
            <div className="top-copy">
              <p className="eyebrow">ListenLab</p>
              <h1>{heroTitle}</h1>
              <p className="lede three-line-clamp">{heroCopy}</p>
            </div>

            <div className="top-side">
              {renderExperienceModeToggle()}
              <button className="primary-button top-login-button" onClick={handleAuthAction} type="button">
                {experienceMode === "local" ? "Open restricted local mode" : "Log in with Spotify"}
              </button>
              {experienceMode === "full" ? (
                <button className="secondary-button top-login-button" onClick={startRecentIngestLogin} type="button">
                  Connect Spotify and ingest recent plays
                </button>
              ) : null}
              {experienceMode === "full" ? (
                <button className="secondary-button top-login-button" onClick={() => void runRecentBeforeProbe()} type="button">
                  Probe recent API before 90 days
                </button>
              ) : null}
              {experienceMode === "full" ? (
                <button className="secondary-button top-login-button" onClick={() => void runRecentBackfillProbe()} type="button">
                  Probe recent API paging (50 x up to 10)
                </button>
              ) : null}
              {recentIngestResult ? (
                <p className="empty-copy">
                  {recentIngestResult.auth_succeeded && recentIngestResult.ingest_succeeded
                    ? `Recent ingest succeeded: ${recentIngestResult.row_count ?? 0} rows (${recentIngestResult.earliest_api_played_at ?? "n/a"} to ${recentIngestResult.latest_api_played_at ?? "n/a"}).`
                    : `Recent ingest failed: ${recentIngestResult.error ?? "unknown error"}`}
                </p>
              ) : null}
              {recentBeforeProbeResult ? (
                <p className="empty-copy">
                  {recentBeforeProbeResult.ok
                    ? `Before-90d probe: ${recentBeforeProbeResult.returned_items ?? 0} rows (${recentBeforeProbeResult.earliest_played_at ?? "n/a"} to ${recentBeforeProbeResult.latest_played_at ?? "n/a"}).`
                    : `Before-90d probe failed: ${recentBeforeProbeResult.detail ?? "unknown error"}`}
                </p>
              ) : null}
              {recentBackfillProbeResult ? (
                <p className="empty-copy">
                  {recentBackfillProbeResult.ok
                    ? `Backfill probe: ${recentBackfillProbeResult.total_items ?? 0} items across ${recentBackfillProbeResult.pages_fetched ?? 0} pages (${recentBackfillProbeResult.earliest_played_at ?? "n/a"} to ${recentBackfillProbeResult.latest_played_at ?? "n/a"}).`
                    : `Backfill probe failed: ${recentBackfillProbeResult.detail ?? "unknown error"}`}
                </p>
              ) : null}
            </div>
          </div>
        ) : null}

        {!profile ? null : (
          <>
            <nav className="jump-links jump-links-sticky" aria-label="Dashboard sections">
              <div className="sticky-bar-left">
                <div className="profile-menu-shell profile-menu-shell-inline" ref={brandMenuRef}>
                  <button
                    aria-expanded={brandMenuOpen}
                    className="bar-trigger bar-trigger-brand"
                    onClick={() => {
                      setBrandMenuOpen((current) => !current);
                      setExperimentalMenuOpen(false);
                      setProfileMenuOpen(false);
                      setRateLimitMenuOpen(false);
                    }}
                    type="button"
                  >
                    <span className="brand-trigger-text" data-text="ListenLab">
                      ListenLab
                    </span>
                  </button>

                  {brandMenuOpen ? (
                    <section className="profile-card top-profile-card profile-menu-card brand-menu-card">
                      <div className="profile-panel-top">
                        <div>
                          <h2>ListenLab</h2>
                          <p className="empty-copy">
                            A local Spotify listening dashboard for exploring recent activity, favorites, albums, artists,
                            and playlists.
                          </p>
                        </div>
                      </div>

                      <div className="actions actions-right actions-in-card">
                        <a className="secondary-button bar-link-button" href={githubRepoUrl} rel="noreferrer" target="_blank">
                          View on GitHub
                        </a>
                      </div>
                    </section>
                  ) : null}
                </div>
                {showRateLimitReload ? (
                  <div className="profile-menu-shell profile-menu-shell-inline" ref={rateLimitMenuRef}>
                    <button
                      aria-expanded={rateLimitMenuOpen}
                      className={`bar-trigger bar-trigger-cooldown${reloadReady ? " bar-trigger-cooldown-ready" : ""}`}
                      onClick={() => {
                        setRateLimitMenuOpen((current) => !current);
                        setBrandMenuOpen(false);
                        setExperimentalMenuOpen(false);
                        setPlayerMenuOpen(false);
                        setProfileMenuOpen(false);
                      }}
                      type="button"
                    >
                      <span className="cooldown-chip-record" aria-hidden="true">
                        <span className="cooldown-chip-record-center" />
                      </span>
                      <span className="cooldown-chip-label">{reloadReady ? "Ready" : "Cooldown"}</span>
                    </button>

                    {rateLimitMenuOpen ? (
                      <section className="profile-card top-profile-card profile-menu-card rate-limit-menu-card">
                        <div className="profile-panel-top">
                          <div>
                            <h2>Spotify cooldown</h2>
                            <p className="empty-copy">
                              {reloadReady
                                ? "Cooldown completed. Spotify sync actions are available again."
                                : `Spotify requests are paused for ${formatCooldownTimerLabel(reloadSecondsRemaining)}.`}
                            </p>
                            <p className="empty-copy">Local sections stay available while cooldown is active.</p>
                          </div>
                        </div>
                        <div className="actions actions-right actions-in-card">
                          <button
                            className="secondary-button"
                            disabled={!reloadReady || loadingProfile || loadingRecentSection || loadingExtendedProfile}
                            onClick={() => {
                              setReloadCooldownUntil(null);
                              setReloadCooldownDurationMs(60_000);
                              setRateLimitMenuOpen(false);
                              void refreshRecentSection(recentRange);
                            }}
                            type="button"
                          >
                            Retry Spotify sync
                          </button>
                        </div>
                      </section>
                    ) : null}
                  </div>
                ) : null}
              </div>

              <div className="sticky-bar-center">
                <div className="profile-menu-shell profile-menu-shell-inline experimental-menu-shell" ref={experimentalMenuRef}>
                  <button
                    aria-expanded={experimentalMenuOpen}
                    aria-label="Experimental tools"
                    className="jump-link jump-link-icon"
                    onClick={() => {
                      setExperimentalMenuOpen((current) => !current);
                      setBrandMenuOpen(false);
                      setPlayerMenuOpen(false);
                      setProfileMenuOpen(false);
                      setRateLimitMenuOpen(false);
                    }}
                    type="button"
                  >
                    {"🧪"}
                  </button>
                  {experimentalMenuOpen ? (
                    <section className="profile-card top-profile-card profile-menu-card experimental-menu-card">
                      <div className="profile-panel-top">
                        <div>
                          <h2>Experimental</h2>
                          <p className="empty-copy">Tools for inspecting the ranking and identity work in progress.</p>
                        </div>
                      </div>
                      <div className="experimental-menu-actions">
                        <button className="secondary-button" onClick={openListeningLogPage} type="button">
                          Listen Log
                        </button>
                        <button className="secondary-button" onClick={openFormulaLabPage} type="button">
                          Formula Lab
                        </button>
                        <button className="secondary-button" onClick={openIdentityAuditPage} type="button">
                          Identity Audit
                        </button>
                        <button className="secondary-button" onClick={openCatalogBackfillPage} type="button">
                          Catalog Backfill
                        </button>
                        <button className="secondary-button" onClick={openSearchLookupPage} type="button">
                          Search / Lookup
                        </button>
                      </div>
                    </section>
                  ) : null}
                </div>
                {experienceMode === "full" ? (
                  <button className="jump-link" onClick={() => openAndScrollToSection("recent", "activity")} type="button">
                    Activity
                  </button>
                ) : null}
                <button className="jump-link" onClick={() => openAndScrollToSection("tracks", "tracks")} type="button">
                  Tracks
                </button>
                <button className="jump-link" onClick={() => openAndScrollToSection("artists", "artists")} type="button">
                  Artists
                </button>
                <button className="jump-link" onClick={() => openAndScrollToSection("albums", "albums")} type="button">
                  Albums
                </button>
                <button
                  className="jump-link"
                  onClick={() => openAndScrollToSection("playlists", "playlists")}
                  type="button"
                >
                  Playlists
                </button>
              </div>

              <div className="sticky-bar-right">
                {hasPremiumPlayback ? (
                <div className="profile-menu-shell profile-menu-shell-inline" ref={playerMenuRef}>
                  <button
                    aria-expanded={playerMenuOpen}
                    className="bar-trigger bar-trigger-player"
                    onClick={() => {
                      setPlayerMenuOpen((current) => !current);
                      setProfileMenuOpen(false);
                      setBrandMenuOpen(false);
                      setExperimentalMenuOpen(false);
                      setRateLimitMenuOpen(false);
                    }}
                    type="button"
                  >
                    <span className="toolbar-player-icon" aria-hidden="true">
                      {playerDisplayTrack?.image ? <img alt="" className="toolbar-player-cover" src={playerDisplayTrack.image} /> : null}
                      {playerDisplayTrack && !playerDisplayPaused ? (
                        <span className="detail-wave-icon">
                          <span />
                          <span />
                          <span />
                        </span>
                      ) : (
                        <span className="detail-play-icon">▶</span>
                      )}
                    </span>
                  </button>

                  {playerMenuOpen ? (
                    <section className="profile-card top-profile-card profile-menu-card player-menu-card">
                      <div className="player-menu-summary">
                        {playerDisplayTrack?.image ? (
                          <img alt={`${playerDisplayTrack.album} cover`} className="player-menu-image" src={playerDisplayTrack.image} />
                        ) : null}

                        <div className="player-menu-copy">
                          <div className="player-menu-copy-top">
                            <h2>
                              {usingLivePlaybackSnapshot && playerDisplayTrack ? (
                                <button
                                  className="player-menu-title-button single-line-ellipsis"
                                  onClick={() => openPlayerTrackDetails()}
                                  type="button"
                                >
                                  {playerDisplayTrack.name ?? "ListenLab Player"}
                                </button>
                              ) : (
                                <span className="single-line-ellipsis">{playerDisplayTrack?.name ?? "ListenLab Player"}</span>
                              )}
                            </h2>
                            {playerDisplayTrack?.uri ? (
                              <a
                                aria-label="Open in Spotify"
                                className="player-menu-external"
                                href={spotifyTrackUrl(playerDisplayTrack.uri) ?? undefined}
                                rel="noreferrer"
                                target="_blank"
                              >
                                {"\u2197"}
                              </a>
                            ) : null}
                          </div>
                          {playerDisplayArtistName ? (
                            <button
                              className="player-menu-meta-button player-menu-line single-line-ellipsis"
                              onClick={() => openPlayerArtistDetails()}
                              type="button"
                            >
                              {playerDisplayArtistName}
                            </button>
                          ) : (
                            <p className="player-menu-line single-line-ellipsis">
                              {playerDisplayTrack?.artists ?? "Spotify Premium playback"}
                            </p>
                          )}
                          {playerDisplayAlbumName ? (
                            <button
                              className="player-menu-meta-button player-menu-line player-menu-line-muted single-line-ellipsis"
                              onClick={() => openPlayerAlbumDetails()}
                              type="button"
                            >
                              {playerDisplayAlbumLabel}
                            </button>
                          ) : (
                            <p className="player-menu-line player-menu-line-muted single-line-ellipsis">
                              {playerDisplayAlbumLabel}
                            </p>
                          )}
                        </div>
                      </div>

                      {playerDisplayTrack ? (
                        <div className="player-progress" aria-label="Playback progress">
                          <input
                            aria-label="Seek playback"
                            className="player-progress-slider"
                            disabled={!canControlPlayback}
                            max={Math.max(playerDisplayDurationMs || playerDisplayTrack.durationMs || 0, 1)}
                            min={0}
                            onChange={(event) => setPendingSeekMs(Number(event.currentTarget.value))}
                            onMouseUp={() => {
                              if (canControlPlayback && pendingSeekMs != null) {
                                void seekPlayer(pendingSeekMs);
                              }
                            }}
                            onTouchEnd={() => {
                              if (canControlPlayback && pendingSeekMs != null) {
                                void seekPlayer(pendingSeekMs);
                              }
                            }}
                            step={1000}
                            title={usingLivePlaybackSnapshot ? livePlaybackControlTooltip : undefined}
                            type="range"
                            value={pendingSeekMs ?? playerDisplayPositionMs}
                          />
                          <div className="player-progress-times">
                            <span>{formatPlaybackClock(pendingSeekMs ?? playerDisplayPositionMs)}</span>
                            <span>{formatPlaybackClock(playerDisplayDurationMs || playerDisplayTrack.durationMs || 0)}</span>
                          </div>
                        </div>
                      ) : null}

                      <div className="actions actions-centered actions-in-card">
                        <span title={livePlaybackControlTooltip}>
                          <button
                            className={`primary-button${liveReadOnlyMode ? " primary-button-readonly" : ""}`}
                            disabled={!playerDisplayTrack || (!playerReady && !usingLivePlaybackSnapshot)}
                            onClick={() => handlePlayerPrimaryButtonClick()}
                            type="button"
                          >
                            {playerDisplayPaused ? "Play" : "Pause"}
                          </button>
                        </span>
                      </div>

                      {usingLivePlaybackSnapshot && liveAwaitingNextTrack ? (
                        <p className="empty-copy">Track ended. Checking for the next song...</p>
                      ) : null}
                      {playerError ? <p className="empty-copy">{playerError}</p> : null}
                      {!usingLivePlaybackSnapshot && !playerReady && !playerError ? <p className="empty-copy">Connecting to Spotify player...</p> : null}
                    </section>
                  ) : null}
                </div>
                ) : null}

                <div className="profile-menu-shell profile-menu-shell-inline" ref={profileMenuRef}>
                  <button
                    aria-expanded={profileMenuOpen}
                    className="bar-trigger bar-trigger-user"
                    onClick={() => {
                      setProfileMenuOpen((current) => !current);
                      setBrandMenuOpen(false);
                      setExperimentalMenuOpen(false);
                      setPlayerMenuOpen(false);
                      setRateLimitMenuOpen(false);
                    }}
                    type="button"
                  >
                    <span className="profile-username profile-username-nav single-line-ellipsis">
                      @{profile.username ?? "spotify-user"}
                      <span
                        aria-label={experienceMode === "local" ? "Restricted local mode" : "Full Spotify experience"}
                        className={`experience-mode-indicator${experienceMode === "local" ? " experience-mode-indicator-local" : ""}`}
                        title={experienceMode === "local" ? "Restricted local mode" : "Full Spotify experience"}
                      />
                    </span>
                  </button>

                  {profileMenuOpen ? (
                    <section className="profile-card top-profile-card profile-menu-card">
                      <div className="profile-panel-top">
                        <a
                          className="profile-identity"
                          href={profile.profile_url ?? undefined}
                          rel="noreferrer"
                          target="_blank"
                        >
                          {profile.image_url ? (
                            <img
                              alt={`${profile.display_name ?? "Spotify user"} profile`}
                              className="profile-image"
                              src={profile.image_url}
                            />
                          ) : (
                            <div className="profile-image profile-image-fallback" aria-hidden="true">
                              {(profile.display_name ?? "S").slice(0, 1).toUpperCase()}
                            </div>
                          )}

                          <div>
                            <h2 className="single-line-ellipsis">{profile.display_name ?? "Spotify user"}</h2>
                            <p className="profile-username single-line-ellipsis">@{profile.username ?? "spotify-user"}</p>
                            {formatListeningSince(profile.history_first_played_at) ? (
                              <p className="profile-history-line">{formatListeningSince(profile.history_first_played_at)}</p>
                            ) : null}
                            {experienceMode === "local" && formatRelativeSyncTime(profile.local_last_synced_at) ? (
                              <p className="profile-history-line">Last synced {formatRelativeSyncTime(profile.local_last_synced_at)}</p>
                            ) : null}
                          </div>
                        </a>
                      </div>
                      <div className="profile-mode-row">
                        {renderExperienceModeToggle()}
                        <button
                          aria-expanded={profileSettingsOpen}
                          className={`profile-settings-button${profileSettingsOpen ? " profile-settings-button-active" : ""}`}
                          onClick={() => setProfileSettingsOpen((current) => !current)}
                          type="button"
                        >
                          {"\u2699"}
                        </button>
                      </div>

                      {profileSettingsOpen ? (
                        <>
                          <div className="profile-settings-divider" aria-hidden="true" />
                          <div className="actions actions-right actions-in-card profile-settings-actions">
                            {analysisMode !== "full" && experienceMode === "full" ? (
                              <button
                                className="primary-button"
                                disabled={loadingExtendedProfile}
                                onClick={() => void loadFullAnalysis()}
                                type="button"
                              >
                                {loadingExtendedProfile ? "Loading full analysis..." : "Load full analysis"}
                              </button>
                            ) : null}
                            <button
                              className="secondary-button"
                              disabled={loadingHistoryRecompute || loadingExtendedProfile}
                              onClick={() => void recomputeHistoryFromLocal()}
                              type="button"
                            >
                              {loadingHistoryRecompute ? "Recomputing history..." : "Recompute from history"}
                            </button>
                            {experienceMode === "full" ? (
                              <button
                                className="primary-button"
                                onClick={handleAuthAction}
                                type="button"
                              >
                                Reconnect Spotify
                              </button>
                            ) : null}
                            <button className="secondary-button" onClick={() => void logout()} type="button">
                              Log out
                            </button>
                            {experienceMode === "full" ? (
                              <a
                                className="secondary-button bar-link-button"
                                href={spotifyAppsUrl}
                                rel="noreferrer"
                                target="_blank"
                              >
                                Revoke permissions
                              </a>
                            ) : null}
                          </div>
                        </>
                      ) : null}
                    </section>
                  ) : null}
                </div>
              </div>
            </nav>
            {appPage === "formulaLab" ? (
              <div className="dashboard-grid">
                {renderFormulaLabPage()}
              </div>
            ) : appPage === "identityAudit" ? (
              <div className="dashboard-grid">
                {renderIdentityAuditPage()}
              </div>
            ) : appPage === "recentDebug" ? (
              <div className="dashboard-grid">
                {renderRecentDebugPage()}
              </div>
            ) : appPage === "catalogBackfill" ? (
              <div className="dashboard-grid">
                {renderCatalogBackfillPage()}
              </div>
            ) : appPage === "searchLookup" ? (
              <div className="dashboard-grid">
                {renderSearchLookupPage()}
              </div>
            ) : (
            <div className="dashboard-grid">
              {renderDualSectionCard({
                title: renderSectionTitle("Activity", "recent_likes"),
                section: "recent",
                anchorId: "activity",
                leftTitle: (
                  <div className="section-column-header">
                    <h3>Recently played</h3>
                  </div>
                ),
                rightTitle: renderSectionTitle("Recently liked", "recent_likes"),
                leftContent: renderTrackColumn(
                  "recent",
                  profile.recent_tracks,
                  profile.recent_tracks_available,
                  "Spotify returned no recent listening history.",
                  recentUnavailableCopy(
                    "Recent listening is not available for this session yet. Log out and log back in to grant the updated Spotify permissions.",
                  ),
                  analysisMode === "quick" && experienceMode === "full" ? (
                    <button
                      className="secondary-button inline-reload-button"
                      disabled={loadingRecentSection}
                      onClick={() => void refreshRecentSection()}
                      type="button"
                    >
                      {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                    </button>
                  ) : null,
                ),
                rightContent: renderTrackColumn(
                  "likes",
                  profile.recent_likes_tracks,
                  profile.recent_likes_available,
                  "Spotify returned no recently liked tracks.",
                  recentUnavailableCopy(
                    "Liked tracks are not available for this session yet. Log out and log back in to grant library access.",
                  ),
                  analysisMode === "quick" && experienceMode === "full" ? (
                    <button
                      className="secondary-button inline-reload-button"
                      disabled={loadingRecentSection}
                      onClick={() => void refreshRecentSection()}
                      type="button"
                    >
                      {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                    </button>
                  ) : null,
                ),
                previewItemsLeft: previewItems(profile.recent_tracks),
                previewItemsRight: previewItems(profile.recent_likes_tracks),
                collapsedPreviewItems: previewItems(collapseRecentPreviewTracks(profile.recent_tracks)),
              })}

              {renderDualSectionCard({
                title: renderSectionTitle("Tracks"),
                section: "tracks",
                anchorId: "tracks",
                leftTitle: (
                  <div className="section-column-header">
                    <h3>All time</h3>
                    <div className="section-column-header-actions">
                      {renderTrackRankingToggle()}
                    </div>
                  </div>
                ),
                rightTitle: renderRecentRangeHeader(),
                leftContent: renderTrackColumn(
                  "tracksAllTime",
                  profile.top_tracks,
                  profile.top_tracks_available,
                  "Spotify returned no top tracks for this account.",
                  quickUnavailableCopy("Top tracks are not available for this session yet. Log out and log back in to grant access."),
                ),
                rightContent: renderTrackColumn(
                  "tracksRecent",
                  profile.recent_top_tracks,
                  profile.recent_top_tracks_available,
                  "Spotify returned no recent top tracks for this account.",
                  recentUnavailableCopy(
                    experienceMode === "local"
                      ? "Recent top tracks are unavailable in restricted local mode."
                      : "Recent top tracks are not available for this session yet. Log out and log back in to grant access.",
                  ),
                  analysisMode === "quick" && experienceMode === "full" ? (
                    <button
                      className="secondary-button inline-reload-button"
                      disabled={loadingRecentSection}
                      onClick={() => void refreshRecentSection()}
                      type="button"
                    >
                      {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                    </button>
                  ) : null,
                ),
                previewItemsLeft: previewItems(profile.top_tracks),
                previewItemsRight: previewItems(profile.recent_top_tracks),
                collapsedPreviewItems: previewItems(
                  collapseTrackPreviewAlbums([
                    ...profile.top_tracks,
                    ...profile.recent_top_tracks,
                  ]),
                ),
              })}

              {renderDualSectionCard({
                title: renderSectionTitle("Artists"),
                section: "artists",
                anchorId: "artists",
                leftTitle: "All time",
                rightTitle: renderRecentRangeHeader(),
                leftContent: renderArtistColumn(
                  "artistsAllTime",
                  profile.followed_artists,
                  profile.followed_artists_list_available,
                  "Spotify returned no top artists for this account.",
                  quickUnavailableCopy("Top artists are not available for this session yet. Log out and log back in to grant access."),
                ),
                rightContent: renderArtistColumn(
                  "artistsRecent",
                  profile.recent_top_artists,
                  profile.recent_top_artists_available,
                  "Spotify returned no recent top artists for this account.",
                  recentUnavailableCopy(
                    experienceMode === "local"
                      ? "Recent top artists are unavailable in restricted local mode."
                      : "Recent top artists are not available for this session yet. Log out and log back in to grant access.",
                  ),
                  analysisMode === "quick" && experienceMode === "full" ? (
                    <button
                      className="secondary-button inline-reload-button"
                      disabled={loadingRecentSection}
                      onClick={() => void refreshRecentSection()}
                      type="button"
                    >
                      {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                    </button>
                  ) : null,
                ),
                previewItemsLeft: previewItems(profile.followed_artists),
                previewItemsRight: previewItems(profile.recent_top_artists),
              })}

              {renderDualSectionCard({
                title: renderSectionTitle("Albums"),
                section: "albums",
                anchorId: "albums",
                leftTitle: "All time",
                rightTitle: renderRecentRangeHeader(),
                leftContent: renderAlbumColumn(
                  "albumsAllTime",
                  profile.top_albums,
                  profile.top_albums_available,
                  "Spotify returned no top albums for this account.",
                  quickUnavailableCopy("Top albums are not available for this session yet. Log out and log back in to grant access."),
                ),
                rightContent: renderAlbumColumn(
                  "albumsRecent",
                  profile.recent_top_albums,
                  profile.recent_top_albums_available,
                  "Spotify returned no recent top albums for this account.",
                  recentUnavailableCopy(
                    experienceMode === "local"
                      ? "Recent top albums are unavailable in restricted local mode."
                      : "Recent top albums are not available for this session yet. Log out and log back in to grant access.",
                  ),
                  analysisMode === "quick" && experienceMode === "full" ? (
                    <button
                      className="secondary-button inline-reload-button"
                      disabled={loadingRecentSection}
                      onClick={() => void refreshRecentSection()}
                      type="button"
                    >
                      {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                    </button>
                  ) : null,
                ),
                previewItemsLeft: previewItems(profile.top_albums),
                previewItemsRight: previewItems(profile.recent_top_albums),
              })}

              {renderPlaylistsSection()}
            </div>
            )}
          </>
        )}
        </section>
      </main>
      {selectedPreview ? (
        <div
          aria-modal="true"
          className="detail-modal-backdrop"
          onClick={() => setSelectedPreview(null)}
          role="dialog"
        >
          <section className="detail-modal" onClick={(event) => event.stopPropagation()}>
            {selectedPreview.url ? (
              <a
                aria-label="Open in Spotify"
                className="detail-modal-spotify-corner"
                href={selectedPreview.url}
                rel="noreferrer"
                target="_blank"
              >
                <img alt="" src={spotifyLogoDataUrl} />
              </a>
            ) : null}
            <div className="detail-modal-left">
              {selectedPreview.image ? (
                <img alt={selectedPreview.label} className="detail-modal-image" src={selectedPreview.image} />
              ) : (
                <div className="detail-modal-image detail-modal-image-fallback" aria-hidden="true">
                  {selectedPreview.fallbackLabel ?? selectedPreview.label.slice(0, 1).toUpperCase()}
                </div>
              )}
            </div>
            <div className="detail-modal-copy">
              <h2>{selectedPreview.label}</h2>
              {selectedPreview.meta ? (
                <div className="detail-modal-meta detail-modal-meta-with-image">
                  {selectedPreview.kind === "track" && selectedPreviewArtistImageUrl ? (
                    <img
                      alt=""
                      className="detail-modal-artist-image"
                      src={selectedPreviewArtistImageUrl}
                    />
                  ) : null}
                  {selectedPreviewCanOpenArtist ? (
                    <button
                      className="detail-modal-inline-link detail-modal-meta-text"
                      onClick={openSelectedTrackArtistPreview}
                      type="button"
                    >
                      {selectedPreviewPrimaryArtistName}
                    </button>
                  ) : (
                    <span className="detail-modal-meta-text">{selectedPreview.meta}</span>
                  )}
                </div>
              ) : null}
              {selectedPreview.detail && selectedPreview.kind !== "track" ? <p className="detail-modal-detail">{selectedPreview.detail}</p> : null}
              {selectedPreview.kind === "track" && !selectedPreviewEffectiveTrackUri ? (
                <p className="detail-modal-preview-missing">This track does not have a playable Spotify URI.</p>
              ) : null}
              {hasPremiumPlayback && (selectedPreview.kind === "artist" || selectedPreview.kind === "album") && representativeLoading ? (
                <p className="detail-modal-preview-missing">Finding a song to play...</p>
              ) : null}
              {hasPremiumPlayback && (selectedPreview.kind === "artist" || selectedPreview.kind === "album") && representativeTrack ? (
                <div className="detail-modal-audio">
                  <p className="detail-modal-audio-label">
                    {selectedPreview.kind === "artist" ? "Top song" : "Representative song"}
                  </p>
                  <p className="detail-modal-related-track">
                    {representativeTrack.track_name ?? "Unknown track"}
                    {representativeTrack.artist_name ? ` - ${representativeTrack.artist_name}` : ""}
                  </p>
                  {!representativeTrack.uri ? (
                    <p className="detail-modal-preview-missing">
                      {representativeReasonMessage(representativeReason, selectedPreview.kind)}
                    </p>
                  ) : null}
                </div>
              ) : null}
              {hasPremiumPlayback && (selectedPreview.kind === "artist" || selectedPreview.kind === "album") && !representativeLoading && !representativeTrack ? (
                <p className="detail-modal-preview-missing">
                  {representativeReasonMessage(representativeReason, selectedPreview.kind)}
                </p>
              ) : null}
              <div className="actions actions-in-card detail-modal-actions">
                {hasPremiumPlayback && selectedPreview.kind === "track" && selectedPreviewEffectiveTrackUri ? (
                  <div className={`detail-top-play-control${overlayTrackPlaybackExpanded ? " detail-top-play-control-expanded" : ""}`}>
                    <button
                      aria-label={isTrackPlaying(selectedPreviewEffectiveTrackUri) ? "Pause in ListenLab" : "Play in ListenLab"}
                      className={`secondary-button detail-icon-button detail-top-play-toggle${isTrackPlaying(selectedPreviewEffectiveTrackUri) ? " detail-icon-button-playing" : ""}`}
                      onClick={() => handleSelectedPreviewTrackPlay(selectedPreviewEffectiveTrackUri)}
                      type="button"
                    >
                      <span className={`detail-top-play-glyph${isTrackPlaying(selectedPreviewEffectiveTrackUri) ? " detail-top-play-glyph-active" : ""}`} aria-hidden="true">
                        {isTrackPlaying(selectedPreviewEffectiveTrackUri) ? (
                          <span className="detail-pause-bars"><span /><span /></span>
                        ) : (
                          <span className="detail-play-icon">{"\u25B6"}</span>
                        )}
                      </span>
                    </button>
                    {overlayTrackPlaybackExpanded ? (
                      <>
                        <span className="detail-top-play-time detail-top-play-time-elapsed">
                          {formatPlaybackClock(overlaySeekMs ?? selectedPreviewTrackElapsedDisplayMs)}
                        </span>
                        <span className="detail-top-play-inline-progress">
                          <span className="detail-top-play-inline-progress-fill" style={{ width: `${selectedPreviewTrackProgressPercent}%` }} />
                          <span className={`detail-top-play-inline-wave${isTrackPlaying(selectedPreviewEffectiveTrackUri) ? " detail-top-play-inline-wave-active" : ""}`} />
                          <input
                            aria-label="Seek selected track"
                            className="detail-top-play-inline-slider"
                            disabled={!canSeekSelectedPreview}
                            max={Math.max(selectedPreviewTrackTotalDisplayMs, 1)}
                            min={0}
                            onChange={(event) => setOverlaySeekMs(Number(event.currentTarget.value))}
                            onMouseUp={() => {
                              if (canSeekSelectedPreview && overlaySeekMs != null) {
                                void seekPlayer(overlaySeekMs);
                              }
                            }}
                            onTouchEnd={() => {
                              if (canSeekSelectedPreview && overlaySeekMs != null) {
                                void seekPlayer(overlaySeekMs);
                              }
                            }}
                            step={1000}
                            type="range"
                            value={overlaySeekMs ?? selectedPreviewTrackElapsedDisplayMs}
                          />
                        </span>
                      </>
                    ) : null}
                    <span className="detail-top-play-time detail-top-play-time-total">
                      {formatPlaybackClock(selectedPreviewTrackTotalDisplayMs)}
                    </span>
                  </div>
                ) : null}
                {hasPremiumPlayback && (selectedPreview.kind === "artist" || selectedPreview.kind === "album") && representativeTrack?.uri ? (
                  <button
                    aria-label={isTrackPlaying(representativeTrack.uri ?? null) ? "Currently playing in ListenLab" : "Play in ListenLab"}
                    className={`secondary-button detail-icon-button${isTrackPlaying(representativeTrack.uri ?? null) ? " detail-icon-button-playing" : ""}`}
                    onClick={() => void handlePopupTrackPlayback(representativeTrack.uri ?? null)}
                    type="button"
                  >
                    {isTrackPlaying(representativeTrack.uri ?? null) ? (
                      <span className="detail-pause-bars" aria-hidden="true"><span /><span /></span>
                    ) : (
                      <span className="detail-play-icon" aria-hidden="true">{"\u25B6"}</span>
                    )}
                  </button>
                ) : null}
              </div>
            </div>
            {selectedPreview.kind === "track" || selectedPreview.kind === "album" ? (
              <div className="detail-modal-album-tracks detail-modal-album-tracks-full">
                {selectedPreview.kind === "track" && selectedPreviewCanOpenAlbum ? (
                  <button
                    className="detail-modal-inline-link detail-modal-album-title"
                    onClick={openSelectedTrackAlbumPreview}
                    type="button"
                  >
                    {previewAlbumHeading(selectedPreview)}
                  </button>
                ) : (
                  <p className="detail-modal-album-title">
                    {selectedPreview.kind === "album" ? selectedPreview.label : previewAlbumHeading(selectedPreview)}
                  </p>
                )}
                <div className="detail-modal-album-header" aria-hidden="true">
                  <span className="detail-modal-album-preview-header">Preview</span>
                  <span className="detail-modal-album-last-played-header">Played</span>
                </div>
                {albumTrackEntriesLoading ? (
                  <p className="detail-modal-preview-missing">Loading album songs...</p>
                ) : null}
                {!albumTrackEntriesLoading && albumTrackEntriesError ? (
                  <p className="detail-modal-preview-missing">{albumTrackEntriesError}</p>
                ) : null}
                {!albumTrackEntriesLoading && !albumTrackEntriesError && albumTrackEntries.length > 0 ? (
                  <ul className="detail-album-track-list">
                    {albumTrackEntries.map((track) => {
                      const rowTrackUri = track.uri ?? (track.id ? `spotify:track:${track.id}` : null);
                      const rowIsCurrentTrack = Boolean(rowTrackUri && currentTrack?.uri === rowTrackUri);
                      const rowPlaying = isTrackPlaying(rowTrackUri);
                      const rowPreviewPlaying = Boolean(rowTrackUri && previewingTrackUri === rowTrackUri);
                      const rowPreviewActive = Boolean(rowPreviewPlaying && rowPlaying);
                      const rowPreviewKey = albumTrackPreviewKey(track, rowTrackUri);
                      const rowPreviewPlayed = previewPlayedTrackKeys.has(rowPreviewKey);
                      const rowPausedCurrent = Boolean(rowIsCurrentTrack && playbackPaused);
                      const rowLastPlayed = formatMonthDay(track.lastPlayedAt);
                      const rowBaseDurationMs = (
                        track.durationMs
                        ?? (rowIsCurrentTrack
                          ? (playbackDurationMs > 0 ? playbackDurationMs : currentTrack?.durationMs ?? null)
                          : null)
                      );
                      const rowElapsedMs = rowIsCurrentTrack
                        ? (
                          rowBaseDurationMs != null
                            ? Math.min(Math.max(0, playbackPositionMs), rowBaseDurationMs)
                            : Math.max(0, playbackPositionMs)
                        )
                        : null;
                      const rowButtonTimeMs = rowIsCurrentTrack
                        ? (
                          rowPlaying
                            ? rowElapsedMs
                            : (rowPausedCurrent ? (pausedTimeFlashOn ? rowElapsedMs : rowBaseDurationMs) : rowBaseDurationMs)
                        )
                        : rowBaseDurationMs;
                      return (
                        <li className={`detail-album-track-row${track.isSelected ? " detail-album-track-row-selected" : ""}`} key={track.id ?? track.name}>
                          {hasPremiumPlayback ? (
                            <button
                              aria-label={rowPlaying ? "Currently playing in ListenLab" : rowTrackUri ? `Play ${track.name} in ListenLab` : `${track.name} is not playable`}
                              className={`secondary-button detail-album-track-play-button${rowPlaying ? " detail-icon-button-playing" : ""}`}
                              disabled={!rowTrackUri}
                              onClick={() => {
                                void handleAlbumTrackPlay(track, rowTrackUri);
                              }}
                              type="button"
                            >
                              {rowPlaying ? (
                                <span className="detail-wave-icon" aria-hidden="true">
                                  <span />
                                  <span />
                                  <span />
                                </span>
                              ) : (
                                <span className="detail-play-icon" aria-hidden="true">{"\u25B6"}</span>
                              )}
                              <span className={`detail-album-track-play-time${rowPausedCurrent ? " detail-album-track-play-time-flash" : ""}`}>
                                {rowButtonTimeMs != null ? formatPlaybackClock(rowButtonTimeMs) : "?:??"}
                              </span>
                            </button>
                          ) : null}
                          <button
                            className="detail-album-track-name-button single-line-ellipsis"
                            onClick={() => openAlbumTrackPreview(track)}
                            type="button"
                          >
                            {track.name}
                          </button>
                          <div className="detail-album-track-actions">
                            {hasPremiumPlayback ? (
                              <button
                                aria-label={rowPreviewPlaying ? `Stop preview for ${track.name}` : `Preview ${track.name}`}
                                className={`detail-album-track-preview-button${rowPreviewActive ? " detail-album-track-preview-button-active" : ""}${rowPreviewPlayed ? " detail-album-track-preview-button-played" : ""}`}
                                disabled={!rowTrackUri}
                                onClick={() => {
                                  void toggleAlbumTrackPreview(track, rowTrackUri);
                                }}
                                type="button"
                              />
                            ) : (
                              <span className="detail-album-track-preview-placeholder" aria-hidden="true" />
                            )}
                            {rowLastPlayed ? <span className="detail-album-track-last-played">{rowLastPlayed}</span> : <span className="detail-album-track-last-played">-</span>}
                          </div>
                        </li>
                      );
                    })}
                  </ul>
                ) : null}
              </div>
            ) : null}
          </section>
        </div>
      ) : null}
    </>
  );
}
