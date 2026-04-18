import { type ReactNode, useEffect, useRef, useState } from "react";

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
  track_id: string | null;
  track_name: string | null;
  artist_name: string | null;
  album_name: string | null;
  album_release_year?: string | null;
  uri?: string | null;
  preview_url?: string | null;
  url?: string | null;
  image_url?: string | null;
  album_id?: string | null;
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

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "/api";
const githubRepoUrl = "https://github.com/moshe-kahn/listen-labs";
const EXPERIENCE_MODE_STORAGE_KEY = "listenlab-experience-mode";
const LIVE_PLAYBACK_POLL_INTERVAL_MS = 10_000;
const LIVE_PLAYBACK_PROGRESS_TICK_MS = 500;
const LIVE_CONTROL_DOUBLE_TAP_WINDOW_MS = 900;
const LIVE_TRACK_END_RECENT_POLL_DELAY_MS = 3_500;
const PAGE_SIZE = 5;
const RECENT_SECTION_FETCH_LIMIT = 10;
const PLAYLISTS_PAGE_SIZE = 10;
const RECENT_RANGE_OPTIONS = [
  { value: "short_term", label: "4 weeks" },
  { value: "medium_term", label: "6 months" },
] as const;
type RecentRange = (typeof RECENT_RANGE_OPTIONS)[number]["value"];
type AnalysisMode = "quick" | "full";
type ExperienceMode = "full" | "local";
type ExperienceVisualMode = ExperienceMode | "test";
type TrackRankingMode = "plays" | "mix" | "longevity";
type AppPage = "dashboard" | "tracksOnly";
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
  togglePlay: () => Promise<void>;
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
  const playerMenuRef = useRef<HTMLDivElement | null>(null);
  const rateLimitMenuRef = useRef<HTMLDivElement | null>(null);
  const spotifyPlayerRef = useRef<SpotifyPlayerInstance | null>(null);
  const spotifyDeviceIdRef = useRef<string | null>(null);
  const liveProgressAnchorRef = useRef<{ baseProgressMs: number; receivedAtMs: number; durationMs: number } | null>(null);
  const liveEndRefreshRequestedRef = useRef(false);
  const liveControlTapMsRef = useRef(0);
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
    && livePlaybackSnapshot?.device_id
    && spotifyDeviceIdRef.current
    && livePlaybackSnapshot.device_id === spotifyDeviceIdRef.current,
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
  const livePlaybackControlTooltip = liveReadOnlyMode
    ? `Playing on ${livePlaybackSnapshot?.device_name ?? "another device"}. Double tap to switch.`
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
    setAlbumTrackEntries([]);
    setAlbumTrackEntriesLoading(false);
    setAlbumTrackEntriesError(null);

    if (experienceMode === "local" || !selectedPreview || selectedPreview.kind !== "track" || spotifyCooldownActive) {
      return () => {
        cancelled = true;
      };
    }

    const selectedTrackId = selectedPreview.trackId ?? selectedPreview.entityId;
    const albumId = selectedPreview.albumId ?? selectedPreview.sourceTrack?.album_id ?? null;
    if (!albumId) {
      setAlbumTrackEntriesError("Album track list is unavailable for this item.");
      return () => {
        cancelled = true;
      };
    }
    const albumIdSafe = albumId;

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
            artists?: Array<{ name?: string | null }>;
          }>;
        };
        const rows = (payload.items ?? []).map((item) => {
          const id = item.id ?? null;
          const artistNames = (item.artists ?? []).map((artist) => artist.name ?? "").filter(Boolean).join(", ");
          const normalizedKey = normalizedTrackArtistKey(item.name ?? null, artistNames || null);
          const isTopTrack = Boolean((id && topTrackIds.has(id)) || normalizedTopTrackKeys.has(normalizedKey));
          return {
            id,
            name: item.name ?? "Unknown track",
            uri: item.uri ?? null,
            isSelected: Boolean(selectedTrackId && id && selectedTrackId === id),
            isTopTrack,
          } satisfies AlbumTrackEntry;
        });

        if (!cancelled) {
          setAlbumTrackEntries(rows);
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
  }, [experienceMode, profile?.recent_top_tracks, profile?.top_tracks, selectedPreview, spotifyCooldownActive]);

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

  function spotifyTrackIdFromUri(trackUri: string | null) {
    if (!trackUri?.startsWith("spotify:track:")) {
      return null;
    }
    const trackId = trackUri.split(":")[2];
    return trackId || null;
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
      image: playerDisplayTrack.image,
      label: playerDisplayTrack.name,
      meta: playerDisplayTrack.artists || null,
      detail: playerDisplayTrack.album || null,
      kind: "track",
      entityId: trackId,
      trackUri: playerDisplayTrack.uri,
      url: trackUrl,
      trackId,
      albumId: null,
      artistName: playerDisplayTrack.artists || null,
      sourceTrack: null,
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
          volume: 0.8,
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
        player.addListener("playback_error", ({ message }: { message: string }) => setPlayerError(message));

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

  async function playTrackUri(trackUri: string | null) {
    if (!trackUri) {
      setPlayerError("This item does not have a playable Spotify track.");
      return;
    }
    const deviceId = spotifyDeviceIdRef.current;
    if (!deviceId) {
      setPlayerError("ListenLab Player is not ready yet.");
      return;
    }

    try {
      await spotifyApiRequest("/me/player", {
        method: "PUT",
        body: JSON.stringify({ device_ids: [deviceId], play: false }),
      });
      await spotifyApiRequest(`/me/player/play?device_id=${encodeURIComponent(deviceId)}`, {
        method: "PUT",
        body: JSON.stringify({
          uris: [trackUri],
          position_ms: 0,
        }),
      });
      setPlayerError(null);
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be started.");
    }
  }

  async function togglePlayerPlayback() {
    try {
      if (
        playbackPaused
        && currentTrack?.uri
        && (playbackDurationMs <= 0 || currentTrack.durationMs <= 0)
      ) {
        await playTrackUri(currentTrack.uri);
        return;
      }
      if (spotifyPlayerRef.current) {
        await spotifyPlayerRef.current.togglePlay();
      }
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be updated.");
    }
  }

  async function takeOverPlaybackFromLiveSnapshot() {
    const deviceId = spotifyDeviceIdRef.current;
    if (!deviceId) {
      setPlayerError("ListenLab Player is not ready yet.");
      return;
    }

    try {
      await spotifyApiRequest("/me/player", {
        method: "PUT",
        body: JSON.stringify({ device_ids: [deviceId], play: true }),
      });
      if (playerDisplayTrack?.uri) {
        await spotifyApiRequest(`/me/player/play?device_id=${encodeURIComponent(deviceId)}`, {
          method: "PUT",
          body: JSON.stringify({
            uris: [playerDisplayTrack.uri],
            position_ms: Math.max(0, Math.floor(playerDisplayPositionMs)),
          }),
        });
      }
      setPlayerError(null);
      setLiveControlOverrideUntilMs(Date.now() + LIVE_PLAYBACK_POLL_INTERVAL_MS);
      await loadCurrentPlaybackSnapshot();
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be switched.");
    }
  }

  function handlePlayerPrimaryButtonClick() {
    if (!liveReadOnlyMode) {
      void togglePlayerPlayback();
      return;
    }
    const now = Date.now();
    if ((now - liveControlTapMsRef.current) <= LIVE_CONTROL_DOUBLE_TAP_WINDOW_MS) {
      liveControlTapMsRef.current = 0;
      void takeOverPlaybackFromLiveSnapshot();
      return;
    }
    liveControlTapMsRef.current = now;
    setPlayerError(`Double tap to switch from ${livePlaybackSnapshot?.device_name ?? "another device"} to ListenLab Player.`);
  }

  async function handlePopupTrackPlayback(trackUri: string | null) {
    if (!trackUri) {
      setPlayerError("This item does not have a playable Spotify track.");
      return;
    }

    const player = spotifyPlayerRef.current;
    const isCurrent = currentTrack?.uri === trackUri;

    try {
      if (isCurrent && !playbackPaused && player) {
        await player.pause();
        setPlaybackPaused(true);
        return;
      }

      if (isCurrent && playbackPaused && player) {
        await player.resume();
        setPlaybackPaused(false);
        return;
      }

      await playTrackUri(trackUri);
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be updated.");
    }
  }

  async function seekPlayer(positionMs: number) {
    const player = spotifyPlayerRef.current;
    if (!player) {
      setPlayerError("ListenLab Player is not ready yet.");
      return;
    }

    try {
      await player.seek(positionMs);
      setPlaybackPositionMs(positionMs);
      setPendingSeekMs(null);
      setPlayerError(null);
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback position could not be updated.");
    }
  }

  function isTrackPlaying(trackUri: string | null) {
    return Boolean(trackUri && currentTrack?.uri === trackUri && !playbackPaused);
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

  function normalizedTrackArtistKey(trackName: string | null | undefined, artistName: string | null | undefined) {
    return `${(trackName ?? "").trim().toLowerCase()}::${(artistName ?? "").trim().toLowerCase()}`;
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

  function openAndScrollToSection(section: SectionKey, anchorId: string) {
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
    return Math.sqrt(normalized);
  }

  function getOldLongevityScore(track: RecentTrack, maxLongevity: number): number {
    return getTrackLongevityScore(track) / Math.max(1, maxLongevity);
  }

  function getNewLongevityScore(track: RecentTrack, maxLongevity: number): number {
    const normalized = getTrackLongevityScore(track) / Math.max(1, maxLongevity);
    return Math.sqrt(normalized);
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
            trackUri: trackUri ?? null,
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

    const rankedItems = sortedTracksForView(section, items);
    const isAllTimeTrackSection =
      section === "tracksAllTime" ||
      section === "tracksAllTimeCurrent" ||
      section === "tracksAllTimeNew";

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
                primaryBadgeText: row.hiddenCount > 0 ? `+${row.hiddenCount} more` : null,
                secondaryText: row.track.artist_name ?? "Unknown artist",
                tertiaryText: row.track.album_name ?? "Unknown album",
                metricText: isAllTimeTrackSection
                  ? (
                      section === "tracksAllTimeNew"
                        ? `${row.track.play_count ?? 0} | ${formatTrackLongevity(row.track) ?? "0d"}`
                        : (
                            trackRankingMode === "longevity"
                              ? formatTrackLongevityMetric(row.track)
                              : (trackRankingMode === "mix"
                                ? (
                                    (row.track.play_count != null && row.track.play_count > 0) || (row.track.listening_span_days != null && row.track.listening_span_days > 0)
                                      ? `${row.track.play_count ?? 0} | ${formatTrackLongevity(row.track) ?? "0d"}`
                                      : null
                                  )
                                : (row.track.play_count != null && row.track.play_count > 0 ? `${row.track.play_count}` : null))
                          )
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

  function renderTracksOnlyPage() {
  if (!profile) {
    return null;
  }

  return (
    <section className="info-card info-card-wide tracks-only-card" id="tracks-page">
      <div className="tracks-only-header">
        <div className="section-column-header">
          <h2>Tracks</h2>
          {renderTrackRankingToggle()}
        </div>
        <button
          className="secondary-button tracks-only-back-button"
          onClick={() => setAppPage("dashboard")}
          type="button"
        >
          Back to dashboard
        </button>
      </div>
      <div className="artists-grid">
        <div className="artists-column">
          <h3>Current formula</h3>
          {renderTrackColumn(
            "tracksAllTimeCurrent",
            profile.top_tracks,
            profile.top_tracks_available,
            "Spotify returned no top tracks for this account.",
            quickUnavailableCopy("Top tracks are not available for this session yet. Log out and log back in to grant access."),
            undefined,
            false,
          )}
        </div>
        <div className="artists-column">
          <h3>New formula</h3>
          {renderTrackColumn(
            "tracksAllTimeNew",
            profile.top_tracks,
            profile.top_tracks_available,
            "Spotify returned no top tracks for this account.",
            quickUnavailableCopy("Top tracks are not available for this session yet. Log out and log back in to grant access."),
            undefined,
            false,
          )}
        </div>
      </div>
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
                          <p className="player-menu-line single-line-ellipsis">
                            {playerDisplayTrack?.artists ?? "Spotify Premium playback"}
                          </p>
                          <p className="player-menu-line player-menu-line-muted single-line-ellipsis">
                            {playerDisplayTrack?.album ?? "Choose something to play"}
                          </p>
                        </div>
                      </div>

                      {playerDisplayTrack ? (
                        <div className="player-progress" aria-label="Playback progress">
                          <input
                            aria-label="Seek playback"
                            className="player-progress-slider"
                            disabled={liveReadOnlyMode}
                            max={Math.max(playerDisplayDurationMs || playerDisplayTrack.durationMs || 0, 1)}
                            min={0}
                            onChange={(event) => setPendingSeekMs(Number(event.currentTarget.value))}
                            onMouseUp={() => {
                              if (!liveReadOnlyMode && pendingSeekMs != null) {
                                void seekPlayer(pendingSeekMs);
                              }
                            }}
                            onTouchEnd={() => {
                              if (!liveReadOnlyMode && pendingSeekMs != null) {
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
                            disabled={!playerDisplayTrack || !playerReady}
                            onClick={() => handlePlayerPrimaryButtonClick()}
                            onDoubleClick={() => {
                              if (liveReadOnlyMode) {
                                liveControlTapMsRef.current = 0;
                                void takeOverPlaybackFromLiveSnapshot();
                              }
                            }}
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
            {appPage === "tracksOnly" ? (
              <div className="dashboard-grid">
                {renderTracksOnlyPage()}
              </div>
            ) : (
            <div className="dashboard-grid">
              {renderDualSectionCard({
                title: renderSectionTitle("Activity", "recent_likes"),
                section: "recent",
                anchorId: "activity",
                leftTitle: "Recently played",
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
                      <button
                        className="secondary-button tracks-page-link-button"
                        onClick={() => setAppPage("tracksOnly")}
                        type="button"
                      >
                        Open tracks page
                      </button>
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
            {selectedPreview.image ? (
              <img alt={selectedPreview.label} className="detail-modal-image" src={selectedPreview.image} />
            ) : (
              <div className="detail-modal-image detail-modal-image-fallback" aria-hidden="true">
                {selectedPreview.fallbackLabel ?? selectedPreview.label.slice(0, 1).toUpperCase()}
              </div>
            )}
            <div className="detail-modal-copy">
              <h2>{selectedPreview.label}</h2>
              {selectedPreview.meta ? <p className="detail-modal-meta">{selectedPreview.meta}</p> : null}
              {selectedPreview.detail ? <p className="detail-modal-detail">{selectedPreview.detail}</p> : null}
              {selectedPreview.kind === "track" && !selectedPreview.trackUri ? (
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
                {hasPremiumPlayback && selectedPreview.kind === "track" && selectedPreview.trackUri ? (
                  <button
                    aria-label={isTrackPlaying(selectedPreview.trackUri) ? "Currently playing in ListenLab" : "Play in ListenLab"}
                    className={`secondary-button detail-icon-button${isTrackPlaying(selectedPreview.trackUri) ? " detail-icon-button-playing" : ""}`}
                    onClick={() => void handlePopupTrackPlayback(selectedPreview.trackUri)}
                    type="button"
                  >
                    {isTrackPlaying(selectedPreview.trackUri) ? (
                      <span className="detail-wave-icon" aria-hidden="true">
                        <span />
                        <span />
                        <span />
                      </span>
                    ) : (
                      <span className="detail-play-icon" aria-hidden="true">▶</span>
                    )}
                  </button>
                ) : null}
                {hasPremiumPlayback && (selectedPreview.kind === "artist" || selectedPreview.kind === "album") && representativeTrack?.uri ? (
                  <button
                    aria-label={isTrackPlaying(representativeTrack.uri ?? null) ? "Currently playing in ListenLab" : "Play in ListenLab"}
                    className={`secondary-button detail-icon-button${isTrackPlaying(representativeTrack.uri ?? null) ? " detail-icon-button-playing" : ""}`}
                    onClick={() => void handlePopupTrackPlayback(representativeTrack.uri ?? null)}
                    type="button"
                  >
                    {isTrackPlaying(representativeTrack.uri ?? null) ? (
                      <span className="detail-wave-icon" aria-hidden="true">
                        <span />
                        <span />
                        <span />
                      </span>
                    ) : (
                      <span className="detail-play-icon" aria-hidden="true">▶</span>
                    )}
                  </button>
                ) : null}
                <a
                  className="primary-button bar-link-button"
                  href={selectedPreview.url}
                  rel="noreferrer"
                  target="_blank"
                >
                  Open in Spotify
                </a>
              </div>
              {selectedPreview.kind === "track" ? (
                <div className="detail-modal-album-tracks">
                  <p className="detail-modal-audio-label">Album songs</p>
                  {albumTrackEntriesLoading ? (
                    <p className="detail-modal-preview-missing">Loading album songs...</p>
                  ) : null}
                  {!albumTrackEntriesLoading && albumTrackEntriesError ? (
                    <p className="detail-modal-preview-missing">{albumTrackEntriesError}</p>
                  ) : null}
                  {!albumTrackEntriesLoading && !albumTrackEntriesError && albumTrackEntries.length > 0 ? (
                    <ul className="detail-album-track-list">
                      {albumTrackEntries.map((track) => (
                        <li className={`detail-album-track-row${track.isSelected ? " detail-album-track-row-selected" : ""}`} key={track.id ?? track.name}>
                          <span className="detail-album-track-name single-line-ellipsis">{track.name}</span>
                          {track.isTopTrack && !track.isSelected ? <span className="detail-album-track-star" aria-label="Other top track">*</span> : null}
                        </li>
                      ))}
                    </ul>
                  ) : null}
                </div>
              ) : null}
            </div>
          </section>
        </div>
      ) : null}
    </>
  );
}
