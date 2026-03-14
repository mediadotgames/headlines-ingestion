// eventArtifactSchema.ts

export const EVENT_ARTIFACT_CONTRACT_VERSION = "newsapi-ai-events/v1" as const;
export const EVENT_INGESTION_SOURCE = "newsapi-ai-events" as const;
export const EVENT_DATA_FILE = "events.csv" as const;
export const EVENT_MANIFEST_FILE = "event-manifest.json" as const;
export const EVENT_LOAD_REPORT_FILE = "event-load-report.json" as const;

export const EVENT_DISCOVERY_STAGES = [
  "article_collect",
  "article_load",
  "event_collect",
  "event_load",
] as const;

export type EventPipelineStage = (typeof EVENT_DISCOVERY_STAGES)[number];

export const EVENT_DISCOVERY_SCOPES = [
  "parent_run",
  "time_window",
  "full_corpus",
  "custom_filter",
] as const;

export type EventDiscoveryScope = (typeof EVENT_DISCOVERY_SCOPES)[number];

export const EVENT_DISCOVERY_TIME_COLUMNS = [
  "ingested_at",
  "date_time_published",
  "created_at",
] as const;

export type EventDiscoveryTimeColumn =
  (typeof EVENT_DISCOVERY_TIME_COLUMNS)[number];

export type EventArtifactRow = {
  uri: string;
  run_type: string;
  nth_run: string;
  total_article_count: string;
  relevance: string;
  event_date: string;
  sentiment: string;
  social_score: string;
  article_counts: string | null;
  title: string | null;
  summary: string | null;
  concepts: string | null;
  categories: string | null;
  common_dates: string | null;
  location: string | null;
  stories: string | null;
  images: string | null;
  wgt: string | null;
  raw_event: string;
};

export const EVENT_CSV_HEADERS: Array<keyof EventArtifactRow> = [
  "uri",
  "run_type",
  "nth_run",
  "total_article_count",
  "relevance",
  "event_date",
  "sentiment",
  "social_score",
  "article_counts",
  "title",
  "summary",
  "concepts",
  "categories",
  "common_dates",
  "location",
  "stories",
  "images",
  "wgt",
  "raw_event",
];

export function csvHeader(): string {
  return EVENT_CSV_HEADERS.join(",");
}

function escapeCsvCell(value: string): string {
  if (/[",\n\r]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

export function toCsvRow(row: EventArtifactRow): string {
  return EVENT_CSV_HEADERS.map((h) => escapeCsvCell(row[h] ?? "")).join(",");
}
