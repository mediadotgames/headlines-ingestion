export type NewsApiAiArtifactRow = {
  uri: string;
  run_type: string;
  nth_run: string;
  url: string;
  title: string;
  body: string;
  date: string;
  time: string;
  date_time: string;
  date_time_published: string;
  lang: string;
  is_duplicate: string;
  data_type: string;
  sentiment: string;
  event_uri: string;
  relevance: string;
  story_uri: string;
  image: string;
  source: string;
  authors: string;
  sim: string;
  wgt: string;
  categories: string | null;
  concepts: string | null;
  links: string | null;
  videos: string | null;
  shares: string | null;
  duplicate_list: string | null;
  extracted_dates: string | null;
  location: string | null;
  original_article: string | null;
  raw_article: string | null;
};

export const ARTIFACT_CONTRACT_VERSION = "newsapi-ai/v3";

const HEADERS: Array<keyof NewsApiAiArtifactRow> = [
  "uri",
  "run_type",
  "nth_run",
  "url",
  "title",
  "body",
  "date",
  "time",
  "date_time",
  "date_time_published",
  "lang",
  "is_duplicate",
  "data_type",
  "sentiment",
  "event_uri",
  "relevance",
  "story_uri",
  "image",
  "source",
  "authors",
  "sim",
  "wgt",
  "categories",
  "concepts",
  "links",
  "videos",
  "shares",
  "duplicate_list",
  "extracted_dates",
  "location",
  "original_article",
  "raw_article",
];

export function csvHeader(): string {
  return HEADERS.join(",");
}

function escapeCsvCell(value: string): string {
  if (/[",\n\r]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

export function toCsvRow(row: NewsApiAiArtifactRow): string {
  return HEADERS.map((h) => escapeCsvCell(row[h] ?? "")).join(",");
}