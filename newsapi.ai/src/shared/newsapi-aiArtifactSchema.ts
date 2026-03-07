export type NewsApiAiArtifactRow = {
  uri: string;
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
};

const HEADERS: Array<keyof NewsApiAiArtifactRow> = [
  "uri",
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
