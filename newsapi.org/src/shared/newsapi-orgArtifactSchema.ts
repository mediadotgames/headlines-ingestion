export const ARTICLE_CSV_COLUMNS = [
    "source_id",
    "source_name",
    "author",
    "title",
    "description",
    "url",
    "url_to_image",
    "published_at",
    "content",
  ] as const;
  
  export type ArticleCsvColumn = (typeof ARTICLE_CSV_COLUMNS)[number];
  
  export type ArticleCsvRow = {
    source_id: string;
    source_name: string;
    author: string;
    title: string;
    description: string;
    url: string;
    url_to_image: string;
    published_at: string;
    content: string;
  };
  
  export function escapeCsv(value: string | null | undefined): string {
    if (value == null) return "";
  
    let v = String(value);
  
    // Keep CSV to one physical line per record.
    v = v.replace(/\r\n/g, " ").replace(/\n/g, " ").replace(/\r/g, " ");
  
    if (v.includes('"') || v.includes(",") || v.includes("\n") || v.includes("\r")) {
      return `"${v.replace(/"/g, '""')}"`;
    }
  
    return v;
  }
  
  export function toCsvRow(row: ArticleCsvRow): string {
    return ARTICLE_CSV_COLUMNS.map((col) => escapeCsv(row[col])).join(",");
  }
  
  export function csvHeader(): string {
    return ARTICLE_CSV_COLUMNS.join(",");
  }