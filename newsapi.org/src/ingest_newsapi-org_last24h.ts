import "dotenv/config";
import { Client } from "pg";

const NEWSAPI_KEY = process.env.NEWSAPI_KEY!;
const SOURCES = process.env.NEWSAPI_SOURCES!;
const DATABASE_URL = process.env.DATABASE_URL!;

interface NewsApiSource {
  id: string | null;
  name: string;
}

interface NewsApiArticle {
  source: NewsApiSource;
  author: string | null;
  title: string;
  description: string | null;
  url: string;
  urlToImage: string | null;
  publishedAt: string;
  content: string | null;
}

interface NewsApiResponse {
  status: string;
  totalResults: number;
  articles: NewsApiArticle[];
}

async function fetchPage(
  fromIso: string,
  toIso: string,
  page: number,
): Promise<NewsApiResponse> {
  const url = new URL("https://newsapi.org/v2/everything");

  url.searchParams.set("sources", SOURCES);
  url.searchParams.set("from", fromIso);
  url.searchParams.set("to", toIso);
  url.searchParams.set("sortBy", "publishedAt");
  url.searchParams.set("pageSize", "100");
  url.searchParams.set("page", page.toString());
  console.log("url is", url.toString());
  const res = await fetch(url.toString(), {
    headers: { "X-Api-Key": NEWSAPI_KEY },
  });

  if (!res.ok) {
    throw new Error(`NewsAPI error ${res.status}`);
  }

  return res.json();
}

async function main() {
  const to = new Date();
  const from = new Date(to.getTime() - 7 * 24 * 60 * 60 * 1000);

  const fromIso = from.toISOString();
  const toIso = to.toISOString();
  console.log("SOURCES:", JSON.stringify(SOURCES));
  console.log("WINDOW:", fromIso, "->", toIso);
  const articles: NewsApiArticle[] = [];

  let page = 1;

  while (true) {
    const data = await fetchPage(fromIso, toIso, page);
    console.log(
      `page ${page}: status=${data.status} totalResults=${data.totalResults} batch=${data.articles.length}`,
    );

    if (data.status !== "ok") {
      console.error("NewsAPI non-ok response:", data);
      throw new Error(`NewsAPI status=${data.status}`);
    }

    articles.push(...data.articles);

    if (data.articles.length < 100) break;

    page++;
    if (page > 50) break;
  }

  const client = new Client({ connectionString: DATABASE_URL });
  await client.connect();

  const sql = `
    INSERT INTO public.headlines (
      source_id,
      source_name,
      author,
      title,
      description,
      url,
      url_to_image,
      published_at,
      content
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT (url) DO UPDATE SET
      source_name = EXCLUDED.source_name,
      author = EXCLUDED.author,
      title = EXCLUDED.title,
      description = EXCLUDED.description,
      url_to_image = EXCLUDED.url_to_image,
      published_at = EXCLUDED.published_at,
      content = EXCLUDED.content,
      updated_at = now()
  `;

  for (const a of articles) {
    await client.query(sql, [
      a.source.id,
      a.source.name,
      a.author,
      a.title,
      a.description,
      a.url,
      a.urlToImage,
      new Date(a.publishedAt),
      a.content,
    ]);
  }

  await client.end();

  console.log(`Inserted/updated ${articles.length} articles`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
