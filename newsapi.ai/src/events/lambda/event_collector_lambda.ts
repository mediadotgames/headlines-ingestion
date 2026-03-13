import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { handler as eventCollector } from "../event_collector";

const s3 = new S3Client({});

async function streamToString(stream: any): Promise<string> {
  return await new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on("data", (chunk: Buffer) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });
}

export const handler = async (event: any) => {
  console.log("event_collector_lambda triggered");
  console.log(JSON.stringify(event, null, 2));

  const record = event?.Records?.[0];
  if (!record) {
    throw new Error("No S3 record in Lambda event");
  }

  const bucket = record.s3.bucket.name;
  const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));

  if (!key.endsWith("load_report.json")) {
    console.log("Ignoring non article load report:", key);
    return;
  }

  console.log(`Reading article load report from s3://${bucket}/${key}`);

  const res = await s3.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    }),
  );

  const body = await streamToString(res.Body);
  const loadReport = JSON.parse(body);

  if (loadReport.pipeline_status !== "loaded") {
    console.log("Article run not loaded, skipping event collection.");
    return;
  }

  const runId = loadReport.run_id;
  const runType = loadReport.run_type;
  const nthRun = loadReport.nth_run;

  console.log("Parent article run:", {
    runId,
    runType,
    nthRun,
  });

  process.env.PARENT_RUN_ID = runId;
  process.env.PARENT_RUN_TYPE = runType;
  process.env.PARENT_NTH_RUN = String(nthRun);
  process.env.PARENT_INGESTION_SOURCE = loadReport.ingestion_source;

  process.env.EVENT_DISCOVERY_SCOPE = process.env.EVENT_DISCOVERY_SCOPE ?? "parent_run";

  await eventCollector();

  console.log("Event collection complete.");
};