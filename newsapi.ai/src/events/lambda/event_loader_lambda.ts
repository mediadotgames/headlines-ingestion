import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { handler as eventLoader } from "../event_loader";

const s3 = new S3Client({});

async function streamToString(stream: any): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];

    stream.on("data", (chunk: Buffer) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });
}

export const handler = async (event: any) => {
  console.log("event_loader_lambda triggered");
  console.log(JSON.stringify(event, null, 2));

  const record = event?.Records?.[0];
  if (!record) {
    throw new Error("No S3 record provided to event loader lambda");
  }

  const bucket = record.s3.bucket.name;
  const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));

  if (!key.endsWith("event-manifest.json")) {
    console.log("Ignoring non event-manifest object:", key);
    return;
  }

  console.log(`Reading event manifest from s3://${bucket}/${key}`);

  const res = await s3.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    }),
  );

  const body = await streamToString(res.Body);
  const manifest = JSON.parse(body);

  if (!manifest || !manifest.run_id) {
    throw new Error("Invalid event manifest structure");
  }

  console.log("Event run info:", {
    runId: manifest.run_id,
    runType: manifest.run_type,
    nthRun: manifest.nth_run,
  });

  process.env.RUN_ID = manifest.run_id;
  process.env.RUN_TYPE = manifest.run_type;
  process.env.NTH_RUN = String(manifest.nth_run);

  process.env.PARENT_RUN_ID = manifest.parent_run_id ?? "";
  process.env.PARENT_RUN_TYPE = manifest.parent_run_type ?? "";
  process.env.PARENT_NTH_RUN = String(manifest.parent_nth_run ?? "");

  await eventLoader();

  console.log("Event loading complete.");
};