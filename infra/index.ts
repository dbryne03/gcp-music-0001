import * as pulumi from "@pulumi/pulumi";
import * as gcp from "@pulumi/gcp";

const config = new pulumi.Config();
const project = gcp.config.project!;
const region = gcp.config.region ?? "europe-west2";

// ── Storage ──────────────────────────────────────────────────────────────────

const rawBucket = new gcp.storage.Bucket("raw-data", {
    name: `${project}-music-raw`,
    location: region,
    uniformBucketLevelAccess: true,
    // TODO: lifecycle rules for raw data retention
});

// ── BigQuery ──────────────────────────────────────────────────────────────────

const rawDataset = new gcp.bigquery.Dataset("raw", {
    datasetId: "raw",
    location: region,
    description: "Raw landing dataset — one table per source",
});

const martDataset = new gcp.bigquery.Dataset("mart", {
    datasetId: "music",
    location: region,
    description: "dbt mart layer — dimensional models",
});

// ── Secret Manager ────────────────────────────────────────────────────────────

const lastfmApiKey = new gcp.secretmanager.Secret("lastfm-api-key", {
    secretId: "lastfm-api-key",
    replication: { auto: {} },
});

const kafkaBootstrapServers = new gcp.secretmanager.Secret("kafka-bootstrap-servers", {
    secretId: "kafka-bootstrap-servers",
    replication: { auto: {} },
});

const kafkaApiKey = new gcp.secretmanager.Secret("kafka-api-key", {
    secretId: "kafka-api-key",
    replication: { auto: {} },
});

// ── Cloud Run Jobs ─────────────────────────────────────────────────────────────
// TODO: define lastfm-extractor, musicbrainz-extractor, spotify-extractor, dbt-runner
// Each job references a container image built and pushed via GitHub Actions

// ── Exports ───────────────────────────────────────────────────────────────────

export const rawBucketName = rawBucket.name;
export const rawDatasetId = rawDataset.datasetId;
export const martDatasetId = martDataset.datasetId;
