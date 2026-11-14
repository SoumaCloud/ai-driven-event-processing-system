// Response shapes of app.py's /api/* endpoints (and of the services it proxies).

export interface ConsoleConfig {
  links: Record<string, string | undefined>;
  bucket: string;
  s3: "minio" | "aws";
}

export interface ServiceCheck {
  id: string;
  name: string;
  role: string;
  port: number | null;
  ok: boolean;
  ms: number;
  detail: string;
}

export type TopicInfo =
  | { name: string; exists: false }
  | { name: string; exists: true; partitions: number; messages: number; retained: number };

export interface GroupLag {
  group: string;
  owner: string;
  topic: string | null;
  lag: number | null;
}

export interface KafkaSnapshot {
  ok: boolean;
  error?: string;
  topics: TopicInfo[];
  groups: GroupLag[];
}

export type CounterName =
  | "ingest_valid"
  | "ingest_invalid"
  | "ingest_error"
  | "scored"
  | "anomalies"
  | "audited"
  | "threshold"
  | "archived_records"
  | "archived_objects"
  | "archive_last_success"
  | "diagnoses_ok"
  | "diagnoses_fallback"
  | "playbooks"
  | "graph_context_count"
  | "graph_context_sum"
  | "llm_seconds_count"
  | "llm_seconds_sum";

/** Prometheus values scraped from the services; null while a series has no sample yet. */
export type Counters = Record<CounterName, number | null>;

export type Severity = "low" | "medium" | "high" | "critical";

export interface AlertSummary {
  active: number;
  by_severity: Partial<Record<Severity, number>>;
}

/** Labels of the forecasting service's `forecaster_info` metric. */
export interface ForecasterInfo {
  forecaster?: string;
  model_version?: string;
}

export interface Overview {
  time: number;
  services: ServiceCheck[];
  kafka: KafkaSnapshot;
  counters: Counters;
  alerts: AlertSummary | null;
  forecaster: ForecasterInfo | null;
}

export interface HistorySample {
  t: number;
  topics: Record<string, number | undefined>;
  lag: number;
  anomalies: number | null;
  archived_objects: number | null;
  ingest_valid: number | null;
  ingest_invalid: number | null;
  playbooks: number | null;
}

export interface HistoryResponse {
  step: number;
  samples: HistorySample[];
}

export interface PlaybookStep {
  rank?: number;
  title: string;
  command: string | null;
  expected_outcome: string;
}

export interface Playbook {
  playbook_id: string;
  alert_id: string | null;
  component_id: string;
  summary: string;
  root_cause_hypothesis: string;
  steps: PlaybookStep[];
  generated_at: string;
  generated_by: string;
}

export interface IncidentsInfo {
  auto_diagnose: boolean;
  graph_backend: string;
  llm: string;
}

export interface PredictiveAlert {
  alert_id: string;
  component_id: string;
  metric_name: string;
  predicted_value: number;
  threshold: number;
  predicted_breach_time: string;
  lead_time_minutes: number;
  severity: Severity;
  status: "open" | "acknowledged" | "resolved";
  created_at: string;
  model_version: string;
  playbook_id?: string | null;
}

export interface ProduceResult {
  run_id: string;
  sent: number;
  invalid: number;
  delivered: number;
  failed: number;
  seconds: number;
  sample: unknown;
}

export interface TailMessage {
  partition: number;
  offset: number;
  timestamp: number;
  key: unknown;
  value: unknown;
  headers: Record<string, string | undefined>;
}

export interface ScoreResult {
  event_id: string;
  score: number;
  model_version: string;
  threshold: number;
  anomalous: boolean;
  audited: boolean;
  reason: string | null;
  latency_ms: number;
  baseline: { n: number; mean: number; stdev: number };
}

export interface ThresholdResult {
  threshold: number;
}

export interface GraphNode {
  node_id: string;
  node_type: string;
  health_status: string;
}

export interface GraphDependency extends GraphNode {
  depth: number;
  via: string;
  direction: "upstream" | "downstream";
}

export interface GraphContext {
  backend: string;
  component: GraphNode & { metadata: { owner_team: string; sla_tier: string } };
  dependencies: GraphDependency[];
}

export interface DiagnoseResult {
  component_id: string;
  playbook_id: string;
  summary: string;
  details: Pick<Playbook, "alert_id" | "generated_by" | "root_cause_hypothesis" | "steps"> & {
    outcome: string;
    graph_backend: string;
    dependency_count: number;
    llm_error?: string;
  };
}

export interface ForecastMetric {
  status: string;
  outcome?: string;
  threshold?: number;
  last_value?: number;
  max_upper_90?: number;
  confidence?: number;
  alert_id?: string | null;
}

export interface ForecastResult {
  component_id: string;
  alert_emitted: boolean;
  alert: PredictiveAlert | null;
  details: {
    forecaster: string;
    model_version: string;
    forecast_window_minutes: number;
    latency_seconds: number;
    metrics: Record<string, ForecastMetric>;
  };
}

export interface SimulateResult {
  metric: string;
  lowered_to: number;
  restored_to: number;
  result: ForecastResult;
}

export interface ArchiveListing {
  bucket: string;
  count: number;
  bytes: number;
  latest: { key: string; size: number; modified: string }[];
}
