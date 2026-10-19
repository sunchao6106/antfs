// admin JSON API 客户端:token 存 localStorage,注入 Authorization。

const TOKEN_KEY = "ambry_operator_token";
const LANG_KEY = "ambry_lang";

export function getToken(): string {
  return localStorage.getItem(TOKEN_KEY) ?? "";
}
export function setToken(t: string): void {
  localStorage.setItem(TOKEN_KEY, t);
}
export type Lang = "en" | "zh";
export function getLang(): Lang {
  return (localStorage.getItem(LANG_KEY) as Lang) ?? "en";
}
export function setLang(l: Lang): void {
  localStorage.setItem(LANG_KEY, l);
}

export interface Cluster {
  this_node: number;
  leader: number | null;
  term: number;
  registered_nodes: number;
  next_partition_id: number;
  total_capacity_bytes: number;
  free_bytes: number;
  used_bytes: number;
  nodes_up: number;
  nodes_total: number;
  under_replicated_partitions: number;
  /** 维护模式:master 的自动收敛(再复制/迁移/下线迁移)已暂停。 */
  maintenance: boolean;
  /** 维护模式下是否连带暂停自动扩容。 */
  pause_autogrow: boolean;
  grafana_url: string;
}
export interface Disk {
  disk_id: string;
  capacity_bytes: number;
  free_bytes: number;
  health: string;
}
export interface Node {
  node_id: number;
  dc: string;
  rack: string;
  addr: string;
  up: boolean;
  disks: Disk[];
}
export interface Replica {
  node_id: number;
  addr: string;
  disk_id: string;
  target_state: string;
  up: boolean;
  current_state: string | null;
  lag_bytes: number | null;
  /** 该副本上报的分区实际占用字节。 */
  used_bytes: number;
}
export interface Partition {
  partition_id: number;
  state: string;
  replication_factor: number;
  replicas: Replica[];
  /** 分区实际占用 = 各副本 used_bytes 的最大值。 */
  used_bytes: number;
  healthy_replicas: number;
  healable: boolean;
  under_replicated_reason: string;
  moving_out_nodes: number[];
}

export interface RepairResult {
  ok: boolean;
  error: string;
  added_node_id: number;
  added_disk_id: string;
}

export interface MoveResult {
  ok: boolean;
  error: string;
  to_node_id: number;
  to_disk_id: string;
}

/** 单个副本节点的 compact 结果:成功带回收统计,失败只有 error(404/超时/网络)。 */
export interface CompactNodeResult {
  node_id: number;
  ok: boolean;
  live_bytes?: number;
  dead_bytes?: number;
  segments_before?: number;
  segments_after?: number;
  error?: string;
}

export interface CompactResult {
  results: CompactNodeResult[];
  error: string;
}

/** 单个副本节点的**异步** compact 提交结果(`job_id` 用于查进度)。 */
export interface CompactSubmitNodeResult {
  node_id: number;
  ok: boolean;
  job_id?: number;
  error?: string;
}

export interface CompactSubmitResult {
  results: CompactSubmitNodeResult[];
  error: string;
}

/** store 侧一条 compact 后台任务;未完成时没有 `finished_at_ms`/`result`/`error` 三个字段。 */
export interface CompactionJob {
  job_id: number;
  partition_id: number;
  state: "running" | "done" | "failed";
  started_at_ms: number;
  finished_at_ms?: number;
  result?: {
    live_bytes: number;
    dead_bytes: number;
    segments_before: number;
    segments_after: number;
  };
  error?: string;
}

/** 单个副本节点上该分区的后台任务(节点不可达 → `ok:false` + `error`)。 */
export interface CompactJobsNodeResult {
  node_id: number;
  ok: boolean;
  jobs?: CompactionJob[];
  error?: string;
}

export interface CompactJobsResult {
  results: CompactJobsNodeResult[];
}

/** 一条审计记录(`GET /api/audit` 的元素;`remote_addr` 在未启用 connect info 的进程里为空串)。 */
export interface AuditEntry {
  ts_ms: number;
  actor: string;
  role: string;
  method: string;
  path: string;
  status: number;
  summary: string;
  remote_addr: string;
}

export interface MasterMetrics {
  is_leader: number;
  raft_term: number;
  heartbeats_total: number;
  applied_commands_total: number;
  transitions_issued_total: number;
  rereplications_total: number;
  replicas_removed_total: number;
  under_replicated_partitions: number;
  registered_nodes: number;
  assign_requests_total: number;
  assign_failures_total: number;
  lookup_requests_total: number;
  partitions_rw: number;
  partitions_ro: number;
  partitions_sealed: number;
  writable_capacity_bytes: number;
  nodes_up: number;
  cluster_free_bytes: number;
  cluster_used_bytes: number;
  max_replica_lag_bytes: number;
  majority_replicas_down_partitions: number;
  /** 维护模式(1/0)。 */
  maintenance: number;
  /** RW 分区的实际占用汇总(每分区取副本最大值再求和)。 */
  writable_used_bytes: number;
  /** 可写余量 = 可写容量 − 可写已用。 */
  writable_free_bytes: number;
  /** 每盘副本数 max − min(均衡度)。 */
  disk_skew: number;
}

export interface AdminMetrics {
  api_requests_total: number;
  ops_total: number;
  master_rpc_errors_total: number;
}

/**
 * 单盘明细(`/api/metrics` stores 段的 `disks` 元素;盘从样本的 `disk_id` label 枚举,按 id 升序)。
 *
 * 数值**跨 `op` 求和**;口径是**请求级**(数据面请求),引擎内部 IO(flush / compact / 入向复制)尚未归盘。
 */
export interface StoreDiskMetrics {
  disk_id: string;
  /** 落在该盘的请求数(差分即该盘 IOPS)。 */
  requests_total: number;
  /** 该盘**成功**请求的字节数(差分即该盘带宽)。 */
  bytes_total: number;
  /** 端到端耗时超过 `disk_health_timeout_secs` 的请求数(差分即超时速率)。 */
  slow_requests_total: number;
  /** 该盘在途请求数(**含正在执行**的) —— 注意它**不是**队列深度,真队列深度看 `pool_queued`。 */
  in_flight: number;
  /** 延迟直方图的 `_sum`(秒)/ `_count`:平均 RT = sum / count(跨进程生命周期累计,重启清零)。 */
  rt_sum_seconds: number;
  rt_count: number;
  /**
   * **端到端**口径的分位点(秒,p50 / p99):由同一份直方图的 `le` 桶**跨 op 求和后线性插值**得到
   * (口径照抄 Prometheus `histogram_quantile`),与 `rt_sum_seconds`/`rt_count` 是**同一批观测** ——
   * 平均会把长尾平均掉,分位点才看得出长尾(`3ms / 900ms` 与 `3ms / 5ms` 的平均可以一模一样)。
   *
   * `null` = 取不到桶样本(老版本 store / 该盘零请求):UI 必须显示「—」,**不许当成 0 ms**。
   * 目标秩落在最高桶(`+Inf`)时**只能给下界**:返回值 = 最高有限 `le`,即"真实分位点 ≥ 这个数"
   * (绝不声称超出最高桶的精度)。见 cluster.md §11 / store.md §12.12。
   */
  rt_p50_seconds: number | null;
  rt_p99_seconds: number | null;
  /** 1 = 该盘有健康探测线程卡在设备里。 */
  probe_stuck: number;
  /** 因上一轮探测未返回而跳过的探测轮次。 */
  probe_skipped_total: number;
  // ── 每盘有界池(真队列深度)────────────────────────────────────────────────
  /** 该盘池的 worker 线程数(启动时定死)。 */
  pool_workers: number;
  /** **真队列深度**:已准入但**还没被 worker 取走**的任务数。 */
  pool_queued: number;
  /** 已准入未完成(运行中 + 排队中)的任务数;逼近 `pool_workers` 即该盘正在被压满。 */
  pool_in_flight: number;
  /** 被该盘池拒掉的提交数(池满超时 / 未知盘 / 池已关)。**持续增长 = 该加池或查盘**。 */
  pool_rejected_total: number;
  // ── 健康与看门狗 ──────────────────────────────────────────────────────────
  /** 该盘当前状态:`HEALTHY` / `UNAVAILABLE`(已被隔离,请求会 503)/ `UNKNOWN`(从未观测)/ 具体故障枚举。 */
  health_state: string;
  /** 看门狗判定的单次 IO 超期次数(闭包内 syscall 口径)。 */
  io_watchdog_trips_total: number;
  /** 因盘不可服务而被跳过的后台工作数(flush / compaction / replication)。 */
  worker_skipped_total: number;
  /**
   * **syscall 口径**的 IO 直方图 `_sum`/`_count`:平均 = sum/count。
   *
   * 与 `rt_sum_seconds`(端到端,**含池排队**)**刻意区分**:这个回答"这盘自己快不快",
   * 那个回答"用户看到多慢"。盘忙但健康时,两者会明显分叉。
   */
  io_sum_seconds: number;
  io_count: number;
  /** **syscall 口径**的分位点(秒,p50 / p99):同上,但取的是闭包内那套 `le` 桶,不含池排队。 */
  io_p50_seconds: number | null;
  io_p99_seconds: number | null;
}

export interface StoreMetrics {
  node_id: number;
  addr: string;
  up: boolean;
  scrape_ok: boolean;
  /** 逐盘明细(抓取失败 → 空数组)。 */
  disks: StoreDiskMetrics[];
  put_total: number;
  get_total: number;
  put_bytes: number;
  get_bytes: number;
  /** 数据面错误数合计(按 kind label 求和:io / unavailable / join_error)。 */
  errors_total: number;
  replicas: number;
  replication_bytes_total: number;
  compactions_total: number;
  manual_compactions_total: number;
  /**
   * 成功 blob 请求计数,按 `op` × `size`(small/medium/large)精确取自
   * `store_server_blob_request_duration_seconds_count` 的对应标签组合。
   */
  blob_put_small_count: number;
  blob_put_medium_count: number;
  blob_put_large_count: number;
  blob_get_small_count: number;
  blob_get_medium_count: number;
  blob_get_large_count: number;
}

export interface Metrics {
  ts_ms: number;
  /** master 指标是否取到;false = master 不可达,master 段数值是占位 0(前端要另作提示)。 */
  master_ok: boolean;
  /** master 不可达时的原因(fetch 失败文本),可读时为空串。 */
  master_error: string;
  master: MasterMetrics;
  admin: AdminMetrics;
  stores: StoreMetrics[];
}


/** 一致性巡检(scrub)结果:逐副本素材 + admin 侧比对结论。 */
export interface ScrubReportSummary {
  live_records: number;
  tombstones: number;
  live_bytes: number;
  sealed_segments: number;
  segments: { position: number; data_end: number }[];
  token_seg: number | null;
  token_off: number | null;
  /** 打开时跳过的坏记录数(>0 = 该副本带伤启动;盘上字节未被删改)。 */
  damaged_records: number;
  /** 打开时放弃扫描剩余部分的段数。 */
  abandoned_segments: number;
}
export interface ScrubVerifySummary {
  records: number;
  bad_records: number;
  index_mismatch: number;
  truncated: boolean;
}
export interface ScrubReplica {
  node_id: number;
  ok: boolean;
  error: string;
  report: ScrubReportSummary | null;
  key_count: number | null;
  /** 该副本的元数据窗口被 `max_bytes` 截断 → 只能算部分比对。 */
  partial: boolean;
  verify: ScrubVerifySummary | null;
}
export type ScrubDivergence =
  | { kind: "summary"; field: string; detail: string }
  | { kind: "verify"; node_id: number; detail: string }
  | { kind: "key"; key_hex: string; nodes: { node_id: number; state: string }[] };
export interface ScrubResult {
  partition_id: number;
  checked_at_ms: number;
  deep: boolean;
  replicas: ScrubReplica[];
  verdict: {
    consistent: boolean;
    inconclusive: boolean;
    partial: boolean;
    divergences: ScrubDivergence[];
  };
  error: string;
}
/** 最近一轮 scrub 的紧凑摘要(`GET /api/scrub`)。 */
export interface ScrubRun {
  partition_id: number;
  checked_at_ms: number;
  deep: boolean;
  consistent: boolean;
  inconclusive: boolean;
  partial: boolean;
  divergence_count: number;
  replicas: { node_id: number; ok: boolean; live_records: number; live_bytes: number }[];
}

export interface PartitionPage {
  items: Partition[];
  total: number;
  offset: number;
  limit: number;
}

export type BalancePlanStatus = "PREVIEW" | "RUNNING" | "CANCELLED" | "DONE";
export type BalanceMoveStatus = "pending" | "in_flight" | "done" | "failed" | "infeasible";

/** 布局反查:机器视角的一条副本(哪个分区、哪块盘、目标态)。 */
export interface LayoutReplica {
  partition_id: number;
  disk_id: string;
  target_state: string;
}

/** 布局反查:一个节点承载的全部副本(按 partition_id 升序)。 */
export interface LayoutNode {
  node_id: number;
  addr: string;
  up: boolean;
  replica_count: number;
  replicas: LayoutReplica[];
}

/** 一处放置违例(kind 与 `VIOLATION_KINDS` 同源:same_node / unknown_node / unknown_disk)。 */
export interface LayoutViolation {
  kind: string;
  partition_id: number | null;
  node_id: number | null;
  disk_id: string | null;
  detail: string;
}

/** `GET /api/layout` 的形状(点击式、无状态)。 */
export interface LayoutReport {
  generated_at_ms: number;
  total_partitions: number;
  total_replicas: number;
  compliant: boolean;
  nodes: LayoutNode[];
  violations: LayoutViolation[];
}

/** 均衡计划里的一条副本搬迁。 */
export interface BalanceMove {
  partition_id: number;
  from_node_id: number;
  from_disk_id: string;
  to_node_id: number;
  to_disk_id: string;
  status: BalanceMoveStatus;
  error: string;
}

/** 一份均衡计划(`status` 取值见 {@link BalancePlanStatus})。 */
export interface BalancePlan {
  plan_id: number;
  status: BalancePlanStatus;
  created_at_ms: number;
  /** 同时在搬的副本数上限。 */
  max_in_flight: number;
  total: number;
  pending: number;
  in_flight: number;
  done: number;
  failed: number;
  moves: BalanceMove[];
}

/** `GET /api/balance`:`present=false` 时 `plan` 为 null(当前没有计划)。 */
export interface BalanceState {
  present: boolean;
  plan: BalancePlan | null;
}

export interface BalanceEvalResult {
  ok: boolean;
  error: string;
  plan: BalancePlan | null;
}

export interface BalanceOpResult {
  ok: boolean;
  error: string;
}

/** `GET /admin/request-control` 的一条禁用记录(`remaining_secs: null` = 永久)。 */
export interface RequestControlEntry {
  op: string;
  /** `null` = 节点级;否则只该分区。 */
  partition_id: number | null;
  remaining_secs: number | null;
}

export interface RequestControlBody {
  op: string;
  partition_id?: number;
  enable: boolean;
  /** 分钟数换算成秒后提交;**省略该字段** = 用 store 的默认时长(不是永久)。 */
  ttl_secs?: number;
}

export type Role = "operator" | "viewer" | "none";

/** 401 时抛 Unauthorized,供上层弹登录框。 */
export class Unauthorized extends Error {}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const token = getToken();
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (token) headers["Authorization"] = `Bearer ${token}`;
  const resp = await fetch(path, { ...init, headers });
  if (resp.status === 401) throw new Unauthorized("unauthorized");
  if (!resp.ok) throw new Error(`${resp.status}: ${await resp.text()}`);
  const ct = resp.headers.get("content-type") ?? "";
  return (ct.includes("application/json") ? await resp.json() : (await resp.text())) as T;
}

export const api = {
  me: () => req<{ role: Role }>("/api/me"),
  cluster: () => req<Cluster>("/api/cluster"),
  nodes: () => req<Node[]>("/api/nodes"),
  partitions: (offset: number, limit: number, state = "") =>
    req<PartitionPage>(
      `/api/partitions?offset=${offset}&limit=${limit}${state ? `&state=${state}` : ""}`,
    ),
  partition: (id: number) => req<Partition>(`/api/partitions/${id}`),
  repairPartition: (id: number) =>
    req<RepairResult>(`/api/partitions/${id}/repair`, { method: "POST" }),
  movePartition: (id: number, from_node_id: number, to_node_id: number) =>
    req<MoveResult>(`/api/partitions/${id}/move`, {
      method: "POST",
      body: JSON.stringify({ from_node_id, to_node_id }),
    }),
  /** 对该分区所有副本节点并发触发手动 compact;单节点失败不影响其余节点。 */
  compactPartition: (id: number) =>
    req<CompactResult>(`/api/partitions/${id}/compact`, { method: "POST" }),
  /** 后台 compact:各副本立即提交任务并回 `job_id`,进度用 {@link api.compactJobs} 轮询。 */
  compactPartitionAsync: (id: number) =>
    req<CompactSubmitResult>(`/api/partitions/${id}/compact?mode=async`, { method: "POST" }),
  /** 各副本节点上该分区的后台 compact 任务(每节点最近几条,最新在前)。 */
  compactJobs: (id: number) => req<CompactJobsResult>(`/api/partitions/${id}/compact-jobs`),
  /** 某节点的复制开关状态(暂停的是该节点**主动拉**复制)。 */
  storeReplicationControl: (nodeId: number) =>
    req<{ paused: boolean }>(`/api/stores/${nodeId}/replication-control`),
  /** 暂停/恢复某节点的主动拉复制(不持久化,重启即恢复)。 */
  setStoreReplicationControl: (nodeId: number, paused: boolean) =>
    req<{ paused: boolean }>(`/api/stores/${nodeId}/replication-control`, {
      method: "POST",
      body: JSON.stringify({ paused }),
    }),
  /** 审计记录(operator 专属;最新在前,limit 默认 200、上限 2000)。 */
  audit: (limit: number) => req<{ entries: AuditEntry[] }>(`/api/audit?limit=${limit}`),
  metrics: () => req<Metrics>("/api/metrics"),
  createPartitions: (count: number, replication_factor: number) =>
    req<{ partition_ids: number[] }>("/api/partitions", {
      method: "POST",
      body: JSON.stringify({ count, replication_factor }),
    }),
  setPartitionState: (id: number, state: string) =>
    req<string>(`/api/partitions/${id}/state`, {
      method: "POST",
      body: JSON.stringify({ state }),
    }),
  scrubPartition: (id: number, deep = false) =>
    req<ScrubResult>(`/api/partitions/${id}/scrub?deep=${deep ? 1 : 0}`, { method: "POST" }),
  scrubRuns: () => req<{ runs: ScrubRun[] }>("/api/scrub"),
  decommission: (nodeId: number, force = false) =>
    req<string>(`/api/nodes/${nodeId}/decommission?force=${force}`, { method: "POST" }),
  /** 维护模式开关(pause_autogrow 仅 paused=true 时有意义)。 */
  setMaintenance: (paused: boolean, pause_autogrow: boolean) =>
    req<string>("/api/maintenance", {
      method: "POST",
      body: JSON.stringify({ paused, pause_autogrow }),
    }),
  /** 某节点的当前请求静默集(禁用中的 op × 分区)。 */
  storeRequestControl: (nodeId: number) =>
    req<RequestControlEntry[]>(`/api/stores/${nodeId}/request-control`),
  /** 开/关某节点(或某分区)某类数据面请求的静默;返回操作后的禁用集。 */
  setStoreRequestControl: (nodeId: number, body: RequestControlBody) =>
    req<RequestControlEntry[]>(`/api/stores/${nodeId}/request-control`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  /** 当前均衡计划(无计划时 `present=false`)。 */
  balanceState: () => req<BalanceState>("/api/balance"),
  /** 布局反查 + 放置合规审计(viewer 可读)。 */
  layout: () => req<LayoutReport>("/api/layout"),
  /** 评估生成一份预览计划(operator;`maxMoves` 省略时用服务端默认)。 */
  balanceEval: (maxMoves?: number) =>
    req<BalanceEvalResult>("/api/balance/eval", {
      method: "POST",
      body: JSON.stringify(maxMoves == null ? {} : { max_moves: maxMoves }),
    }),
  /** 执行指定计划(operator;按 `max_in_flight` 逐个搬迁)。 */
  balanceStart: (planId: number) =>
    req<BalanceOpResult>("/api/balance/start", {
      method: "POST",
      body: JSON.stringify({ plan_id: planId }),
    }),
  /** 取消指定计划(operator)。 */
  balanceCancel: (planId: number) =>
    req<BalanceOpResult>("/api/balance/cancel", {
      method: "POST",
      body: JSON.stringify({ plan_id: planId }),
    }),
};

/** 数据面可被静默的操作名(store 侧 `DataOp::ALL` 的对外真值,顺序一致)。 */
export const DATA_OPS = ["put", "get", "delete"] as const;

/** 秒 → 人读时长("-" = 永久)。 */
export function fmtSecs(secs: number | null): string {
  if (secs == null) return "∞";
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.round(secs / 60)}m`;
  return `${(secs / 3600).toFixed(1)}h`;
}

export function fmtBytes(n: number): string {
  const u = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < u.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${u[i]}`;
}
