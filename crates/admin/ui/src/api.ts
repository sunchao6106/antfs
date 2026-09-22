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
}
export interface Partition {
  partition_id: number;
  state: string;
  replication_factor: number;
  replicas: Replica[];
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
}

export interface AdminMetrics {
  api_requests_total: number;
  ops_total: number;
  master_rpc_errors_total: number;
}

export interface StoreMetrics {
  node_id: number;
  addr: string;
  up: boolean;
  scrape_ok: boolean;
  put_total: number;
  get_total: number;
  put_bytes: number;
  get_bytes: number;
  errors_total: number;
  replicas: number;
  replication_bytes_total: number;
  compactions_total: number;
}

export interface Metrics {
  ts_ms: number;
  master: MasterMetrics;
  admin: AdminMetrics;
  stores: StoreMetrics[];
}

export interface PartitionPage {
  items: Partition[];
  total: number;
  offset: number;
  limit: number;
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
  partitions: (offset: number, limit: number) =>
    req<PartitionPage>(`/api/partitions?offset=${offset}&limit=${limit}`),
  partition: (id: number) => req<Partition>(`/api/partitions/${id}`),
  repairPartition: (id: number) =>
    req<RepairResult>(`/api/partitions/${id}/repair`, { method: "POST" }),
  movePartition: (id: number, from_node_id: number, to_node_id: number) =>
    req<MoveResult>(`/api/partitions/${id}/move`, {
      method: "POST",
      body: JSON.stringify({ from_node_id, to_node_id }),
    }),
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
  decommission: (nodeId: number, force = false) =>
    req<string>(`/api/nodes/${nodeId}/decommission?force=${force}`, { method: "POST" }),
};

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
