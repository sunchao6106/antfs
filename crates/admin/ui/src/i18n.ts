// 中英双语字典。t(lang, key) 取文案;缺失回落 key 本身。

import type { Lang } from "./api";

type Dict = Record<string, string>;

const en: Dict = {
  title: "ambry-rs · admin",
  grafana: "Grafana",
  signIn: "Sign in",
  signOut: "Sign out",
  operatorToken: "operator token",
  tokenRequired: "Operator or viewer token required.",
  leader: "Leader",
  raftTerm: "Raft term",
  nodesUpTotal: "Nodes (up / total)",
  partitions: "Partitions",
  node: "node",
  createPartitions: "Create partitions",
  count: "Count",
  replicationFactor: "Replication factor",
  create: "Create",
  decommissionNode: "Decommission node",
  decommission: "Decommission",
  select: "select…",
  nodes: "Nodes",
  dcRack: "DC / Rack",
  addr: "Addr",
  status: "Status",
  disks: "Disks (free / capacity · health)",
  up: "UP",
  down: "DOWN",
  noNodes: "no nodes registered",
  state: "State",
  rf: "RF",
  replicas: "Replicas (node · target / current)",
  noPartitions: "no partitions",
  prev: "Prev",
  next: "Next",
  page: "Page",
  actions: "Actions",
  markRO: "RO",
  markSealed: "Seal",
  markRW: "RW",
  viewerReadOnly: "viewer (read-only)",
  role: "Role",
  confirmDecom: "Decommission node {id}? Replicas will be evacuated (add-before-remove).",
  createdPartitions: "Created partitions: {ids}",
};

const zh: Dict = {
  title: "ambry-rs · 管理台",
  grafana: "Grafana",
  signIn: "登录",
  signOut: "登出",
  operatorToken: "操作令牌",
  tokenRequired: "需要 operator 或 viewer 令牌。",
  leader: "主节点",
  raftTerm: "Raft 任期",
  nodesUpTotal: "节点(在线 / 总数)",
  partitions: "分区",
  node: "节点",
  createPartitions: "创建分区",
  count: "数量",
  replicationFactor: "副本因子",
  create: "创建",
  decommissionNode: "下线节点",
  decommission: "下线",
  select: "请选择…",
  nodes: "节点",
  dcRack: "机房 / 机架",
  addr: "地址",
  status: "状态",
  disks: "磁盘(可用 / 容量 · 健康)",
  up: "在线",
  down: "离线",
  noNodes: "无已注册节点",
  state: "状态",
  rf: "副本",
  replicas: "副本(节点 · 目标 / 实然)",
  noPartitions: "无分区",
  prev: "上一页",
  next: "下一页",
  page: "第",
  actions: "操作",
  markRO: "只读",
  markSealed: "封存",
  markRW: "可写",
  viewerReadOnly: "观察者(只读)",
  role: "角色",
  confirmDecom: "下线节点 {id}?其副本将被迁移(先补后删)。",
  createdPartitions: "已创建分区:{ids}",
};

const dicts: Record<Lang, Dict> = { en, zh };

export function makeT(lang: Lang) {
  return (key: string, vars?: Record<string, string | number>): string => {
    let s = dicts[lang][key] ?? key;
    if (vars) for (const k in vars) s = s.replace(`{${k}}`, String(vars[k]));
    return s;
  };
}
