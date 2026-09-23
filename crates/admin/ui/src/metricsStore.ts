// 指标时间窗的**进程内单例采集器**。
//
// 概览 / 容量 / 指标三个 tab 都需要 `/api/metrics`(每次请求都会 fan-out 抓所有 store 的 /metrics,
// 是 admin 最贵的接口)。此前每个 tab 各起一个 5s 轮询:① 重复抓取;② 切到指标 tab 时要重新攒够样本,
// 期间只能显示"正在采集指标…"。这里收敛成一个轮询 + 一份共享窗口:
// 所有 tab 订阅同一份数据、切换 tab 立即可画,并在 sessionStorage 里留一份种子做刷新后的瞬时回填。

import { useEffect, useState } from "react";
import { api, Metrics } from "./api";

/** 采样间隔;与 Chart 的 x 轴粒度无关(轴用真实 ts_ms)。 */
const REFRESH_MS = 5000;
/** 滚动窗口样本数(5s × 360 ≈ 30 分钟)。 */
const WINDOW = 360;
/** 首屏补采间隔:挂载后先立刻采一次,再隔 1s 补一次,让"速率类曲线"秒级就有斜率。 */
const CATCHUP_MS = 1000;
/** 种子的最大陈旧度:更早的样本丢弃,避免把停机前后的两点连成一段假历史。 */
const SEED_MAX_AGE_MS = 5 * 60 * 1000;
const SEED_KEY = "ambry_metrics_window";

export interface MetricsState {
  /** 时间升序的样本窗口(可能来自上次会话的种子 + 本次采样)。 */
  hist: Metrics[];
  /** 最近一次成功采样的时刻;从未成功 = null。 */
  lastOkMs: number | null;
  /** 最近一次失败原因;成功后清空。 */
  lastError: string | null;
  /** 是否有请求在途。 */
  inflight: boolean;
}

type Listener = (s: MetricsState) => void;

function loadSeed(): Metrics[] {
  try {
    const raw = sessionStorage.getItem(SEED_KEY);
    if (!raw) return [];
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    const cutoff = Date.now() - SEED_MAX_AGE_MS;
    return (parsed as Metrics[]).filter((m) => typeof m?.ts_ms === "number" && m.ts_ms >= cutoff).slice(-WINDOW);
  } catch {
    return [];
  }
}

function saveSeed(hist: Metrics[]): void {
  try {
    sessionStorage.setItem(SEED_KEY, JSON.stringify(hist.slice(-WINDOW)));
  } catch {
    // 配额满/隐私模式:种子只是加速,失败静默。
  }
}

let state: MetricsState = {
  hist: loadSeed(),
  lastOkMs: null,
  lastError: null,
  inflight: false,
};
const listeners = new Set<Listener>();
let timer: ReturnType<typeof setInterval> | null = null;
let catchup: ReturnType<typeof setTimeout> | null = null;

function emit(): void {
  for (const l of listeners) l(state);
}

async function poll(): Promise<void> {
  state = { ...state, inflight: true };
  emit();
  try {
    const m = await api.metrics();
    state = {
      hist: [...state.hist.slice(-(WINDOW - 1)), m],
      lastOkMs: Date.now(),
      lastError: null,
      inflight: false,
    };
    saveSeed(state.hist);
    emit();
  } catch (e) {
    state = { ...state, inflight: false, lastError: e instanceof Error ? e.message : String(e) };
    emit();
  }
}

/** 手动刷新(页面上的"刷新"按钮)。 */
export function refreshMetrics(): Promise<void> {
  return poll();
}

function start(): void {
  if (timer !== null) return;
  void poll();
  catchup = setTimeout(() => void poll(), CATCHUP_MS);
  timer = setInterval(() => void poll(), REFRESH_MS);
}

function stop(): void {
  if (timer === null) return;
  clearInterval(timer);
  timer = null;
  if (catchup !== null) {
    clearTimeout(catchup);
    catchup = null;
  }
}

/** 订阅共享窗口:首个订阅者启动轮询,最后一个退订时停(reload/切走 tab 不空转)。 */
export function subscribeMetrics(fn: Listener): () => void {
  listeners.add(fn);
  fn(state);
  start();
  return () => {
    listeners.delete(fn);
    if (listeners.size === 0) stop();
  };
}

export function metricsState(): MetricsState {
  return state;
}

/** 订阅共享指标窗口(替代各 tab 自己轮询)。 */
export function useMetricsState(): MetricsState {
  const [s, setS] = useState<MetricsState>(metricsState);
  useEffect(() => subscribeMetrics(setS), []);
  return s;
}

/**
 * 兼容既有用法的窗口 hook:返回共享窗口的样本数组。
 *
 * `refreshMs` / `window` 参数保留是为了不改调用点;采样节奏与窗口长度由本模块统一决定。
 */
export function useMetricsWindow(_refreshMs = REFRESH_MS, _window = WINDOW): Metrics[] {
  return useMetricsState().hist;
}
