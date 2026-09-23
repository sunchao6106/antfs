// 指标时间窗的差分与速率计算(窗口轮询已收敛到 `metricsStore` 的共享单例)。

import { Metrics } from "./api";
import { useMetricsWindow as storeUseMetricsWindow } from "./metricsStore";

export type Series = (number | null)[];

/** 差分速率(`clampNegative` = 计数器语义,回退按 0 处理;false 时保留负值,给容量类 gauge)。 */
export function diffRates(
  hist: Metrics[],
  get: (m: Metrics) => number | null,
  clampNegative: boolean,
): Series {
  const out: Series = [null];
  for (let i = 1; i < hist.length; i++) {
    const prev = get(hist[i - 1]);
    const cur = get(hist[i]);
    const dt = (hist[i].ts_ms - hist[i - 1].ts_ms) / 1000;
    if (prev == null || cur == null || dt <= 0) {
      out.push(null);
      continue;
    }
    const dv = cur - prev;
    out.push(dv >= 0 ? dv / dt : clampNegative ? 0 : dv / dt);
  }
  return out;
}

/** 计数器差分速率;回退(进程重启)按 0 处理,缺样本为 null(断线)。 */
export function rates(hist: Metrics[], get: (m: Metrics) => number | null): Series {
  return diffRates(hist, get, true);
}

/** 有符号差分速率(可负);用于容量/已用字节这类可增可减的 gauge。 */
export function deltas(hist: Metrics[], get: (m: Metrics) => number | null): Series {
  return diffRates(hist, get, false);
}

/** 轮询 `/api/metrics`,保留最近 `window` 个样本(默认 5s × 360 ≈ 30 分钟)。 */
export function useMetricsWindow(refreshMs = 5000, window = 360): Metrics[] {
  return storeUseMetricsWindow(refreshMs, window);
}

/**
 * 最近 `secs` 秒的平均增速(单位/秒,可负)。
 *
 * 取窗口内"最新的样本"与"最早的 ≥ `secs` 秒前的样本"做差分:采样间隔远小于 `secs` 时,
 * 相当于对 `secs` 窗口做一次平均(平滑掉单次采样的抖动)。历史不足 `secs/2` 或样本缺值 → `null`
 * (首次进入页面时显示 "—",不报一个基于 5s 的假读数)。
 */
export function recentRate(
  hist: Metrics[],
  get: (m: Metrics) => number | null,
  secs: number,
): number | null {
  if (hist.length < 2) return null;
  const cur = hist[hist.length - 1];
  const curVal = get(cur);
  if (curVal == null) return null;
  const base = hist.find((m) => m.ts_ms >= cur.ts_ms - secs * 1000);
  if (!base || base === cur) return null;
  const baseVal = get(base);
  const dt = (cur.ts_ms - base.ts_ms) / 1000;
  if (baseVal == null || dt < secs / 2) return null;
  return (curVal - baseVal) / dt;
}
