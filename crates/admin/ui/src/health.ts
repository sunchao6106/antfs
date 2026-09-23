// 分区健康 / 水位阈值的共享判定(概览、容量、分区三个 tab 同一口径)。

import { Partition } from "./api";
import type { T } from "./App";

/**
 * 容量水位阈值(0.8 / 0.95)。
 *
 * 来源:master 侧无现成阈值配置(只有 `partition_capacity_bytes` 这类绝对量),故就近满经验值取
 * Ceph 的 nearfull/full 量级(0.85 / 0.95)下探一档到 0.8,给运维留出扩容决策时间。要改口径先改这里。
 */
export const WARN_WATERMARK = 0.8;
export const DANGER_WATERMARK = 0.95;

/** 水位 → 进度条色(`prog` 的修饰类)。 */
export function watermarkClass(frac: number): string {
  if (frac >= DANGER_WATERMARK) return "full";
  if (frac >= WARN_WATERMARK) return "hot";
  return "";
}

/** 分区健康徽标(欠副本 → warn/danger 并给原因;健康 → ok)。 */
export function partitionHealth(t: T, p: Partition): { cls: string; text: string } {
  const frac = `${p.healthy_replicas}/${p.replication_factor}`;
  if (!p.under_replicated_reason) return { cls: "ok", text: `${frac} ${t("healthy")}` };
  const reason = t(`reason_${p.under_replicated_reason}`);
  if (!p.healable) return { cls: "down", text: `${frac} · ${t("stuck")} · ${reason}` };
  return { cls: "warn", text: `${frac} · ${t("recovering")} · ${reason}` };
}
