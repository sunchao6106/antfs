import { useMemo } from "react";
import { fmtBytes, Metrics, Node, StoreDiskMetrics, StoreMetrics } from "../api";
import { watermarkClass } from "../health";
import { refreshMetrics, useMetricsState } from "../metricsStore";
import { rates, Series } from "../trend";
import { Card } from "./OverviewTab";
import type { T } from "../App";

/** 样本新鲜度上限(3× 采样间隔):更旧的样本只当历史,不当作"当前值"喂表格。 */
const STALE_MS = 15000;

/** 速率格式化(与指标页同口径:大数不带小数,小数保留两位)。 */
const fmtRate = (v: number) => (v >= 100 ? v.toFixed(0) : v >= 10 ? v.toFixed(1) : v.toFixed(2));

/**
 * 延迟列的「平均 / p99」格式化(毫秒)。单侧缺数据变「—」,**不写成 0**:
 * **没数据 ≠ 零延迟**(后端给 `null` 而不是 0);两侧都缺 → 整格「—」。
 * p99 落在最高桶(`+Inf`)时后端只给**下界**,故该数值的含义是"≥ 这个数"(见表头提示文案)。
 */
const fmtRt = (avgMs: number | null, p99Seconds: number | null): string => {
  const p99Ms = p99Seconds == null ? null : p99Seconds * 1000;
  if (avgMs == null && p99Ms == null) return "—";
  const one = (v: number | null) => (v == null ? "—" : v.toFixed(2));
  return `${one(avgMs)} / ${one(p99Ms)}`;
};

/** 卡片里带单位;没数据同样是「—」而不是 `0.00 ms`。 */
const rtCardValue = (avgMs: number | null, p99Seconds: number | null): string => {
  const v = fmtRt(avgMs, p99Seconds);
  return v === "—" ? v : `${v} ms`;
};

/** 逐盘 p50 的 hover 提示。p50 与 p99 同源同口径,但列宽有限,故不占列、只在提示里给。 */
const p50Hint = (t: T, p50Seconds: number | null): string =>
  t("hostDiskP50", { v: p50Seconds == null ? "—" : `${(p50Seconds * 1000).toFixed(2)} ms` });

const last = (s: Series): number | null => (s.length > 0 ? s[s.length - 1] : null);

/** 单个 (节点,盘) 的最新速率(窗口差分;缺样本 / 断线 → null)。 */
interface DiskRates {
  iops: number | null;
  bytes: number | null;
  slow: number | null;
}

/** 主机磁盘指标:按 store 节点(= 主机)分组,盘级 IOPS / RT / 带宽 / 超时 / 在飞 + 主机级汇总。 */
export function HostDisksTab({ t, nodes }: { t: T; nodes: Node[] }) {
  const { hist, lastOkMs, lastError, inflight } = useMetricsState();
  const newest = hist.length > 0 ? hist[hist.length - 1] : null;
  const isFresh = newest !== null && Date.now() - newest.ts_ms <= STALE_MS;
  /** 新鲜的当前快照;null = 首屏还没样本或窗口已过期(此时指标列一律 "—")。 */
  const latest = isFresh ? newest : null;

  /** 窗口里出现过的 (节点,盘) 全枚举一遍做差分:盘可能中途出现/消失,取并集才不会漏。 */
  const ratesByDisk = useMemo(() => {
    const seen = new Map<number, Set<string>>();
    for (const m of hist) {
      for (const s of m.stores) {
        if (!s.scrape_ok) continue;
        for (const d of s.disks) {
          const ids = seen.get(s.node_id) ?? new Set<string>();
          ids.add(d.disk_id);
          seen.set(s.node_id, ids);
        }
      }
    }
    const out = new Map<number, Map<string, DiskRates>>();
    for (const [nodeId, ids] of seen) {
      const perDisk = new Map<string, DiskRates>();
      for (const diskId of ids) {
        // 抓取失败的那几轮按断线(null)处理,不要把"抓不到"差分成一个假速率。
        const counter = (pick: (d: StoreDiskMetrics) => number) => (m: Metrics): number | null => {
          const s = m.stores.find((x) => x.node_id === nodeId);
          if (!s || !s.scrape_ok) return null;
          const d = s.disks.find((x) => x.disk_id === diskId);
          return d ? pick(d) : null;
        };
        perDisk.set(diskId, {
          iops: last(rates(hist, counter((d) => d.requests_total))),
          bytes: last(rates(hist, counter((d) => d.bytes_total))),
          slow: last(rates(hist, counter((d) => d.slow_requests_total))),
        });
      }
      out.set(nodeId, perDisk);
    }
    return out;
  }, [hist]);

  /** 主机清单 = `/api/nodes` 的节点 ∪ 本次抓取到的 store 节点(任一侧缺失也照出该主机)。 */
  const hosts = useMemo(() => {
    const ids = new Set<number>(nodes.map((n) => n.node_id));
    if (newest) for (const s of newest.stores) ids.add(s.node_id);
    return Array.from(ids).sort((a, b) => a - b);
  }, [nodes, newest]);

  return (
    <section>
      {/* 采集状态条:与指标页同一口径(共享单例窗口)。 */}
      <div className="metrics-bar">
        <span>
          {t("metricsLastUpdate")}: {lastOkMs == null ? "—" : new Date(lastOkMs).toLocaleTimeString()}
        </span>
        <span>{t("metricsSamples")}: {hist.length}</span>
        <span className="muted">{t("metricsAuto")}</span>
        {newest !== null && !isFresh && <span className="pill small warn">{t("metricsStale")}</span>}
        <span className="spacer" />
        <button className="btn ghost tiny" disabled={inflight} onClick={() => void refreshMetrics()}>
          {inflight ? t("metricsRefreshing") : t("refresh")}
        </button>
      </div>

      {lastError && <div className="banner err">{t("metricsError", { err: lastError })}</div>}

      <p className="card-sub">{t("hostDisksNote")}</p>

      {hosts.length === 0 && <p className="empty">{t("noNodes")}</p>}

      {hosts.map((nodeId) => {
        const node = nodes.find((n) => n.node_id === nodeId) ?? null;
        const raw: StoreMetrics | null = newest?.stores.find((x) => x.node_id === nodeId) ?? null;
        /** 抓取成功的新鲜快照;null = 该主机本轮没有可用盘指标(列一律 "—")。 */
        const store = latest && raw && raw.scrape_ok ? raw : null;

        const capacity = new Map((node?.disks ?? []).map((d) => [d.disk_id, d]));
        const scraped = new Map((store?.disks ?? []).map((d) => [d.disk_id, d]));
        // 盘清单也是并集:有容量没指标的盘留行(指标列 "—"),有指标没容量的盘同样留行。
        const diskIds = Array.from(new Set([...capacity.keys(), ...scraped.keys()])).sort((a, b) =>
          a.localeCompare(b),
        );

        const rOf = (id: string): DiskRates | null => (store ? ratesByDisk.get(nodeId)?.get(id) ?? null : null);
        const sumRate = (f: (r: DiskRates) => number | null): number | null => {
          const vals = diskIds.map((id) => {
            const r = rOf(id);
            return r ? f(r) : null;
          }).filter((v): v is number => v != null);
          return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) : null;
        };
        const iops = sumRate((r) => r.iops);
        const bw = sumRate((r) => r.bytes);
        const slow = sumRate((r) => r.slow);

        // RT / 池水位是瞬时量(直方图累计与 gauge):用当前样本求和,不做差分。
        const disks = Array.from(scraped.values());
        const rtSum = store ? disks.reduce((a, d) => a + d.rt_sum_seconds, 0) : null;
        const rtCount = store ? disks.reduce((a, d) => a + d.rt_count, 0) : null;
        const avgRtMs = rtSum != null && rtCount ? (rtSum / rtCount) * 1000 : null;
        // syscall 口径(闭包内,不含池排队):与上一行**刻意分开**,池忙而盘健康时两者分叉。
        const ioSum = store ? disks.reduce((a, d) => a + d.io_sum_seconds, 0) : null;
        const ioCount = store ? disks.reduce((a, d) => a + d.io_count, 0) : null;
        const avgIoMs = ioSum != null && ioCount ? (ioSum / ioCount) * 1000 : null;
        // 主机级 p99 = **该主机最差那块的盘**的 p99(取最大)。**分位点不能相加**,所以不做求和:
        // 对每块盘都是一个上界(全部盘超阈的观测数 ≤ 各盘 1% 之和 = 总体的 1%),
        // 即它是"主机 p99 的上界",比伪造一个"平均 p99"诚实。
        const p99Max = (pick: (d: StoreDiskMetrics) => number | null): number | null => {
          const vals = disks.map(pick).filter((v): v is number => v != null);
          return vals.length > 0 ? Math.max(...vals) : null;
        };
        const rtP99 = store ? p99Max((d) => d.rt_p99_seconds) : null;
        const ioP99 = store ? p99Max((d) => d.io_p99_seconds) : null;
        const inFlight = store ? disks.reduce((a, d) => a + d.pool_in_flight, 0) : null;
        const queued = store ? disks.reduce((a, d) => a + d.pool_queued, 0) : null;
        const poolRejected = store ? disks.reduce((a, d) => a + d.pool_rejected_total, 0) : null;

        const capBytes = (node?.disks ?? []).reduce((a, d) => a + d.capacity_bytes, 0);
        const freeBytes = (node?.disks ?? []).reduce((a, d) => a + d.free_bytes, 0);
        const used = capBytes > 0 ? 1 - freeBytes / capBytes : 0;
        // 主机级"最差盘健康":优先用 store **自报**的 `health_state`(与逐盘列同一口径),
        // 抓不到再退回 master 心跳带来的那份。两者都没有 → 不显示徽标(不谎报健康)。
        const worst =
          disks.map((d) => d.health_state).find((h) => h && h !== "HEALTHY") ??
          (node?.disks ?? []).map((d) => d.health).find((h) => h && h !== "HEALTHY") ??
          null;

        return (
          <section key={nodeId}>
            <h2>{t("hostTitle", { node: nodeId })}</h2>
            <div className="panel">
              <div className="panel-head">
                {node && <span className="mono">{node.addr}</span>}
                {node && (
                  <span className={`pill ${node.up ? "ok" : "down"}`}>
                    {node.up ? t("up") : t("down")}
                  </span>
                )}
                {newest && raw && !raw.scrape_ok && (
                  <span className="pill down small">{t("scrapeFail")}</span>
                )}
                {worst && <span className="pill warn small">{worst}</span>}
                <div className="spacer" />
                <span className="card-sub">{t("diskCount", { n: diskIds.length })}</span>
              </div>
              <div className="kpis" style={{ marginTop: 12 }}>
                <Card label={t("hostSumIops")} value={iops == null ? "—" : fmtRate(iops)} />
                <Card
                  label={t("hostSumBandwidth")}
                  value={bw == null ? "—" : `${fmtBytes(bw)}/s`}
                />
                <Card label={t("hostAvgRt")} value={rtCardValue(avgRtMs, rtP99)} />
                <Card label={t("hostDiskRtSyscall")} value={rtCardValue(avgIoMs, ioP99)} />
                <Card label={t("hostSumSlow")} value={slow == null ? "—" : fmtRate(slow)} />
                <Card
                  label={t("hostSumInFlight")}
                  value={inFlight == null ? "—" : String(inFlight)}
                  danger={poolRejected != null && poolRejected > 0}
                />
                <Card
                  label={t("hostSumQueue")}
                  value={queued == null ? "—" : String(queued)}
                />
                <Card
                  label={t("hostCapacity")}
                  value={capBytes > 0 ? `${fmtBytes(freeBytes)} / ${fmtBytes(capBytes)}` : "—"}
                  danger={capBytes > 0 && used >= 0.95}
                />
              </div>
              {capBytes > 0 && (
                <div className={`prog ${watermarkClass(used)}`}>
                  <div style={{ width: `${Math.round(used * 100)}%` }} />
                </div>
              )}
            </div>

            <table style={{ marginTop: 12 }}>
              <thead>
                <tr>
                  <th>{t("hostDiskCol")}</th>
                  <th>{t("health")}</th>
                  <th>{t("hostDiskCapacity")}</th>
                  <th>{t("hostDiskIops")}</th>
                  <th title={t("hostDiskRtHint")}>{t("hostDiskRt")}</th>
                  <th title={t("hostDiskRtSyscallHint")}>{t("hostDiskRtSyscall")}</th>
                  <th>{t("hostDiskBandwidth")}</th>
                  <th>{t("hostDiskSlow")}</th>
                  <th title={t("hostDiskQueueHint")}>{t("hostDiskQueue")}</th>
                  <th title={t("hostDiskInFlightHint")}>{t("hostDiskInFlight")}</th>
                </tr>
              </thead>
              <tbody>
                {diskIds.map((id) => {
                  const cap = capacity.get(id);
                  const m = scraped.get(id);
                  const r = rOf(id);
                  const frac = cap && cap.capacity_bytes > 0 ? 1 - cap.free_bytes / cap.capacity_bytes : 0;
                  const pct = Math.round(frac * 100);
                  return (
                    <tr key={id}>
                      <td className="mono">{id}</td>
                      <td>
                        {/* 优先用 store **自己**上报的状态(D2 的派生态,最新);抓不到退回 master 侧
                            心跳带来的那份;两者都没有则显示 UNKNOWN —— 绝不默认成"健康"。 */}
                        {(() => {
                          const st = m?.health_state ?? cap?.health ?? "";
                          if (!st) return "—";
                          const cls =
                            st === "HEALTHY" ? "ok" : st === "UNKNOWN" ? "" : "warn";
                          return (
                            <>
                              <span
                                className={`pill small ${cls}`}
                                title={st === "UNAVAILABLE" ? t("hostDiskEvicted") : st}
                              >
                                {st}
                              </span>
                              {m != null && m.io_watchdog_trips_total > 0 && (
                                <span className="card-sub">
                                  {t("hostDiskWatchdog", { n: m.io_watchdog_trips_total })}
                                </span>
                              )}
                              {m != null && m.worker_skipped_total > 0 && (
                                <span className="card-sub">
                                  {t("hostDiskWorkSkipped", { n: m.worker_skipped_total })}
                                </span>
                              )}
                              {m?.probe_stuck === 1 && (
                                <span className="card-sub">{t("hostProbeStuck")}</span>
                              )}
                              {m != null && m.probe_skipped_total > 0 && (
                                <span className="card-sub">
                                  {t("hostProbeSkipped", { n: m.probe_skipped_total })}
                                </span>
                              )}
                            </>
                          );
                        })()}
                      </td>
                      <td>
                        {cap ? (
                          <>
                            {fmtBytes(cap.free_bytes)} / {fmtBytes(cap.capacity_bytes)}
                            <div className={`prog ${watermarkClass(frac)}`}>
                              <div style={{ width: `${pct}%` }} />
                            </div>
                            <span className="card-sub">{t("usedPct", { pct })}</span>
                          </>
                        ) : (
                          "—"
                        )}
                      </td>
                      <td className="mono">{r?.iops == null ? "—" : fmtRate(r.iops)}</td>
                      {/* 端到端口径(含池排队):平均 / p99。分位点取不到桶样本时该侧是「—」,不写成 0;
                          p50 不占列(列宽有限),放在 hover 提示里 —— 数据在后端 JSON 里同源同口径。 */}
                      <td className="mono" title={m ? p50Hint(t, m.rt_p50_seconds) : undefined}>
                        {m
                          ? fmtRt(
                              m.rt_count > 0 ? (m.rt_sum_seconds / m.rt_count) * 1000 : null,
                              m.rt_p99_seconds,
                            )
                          : "—"}
                      </td>
                      {/* syscall 口径(闭包内,不含池排队):与上一列**刻意分开**,池忙而盘健康时两者分叉。 */}
                      <td className="mono" title={m ? p50Hint(t, m.io_p50_seconds) : undefined}>
                        {m
                          ? fmtRt(
                              m.io_count > 0 ? (m.io_sum_seconds / m.io_count) * 1000 : null,
                              m.io_p99_seconds,
                            )
                          : "—"}
                      </td>
                      <td className="mono">{r?.bytes == null ? "—" : `${fmtBytes(r.bytes)}/s`}</td>
                      <td className="mono">
                        {r?.slow == null ? "—" : fmtRate(r.slow)}
                        {m != null && m.pool_rejected_total > 0 && (
                          <span className="card-sub">
                            {t("hostDiskPoolRejected", { n: m.pool_rejected_total })}
                          </span>
                        )}
                      </td>
                      {/* 真队列深度:已准入未被取走 ÷ 该盘 worker 数。 */}
                      <td className="mono">
                        {m ? `${m.pool_queued} / ${m.pool_workers}` : "—"}
                      </td>
                      <td className="mono">{m ? String(m.pool_in_flight) : "—"}</td>
                    </tr>
                  );
                })}
                {diskIds.length === 0 && (
                  <tr>
                    <td colSpan={10} className="empty">
                      {t("noDisks")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </section>
        );
      })}
    </section>
  );
}
