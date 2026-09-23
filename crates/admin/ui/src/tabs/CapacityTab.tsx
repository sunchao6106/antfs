import { useCallback, useEffect, useMemo, useState } from "react";
import uPlot from "uplot";
import { api, Cluster, fmtBytes, Metrics, Partition } from "../api";
import { Chart, PALETTE } from "../Chart";
import { partitionHealth, watermarkClass } from "../health";
import { deltas, recentRate, useMetricsWindow } from "../trend";
import type { T } from "../App";

const REFRESH_MS = 5000;
const PAGE_SIZES = [10, 20, 50];
/** 增速窗口 ≈10 分钟 @5s。 */
const TREND_WINDOW = 120;
/** 分区级 1 分钟增量的本地快照:留足够长的历史(2 分钟)即可覆盖 60s 差分。 */
const SNAPSHOT_KEEP = 24;
const GROWTH_SECS = 60;

interface Snap {
  ts: number;
  used: Map<number, number>;
}

/** 容量 tab:集群水位 / 可写分区集合 / 增速 / 可写分区列表(viewer 可见)。 */
export function CapacityTab({
  t,
  cluster,
  onGotoPartitions,
}: {
  t: T;
  cluster: Cluster | null;
  onGotoPartitions: (state: string) => void;
}) {
  const hist = useMetricsWindow(REFRESH_MS, TREND_WINDOW);
  const [pageSize, setPageSize] = useState(10);
  const [offset, setOffset] = useState(0);
  const [rows, setRows] = useState<Partition[]>([]);
  const [total, setTotal] = useState(0);
  const [snaps, setSnaps] = useState<Snap[]>([]);
  const [err, setErr] = useState("");

  const refresh = useCallback(async () => {
    try {
      const p = await api.partitions(offset, pageSize, "RW");
      setRows(p.items);
      setTotal(p.total);
      setErr("");
      const ts = Date.now();
      setSnaps((h) =>
        [...h, { ts, used: new Map(p.items.map((x) => [x.partition_id, x.used_bytes])) }].slice(
          -SNAPSHOT_KEEP,
        ),
      );
    } catch (e) {
      setErr(String(e));
    }
  }, [offset, pageSize]);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, REFRESH_MS);
    return () => clearInterval(id);
  }, [refresh]);

  const latest = hist.length > 0 ? hist[hist.length - 1] : null;
  // master 的 gauge 只在 leader 的调谐循环里刷新,follower 上恒 0 → 只在 leader 视角显示。
  const isLeader = latest?.master.is_leader === 1;
  const m = latest?.master;

  /** 分区 1 分钟增量(前端窗口差分);历史不足 30s 或跨页缺样本 → null(显示 "—")。 */
  const delta1m = (pid: number, cur: number): number | null => {
    if (snaps.length === 0) return null;
    const now = snaps[snaps.length - 1].ts;
    const base = snaps.find((s) => s.ts >= now - GROWTH_SECS * 1000 && s.used.has(pid));
    if (!base || base.ts === now || now - base.ts < (GROWTH_SECS * 1000) / 2) return null;
    const old = base.used.get(pid);
    return old == null ? null : cur - old;
  };

  const clusterUsedRate = recentRate(
    hist,
    (x) => (x.master.is_leader ? x.master.cluster_used_bytes : null),
    GROWTH_SECS,
  );
  const writableUsedRate = recentRate(
    hist,
    (x) => (x.master.is_leader ? x.master.writable_used_bytes : null),
    GROWTH_SECS,
  );

  const charts = useMemo(() => {
    if (hist.length < 2) return null;
    const xs = hist.map((x) => x.ts_ms / 1000);
    const pick = (f: (x: Metrics) => number) => hist.map((x) => (x.master.is_leader ? f(x) : null));
    return {
      xs,
      writable: [
        xs,
        pick((x) => x.master.writable_used_bytes),
        pick((x) => x.master.writable_capacity_bytes),
      ] as uPlot.AlignedData,
      growth: [
        xs,
        deltas(hist, (x) => (x.master.is_leader ? x.master.cluster_used_bytes : null)),
        deltas(hist, (x) => (x.master.is_leader ? x.master.writable_used_bytes : null)),
      ] as uPlot.AlignedData,
    };
  }, [hist]);

  const capTotal = cluster?.total_capacity_bytes ?? 0;
  const capUsed = cluster?.used_bytes ?? 0;
  const capFrac = capTotal > 0 ? capUsed / capTotal : 0;
  const pageNo = Math.floor(offset / pageSize) + 1;
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  const rateText = (v: number | null) => (v == null ? "—" : fmtBytes(v));

  return (
    <section>
      <div className="panel">
        <div className="panel-head">
          <h3>{t("clusterCapacity")}</h3>
          <div className="spacer" />
          <span className="pill small">{t("capacityThresholds")}</span>
        </div>
        <div className="bigvalue">
          {capTotal > 0 ? `${(capFrac * 100).toFixed(1)}%` : "—"}
        </div>
        <div className={`prog ${watermarkClass(capFrac)}`}>
          <div style={{ width: `${Math.min(100, capFrac * 100)}%` }} />
        </div>
        <div className="bigmeta">
          {fmtBytes(capUsed)} / {fmtBytes(capTotal)}
        </div>
      </div>

      <div className="bigcards" style={{ marginTop: 12 }}>
        <WritableCard
          label={t("writablePartitions")}
          value={isLeader && m ? String(m.partitions_rw) : "—"}
        />
        <WritableCard
          label={t("writableCapacity")}
          value={isLeader && m ? fmtBytes(m.writable_capacity_bytes) : "—"}
          meta={t("writableCeiling")}
        />
        <WritableCard
          label={t("writableUsedLabel")}
          value={isLeader && m ? fmtBytes(m.writable_used_bytes) : "—"}
          meta={
            isLeader && m && m.writable_capacity_bytes > 0
              ? t("pctOfWritable", {
                  pct: ((m.writable_used_bytes / m.writable_capacity_bytes) * 100).toFixed(1),
                })
              : "—"
          }
        />
        <WritableCard
          label={t("writableFreeLabel")}
          value={isLeader && m ? fmtBytes(m.writable_free_bytes) : "—"}
          danger={isLeader && !!m && m.writable_capacity_bytes > 0 && m.writable_free_bytes === 0}
        />
      </div>

      {latest != null && !isLeader && <p className="empty">{t("gaugeLeaderOnly")}</p>}

      <div className="grid-2">
        <section>
          {charts ? (
            <Chart
              title={t("chartWritableUsed")}
              labels={[t("writableUsed"), t("writableCapacity")]}
              colors={PALETTE}
              data={charts.writable}
              fmt={fmtBytes}
            />
          ) : (
            <p className="empty">{t("loadingMetrics")}</p>
          )}
        </section>
        <section className="kv">
          <div className="kv-row">
            <span className="k">{t("perSec", { rate: t("clusterUsedGrowthRate") })}</span>
            <span className="v">{rateText(clusterUsedRate)}</span>
          </div>
          <div className="kv-row">
            <span className="k">{t("perMin", { rate: t("clusterUsedGrowthRate") })}</span>
            <span className="v">
              {clusterUsedRate == null ? "—" : fmtBytes(clusterUsedRate * 60)}
            </span>
          </div>
          <div className="kv-row">
            <span className="k">{t("perSec", { rate: t("writableUsedGrowthRate") })}</span>
            <span className="v">{rateText(writableUsedRate)}</span>
          </div>
          <div className="kv-row">
            <span className="k">{t("perMin", { rate: t("writableUsedGrowthRate") })}</span>
            <span className="v">
              {writableUsedRate == null ? "—" : fmtBytes(writableUsedRate * 60)}
            </span>
          </div>
        </section>
      </div>

      {charts && (
        <Chart
          title={t("chartGrowth")}
          subtitle={t("lastMinRate", {
            rate: clusterUsedRate == null ? "—" : fmtBytes(clusterUsedRate * 60),
          })}
          labels={[t("clusterUsedGrowthRate"), t("writableUsedGrowthRate")]}
          colors={PALETTE}
          data={charts.growth}
          fmt={fmtBytes}
        />
      )}

      <div className="panel-head" style={{ marginTop: 20 }}>
        <h3>{t("writablePartitions")}</h3>
        <div className="spacer" />
        <label className="check">
          {t("pageSize")}
          <select
            value={pageSize}
            onChange={(e) => {
              setPageSize(Number(e.target.value));
              setOffset(0);
            }}
          >
            {PAGE_SIZES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
        <button className="btn ghost tiny" onClick={() => onGotoPartitions("RW")}>
          {t("viewAll")} →
        </button>
      </div>

      {err && <div className="msg err">{err}</div>}

      <table>
        <thead>
          <tr>
            <th>{t("partitions")}</th>
            <th>{t("usedCol")}</th>
            <th>{t("rf")}</th>
            <th>{t("replicasCol")}</th>
            <th>{t("oneMinDelta")}</th>
            <th>{t("health")}</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((p) => {
            const h = partitionHealth(t, p);
            const d = delta1m(p.partition_id, p.used_bytes);
            return (
              <tr key={p.partition_id}>
                <td>{p.partition_id}</td>
                <td>{fmtBytes(p.used_bytes)}</td>
                <td>{p.replication_factor}</td>
                <td className="mono">{p.replicas.map((r) => `n${r.node_id}`).join(" / ")}</td>
                <td>{d == null ? "—" : `${d >= 0 ? "+" : ""}${fmtBytes(Math.abs(d))}`}</td>
                <td>
                  <span className={`pill small ${h.cls}`}>{h.text}</span>
                  {p.moving_out_nodes.length > 0 && (
                    <span className="pill warn small">{t("moving")}</span>
                  )}
                </td>
              </tr>
            );
          })}
          {rows.length === 0 && (
            <tr>
              <td colSpan={6} className="empty">
                {t("noWritable")}
              </td>
            </tr>
          )}
        </tbody>
      </table>

      <div className="pager">
        <button
          className="btn ghost"
          disabled={offset === 0}
          onClick={() => setOffset(Math.max(0, offset - pageSize))}
        >
          {t("prev")}
        </button>
        <span>
          {t("page")} {pageNo} / {pageCount}
        </span>
        <button
          className="btn ghost"
          disabled={offset + pageSize >= total}
          onClick={() => setOffset(offset + pageSize)}
        >
          {t("next")}
        </button>
      </div>
    </section>
  );
}

function WritableCard({
  label,
  value,
  meta,
  danger,
}: {
  label: string;
  value: string;
  meta?: string;
  danger?: boolean;
}) {
  return (
    <div className={`bigcard ${danger ? "danger" : ""}`}>
      <div className="card-label">{label}</div>
      <div className="bigvalue">{value}</div>
      {meta && <div className="bigmeta">{meta}</div>}
    </div>
  );
}
