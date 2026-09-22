import { useEffect, useMemo, useState } from "react";
import uPlot from "uplot";
import { api, Cluster, Metrics, StoreMetrics } from "../api";
import { Chart } from "../Chart";
import { Card } from "./OverviewTab";
import type { T } from "../App";

const REFRESH_MS = 5000;
const WINDOW = 360; // ≈30 分钟 @5s
const PALETTE = ["#2f81f7", "#2ea043", "#d29922", "#f85149", "#a371f7", "#39c5cf", "#f778ba", "#e3b341"];

type Series = (number | null)[];

/** 计数器差分速率;回退(进程重启)按 0 处理,缺样本为 null(断线)。 */
function rates(hist: Metrics[], get: (m: Metrics) => number | null): Series {
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
    out.push(dv >= 0 ? dv / dt : 0);
  }
  return out;
}

const fmtRate = (v: number) => (v >= 100 ? v.toFixed(0) : v.toFixed(1));
const fmtCount = (v: number) => String(Math.round(v));

export function MetricsTab({ t, cluster }: { t: T; cluster: Cluster | null }) {
  const [hist, setHist] = useState<Metrics[]>([]);

  useEffect(() => {
    let alive = true;
    const poll = async () => {
      try {
        const m = await api.metrics();
        if (alive) setHist((h) => [...h.slice(-(WINDOW - 1)), m]);
      } catch {
        // 保留旧数据,下一轮重试。
      }
    };
    poll();
    const id = setInterval(poll, REFRESH_MS);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  const nodeIds = useMemo(
    () => [...new Set(hist.flatMap((m) => m.stores.map((s) => s.node_id)))].sort((a, b) => a - b),
    [hist],
  );

  const charts = useMemo(() => {
    if (hist.length === 0) return null;
    const xs = hist.map((m) => m.ts_ms / 1000); // uPlot 时间轴用秒
    const store = (m: Metrics, id: number): StoreMetrics | null =>
      m.stores.find((s) => s.node_id === id) ?? null;
    const storeCounter = (id: number, pick: (s: StoreMetrics) => number) => (m: Metrics) => {
      const s = store(m, id);
      return s && s.scrape_ok ? pick(s) : null; // scrape 失败 → 断线
    };

    const under: Series = hist.map((m) => m.master.under_replicated_partitions);
    const hb = rates(hist, (m) => m.master.heartbeats_total);
    const trans = rates(hist, (m) => m.master.transitions_issued_total);
    const rerep = rates(hist, (m) => m.master.rereplications_total);
    const putRates = nodeIds.map((id) => rates(hist, storeCounter(id, (s) => s.put_total)));
    const getRates = nodeIds.map((id) => rates(hist, storeCounter(id, (s) => s.get_total)));
    const replicas = nodeIds.map((id) =>
      hist.map((m) => {
        const s = store(m, id);
        return s && s.scrape_ok ? s.replicas : null;
      }),
    );

    return {
      under: [xs, under] as uPlot.AlignedData,
      hb: [xs, hb] as uPlot.AlignedData,
      trans: [xs, trans, rerep] as uPlot.AlignedData,
      put: [xs, ...putRates] as uPlot.AlignedData,
      get: [xs, ...getRates] as uPlot.AlignedData,
      replicas: [xs, ...replicas] as uPlot.AlignedData,
    };
  }, [hist, nodeIds]);

  const latest = hist.length > 0 ? hist[hist.length - 1] : null;
  const capPct =
    cluster && cluster.total_capacity_bytes > 0
      ? ((cluster.used_bytes / cluster.total_capacity_bytes) * 100).toFixed(1) + "%"
      : "—";
  const storeLabels = nodeIds.map((id) => `n${id}`);

  return (
    <section>
      <div className="kpis">
        <Card label={t("leader")} value={latest ? (latest.master.is_leader ? t("yes") : t("no")) : "—"} />
        <Card label={t("raftTerm")} value={latest ? String(latest.master.raft_term) : "—"} />
        <Card
          label={t("nodesOnline")}
          value={
            cluster
              ? `${cluster.nodes_up} / ${cluster.nodes_total}`
              : latest
                ? String(latest.stores.filter((s) => s.up).length)
                : "—"
          }
        />
        <Card
          label={t("underReplicated")}
          value={latest ? String(latest.master.under_replicated_partitions) : "—"}
          danger={(latest?.master.under_replicated_partitions ?? 0) > 0}
        />
        <Card label={t("capacityUsed")} value={capPct} />
        <Card label={t("apiRequests")} value={latest ? String(latest.admin.api_requests_total) : "—"} />
      </div>

      {!charts && <p className="empty">{t("loadingMetrics")}</p>}
      {charts && (
        <div className="charts">
          <Chart title={t("chartUnder")} labels={[t("underReplicated")]} colors={PALETTE} data={charts.under} fmt={fmtCount} />
          <Chart title={t("chartHeartbeat")} labels={[t("chartHeartbeat")]} colors={PALETTE} data={charts.hb} fmt={fmtRate} />
          <Chart
            title={t("chartTransitions")}
            labels={[t("transitions"), t("rereplications")]}
            colors={PALETTE}
            data={charts.trans}
            fmt={fmtRate}
          />
          {nodeIds.length > 0 && (
            <>
              <Chart title={t("chartPut")} labels={storeLabels} colors={PALETTE} data={charts.put} fmt={fmtRate} />
              <Chart title={t("chartGet")} labels={storeLabels} colors={PALETTE} data={charts.get} fmt={fmtRate} />
              <Chart title={t("chartReplicas")} labels={storeLabels} colors={PALETTE} data={charts.replicas} fmt={fmtCount} />
            </>
          )}
        </div>
      )}
    </section>
  );
}
