import { useMemo } from "react";
import uPlot from "uplot";
import { Cluster, fmtBytes, Metrics, StoreMetrics } from "../api";
import { Chart, PALETTE } from "../Chart";
import { Card } from "./OverviewTab";
import { deltas, rates, Series } from "../trend";
import { refreshMetrics, useMetricsState } from "../metricsStore";
import type { T } from "../App";

/** 按 blob 大小分桶的序列(与 `size` label 一一对应)。 */
const SIZES = ["small", "medium", "large"] as const;

const fmtRate = (v: number) => (v >= 100 ? v.toFixed(0) : v >= 10 ? v.toFixed(1) : v.toFixed(2));
const fmtCount = (v: number) => String(Math.round(v));

export function MetricsTab({ t, cluster }: { t: T; cluster: Cluster | null }) {
  const { hist, lastOkMs, lastError, inflight } = useMetricsState();

  const nodeIds = useMemo(
    () => [...new Set(hist.flatMap((m) => m.stores.map((s) => s.node_id)))].sort((a, b) => a - b),
    [hist],
  );

  const charts = useMemo(() => {
    // 一个样本起就能画:单点的 x 区间由 Chart 自己撑开(不必等第二个采样点)。
    if (hist.length === 0) return null;
    const xs = hist.map((m) => m.ts_ms / 1000); // uPlot 时间轴用秒
    const store = (m: Metrics, id: number): StoreMetrics | null =>
      m.stores.find((s) => s.node_id === id) ?? null;
    const storeCounter = (id: number, pick: (s: StoreMetrics) => number) => (m: Metrics) => {
      const s = store(m, id);
      return s && s.scrape_ok ? pick(s) : null; // scrape 失败 → 断线
    };

    // master 不可达的那几轮里,master 段是占位 0:按"断线"处理(null),不要画成跌到 0 的线
    // (与各 store scrape 失败的处理一致)。
    const g = <V,>(m: Metrics, f: (x: Metrics) => V): V | null => (m.master_ok === false ? null : f(m));

    const under: Series = hist.map((m) => g(m, (x) => x.master.under_replicated_partitions));
    const hb = rates(hist, (m) => g(m, (x) => x.master.heartbeats_total));
    const trans = rates(hist, (m) => g(m, (x) => x.master.transitions_issued_total));
    const rerep = rates(hist, (m) => g(m, (x) => x.master.rereplications_total));
    const putRates = nodeIds.map((id) => rates(hist, storeCounter(id, (s) => s.put_total)));
    const getRates = nodeIds.map((id) => rates(hist, storeCounter(id, (s) => s.get_total)));
    const replicas = nodeIds.map((id) =>
      hist.map((m) => {
        const s = store(m, id);
        return s && s.scrape_ok ? s.replicas : null;
      }),
    );

    // master 分配/查找类计数器(任何节点都有值,不受 leader 限制)。
    const assign = rates(hist, (m) => g(m, (x) => x.master.assign_requests_total));
    const assignFail = rates(hist, (m) => g(m, (x) => x.master.assign_failures_total));

    // 按 blob 大小分桶的 put/get 速率:各 store 的计数先跨节点求和再差分;
    // 全部 store 都抓取失败时给 null(断线),不要把"抓不到"画成 0。
    const blobSum = (m: Metrics, f: (s: StoreMetrics) => number): number | null => {
      const ok = m.stores.filter((s) => s.scrape_ok);
      return ok.length === 0 ? null : ok.reduce((a, s) => a + f(s), 0);
    };
    const blobRates = (op: "put" | "get", size: (typeof SIZES)[number]) =>
      rates(hist, (m) => blobSum(m, (s) => s[`blob_${op}_${size}_count`]));
    const blobPut = SIZES.map((sz) => blobRates("put", sz));
    const blobGet = SIZES.map((sz) => blobRates("get", sz));
    // master gauge 只在 leader 的 reconciler 里刷新(follower 恒 0),故仅 leader 视角画;
    // master 不可达的那几轮同样按断线(null)处理。
    const writable: Series = hist.map((m) =>
      m.master_ok !== false && m.master.is_leader ? m.master.writable_capacity_bytes : null,
    );
    const usedGrowth = deltas(hist, (m) =>
      g(m, (x) => (x.master.is_leader ? x.master.cluster_used_bytes : null)),
    );
    const lag: Series = hist.map((m) =>
      m.master_ok !== false && m.master.is_leader ? m.master.max_replica_lag_bytes : null,
    );
    const nodesUp: Series = hist.map((m) =>
      m.master_ok !== false && m.master.is_leader ? m.master.nodes_up : null,
    );
    const majorityDown: Series = hist.map((m) =>
      m.master_ok !== false && m.master.is_leader ? m.master.majority_replicas_down_partitions : null,
    );

    return {
      under: [xs, under] as uPlot.AlignedData,
      hb: [xs, hb] as uPlot.AlignedData,
      trans: [xs, trans, rerep] as uPlot.AlignedData,
      put: [xs, ...putRates] as uPlot.AlignedData,
      get: [xs, ...getRates] as uPlot.AlignedData,
      replicas: [xs, ...replicas] as uPlot.AlignedData,
      assign: [xs, assign, assignFail] as uPlot.AlignedData,
      writable: [xs, writable] as uPlot.AlignedData,
      usedGrowth: [xs, usedGrowth] as uPlot.AlignedData,
      lag: [xs, lag] as uPlot.AlignedData,
      nodesUp: [xs, nodesUp] as uPlot.AlignedData,
      majorityDown: [xs, majorityDown] as uPlot.AlignedData,
      blobPut: [xs, ...blobPut] as uPlot.AlignedData,
      blobGet: [xs, ...blobGet] as uPlot.AlignedData,
    };
  }, [hist, nodeIds]);

  /** 样本新鲜度上限(3× 采样间隔):更旧的样本只用于画历史,不当作"当前值"喂 KPI —— 
      否则带着 sessionStorage 种子刷新页面时,KPI 会先把几分钟前的旧值当现状显示。 */
  const STALE_MS = 15000;
  const newest = hist.length > 0 ? hist[hist.length - 1] : null;
  const isFresh = newest !== null && Date.now() - newest.ts_ms <= STALE_MS;
  const latest = isFresh ? newest : null;
  const capPct =
    cluster && cluster.total_capacity_bytes > 0
      ? ((cluster.used_bytes / cluster.total_capacity_bytes) * 100).toFixed(1) + "%"
      : "—";
  const storeLabels = nodeIds.map((id) => `n${id}`);
  // master 的 gauge 只在 leader 的 reconciler 里刷新:follower 上恒 0,故不画这类图、KPI 显示 "—"。
  const isLeader = latest?.master.is_leader === 1;
  // master 不可达时 master 段是占位 0:不要把它当现状展示(KPI 显示 "—",另有横幅说明原因)。
  const mv = latest != null && latest.master_ok === false ? null : latest;

  return (
    <section>
      <div className="kpis">
        <Card label={t("leader")} value={mv ? (mv.master.is_leader ? t("yes") : t("no")) : "—"} />
        <Card label={t("raftTerm")} value={mv ? String(mv.master.raft_term) : "—"} />
        <Card
          label={t("nodesOnline")}
          value={
            cluster
              ? `${cluster.nodes_up} / ${cluster.nodes_total}`
              : mv
                ? String(mv.stores.filter((s) => s.up).length)
                : "—"
          }
        />
        <Card
          label={t("underReplicated")}
          value={mv ? String(mv.master.under_replicated_partitions) : "—"}
          danger={(mv?.master.under_replicated_partitions ?? 0) > 0}
        />
        <Card label={t("capacityUsed")} value={capPct} />
        <Card
          label={t("writableCapacity")}
          value={isLeader && mv ? fmtBytes(mv.master.writable_capacity_bytes) : "—"}
        />
        <Card
          label={t("partitionStates")}
          value={
            isLeader && mv
              ? `${mv.master.partitions_rw} / ${mv.master.partitions_ro} / ${mv.master.partitions_sealed}`
              : "—"
          }
        />
        <Card
          label={t("majorityDown")}
          value={isLeader && mv ? String(mv.master.majority_replicas_down_partitions) : "—"}
          danger={isLeader && (mv?.master.majority_replicas_down_partitions ?? 0) > 0}
        />
        <Card label={t("apiRequests")} value={latest ? String(latest.admin.api_requests_total) : "—"} />
        <Card
          label={t("maintenanceMode")}
          value={mv ? (mv.master.maintenance ? t("maintenanceOn") : t("maintenanceOff")) : "—"}
          danger={mv?.master.maintenance === 1}
        />
      </div>

      {latest && !isLeader && latest.master_ok !== false && (
        <p className="empty">{t("gaugeLeaderOnly")}</p>
      )}

      {/* 采集状态:最后更新时间 + 样本数 + 各 store 抓取状态 + 手动刷新。
          抓取失败的 store 单独标出来 —— 数据"慢/缺"时运维能一眼看出是哪个节点在拖。 */}
      <div className="metrics-bar">
        <span>{t("metricsLastUpdate")}: {lastOkMs == null ? "—" : new Date(lastOkMs).toLocaleTimeString()}</span>
        <span>{t("metricsSamples")}: {hist.length}</span>
        <span className="muted">{t("metricsAuto")}</span>
        {newest !== null && !isFresh && <span className="pill small warn">{t("metricsStale")}</span>}
        <span className="spacer" />
        {latest?.stores.map((s) => (
          <span
            key={s.node_id}
            className={`pill small ${s.scrape_ok ? "ok" : "down"}`}
            title={s.addr}
          >
            n{s.node_id} {s.scrape_ok ? t("scrapeOk") : t("scrapeFail")}
          </span>
        ))}
        <button className="btn ghost tiny" disabled={inflight} onClick={() => void refreshMetrics()}>
          {inflight ? t("metricsRefreshing") : t("refresh")}
        </button>
      </div>

      {lastError && <div className="banner err">{t("metricsError", { err: lastError })}</div>}
      {latest && latest.master_ok === false && (
        <div className="banner err">
          {t("metricsMasterDown", { err: latest.master_error || "unavailable" })}
        </div>
      )}

      {!charts && <p className="empty">{lastError ? t("metricsError", { err: lastError }) : t("metricsFirstSample")}</p>}
      {charts && (
        <>
          <h2>{t("sectionRequests")}</h2>
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
            <Chart
              title={t("chartAssign")}
              labels={[t("assign"), t("assignFailures")]}
              colors={PALETTE}
              data={charts.assign}
              fmt={fmtRate}
            />
          </div>
          {nodeIds.length > 0 && (
            <>
              <h2>{t("sectionStores")}</h2>
              <div className="charts">
                <Chart title={t("chartPut")} labels={storeLabels} colors={PALETTE} data={charts.put} fmt={fmtRate} />
                <Chart title={t("chartGet")} labels={storeLabels} colors={PALETTE} data={charts.get} fmt={fmtRate} />
                <Chart title={t("chartReplicas")} labels={storeLabels} colors={PALETTE} data={charts.replicas} fmt={fmtCount} />
                <Chart
                  title={t("chartBlobPutSize")}
                  labels={[t("sizeSmall"), t("sizeMedium"), t("sizeLarge")]}
                  colors={PALETTE}
                  data={charts.blobPut}
                  fmt={fmtRate}
                />
                <Chart
                  title={t("chartBlobGetSize")}
                  labels={[t("sizeSmall"), t("sizeMedium"), t("sizeLarge")]}
                  colors={PALETTE}
                  data={charts.blobGet}
                  fmt={fmtRate}
                />
              </div>
            </>
          )}
          {isLeader && (
            <>
              <h2>{t("sectionCapacity")}</h2>
              <div className="charts">
                <Chart title={t("chartWritable")} labels={[t("writableCapacity")]} colors={PALETTE} data={charts.writable} fmt={fmtBytes} />
                <Chart title={t("chartUsedGrowth")} labels={[t("chartUsedGrowth")]} colors={PALETTE} data={charts.usedGrowth} fmt={fmtBytes} />
                <Chart title={t("chartLag")} labels={[t("chartLag")]} colors={PALETTE} data={charts.lag} fmt={fmtBytes} />
                <Chart title={t("chartNodesUp")} labels={[t("chartNodesUp")]} colors={PALETTE} data={charts.nodesUp} fmt={fmtCount} />
                <Chart
                  title={t("chartMajorityDown")}
                  labels={[t("majorityDown")]}
                  colors={PALETTE}
                  data={charts.majorityDown}
                  fmt={fmtCount}
                />
              </div>
            </>
          )}
        </>
      )}
    </section>
  );
}
