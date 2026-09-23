import { useEffect, useMemo, useState } from "react";
import uPlot from "uplot";
import { api, Cluster, fmtBytes, Metrics, Node, Partition } from "../api";
import { Chart, PALETTE } from "../Chart";
import { partitionHealth, watermarkClass } from "../health";
import { recentRate, useMetricsWindow } from "../trend";
import type { T } from "../App";

const REFRESH_MS = 5000;
const TOP_N = 8;
/** 趋势窗口 ≈10 分钟 @5s。 */
const TREND_WINDOW = 120;

/** 概览:分层仪表盘(健康横幅 → 4 张大卡 → 趋势/概况两列 → 磁盘榜/可写分区两列)。 */
export function OverviewTab({
  t,
  cluster,
  nodes,
  onGotoNodes,
  onGotoPartitions,
}: {
  t: T;
  cluster: Cluster | null;
  nodes: Node[];
  /** 点磁盘行 → 跳节点页(带 node 过滤)。 */
  onGotoNodes: (nodeId: number) => void;
  /** 「查看全部」→ 跳分区页(带状态筛选)。 */
  onGotoPartitions: (state: string) => void;
}) {
  const hist = useMetricsWindow(REFRESH_MS, TREND_WINDOW);
  const [rw, setRw] = useState<Partition[]>([]);
  const [total, setTotal] = useState<number | null>(null);

  useEffect(() => {
    let alive = true;
    const poll = async () => {
      try {
        const [page, all] = await Promise.all([api.partitions(0, TOP_N, "RW"), api.partitions(0, 1)]);
        if (!alive) return;
        setRw(page.items);
        setTotal(all.total);
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

  const latest = hist.length > 0 ? hist[hist.length - 1] : null;
  // master 的 gauge 只在 leader 的调谐循环里刷新,follower 上恒 0 → 这类读数只在 leader 视角显示。
  const isLeader = latest?.master.is_leader === 1;

  // ---- 健康横幅:仅异常时着色(danger > warn),正常时低调一句话。
  const under = cluster?.under_replicated_partitions ?? 0;
  const offline = cluster ? Math.max(0, cluster.nodes_total - cluster.nodes_up) : 0;
  const majorityDown = isLeader ? (latest?.master.majority_replicas_down_partitions ?? 0) : 0;
  const writableFree = latest?.master.writable_free_bytes ?? 0;
  const noSpace = isLeader && (latest?.master.writable_capacity_bytes ?? 0) > 0 && writableFree === 0;
  const alerts: { level: "warn" | "danger"; text: string }[] = [];
  if (noSpace) alerts.push({ level: "danger", text: t("healthNoSpace") });
  if (majorityDown > 0) alerts.push({ level: "danger", text: t("healthMajorityDown", { n: majorityDown }) });
  if (under > 0) alerts.push({ level: "warn", text: t("healthUnderReplicated", { n: under }) });
  if (offline > 0) alerts.push({ level: "warn", text: t("healthStoresDown", { n: offline }) });
  if (cluster?.maintenance) alerts.push({ level: "warn", text: t("healthMaintenance") });
  const alertLevel = alerts.some((a) => a.level === "danger")
    ? "danger"
    : alerts.length > 0
      ? "warn"
      : "ok";
  const alertText = alerts.length > 0 ? alerts.map((a) => a.text).join(" · ") : t("healthOk");

  // ---- 大卡:容量水位 / 可写容量 / 在线节点 / 欠副本。
  const capTotal = cluster?.total_capacity_bytes ?? 0;
  const capUsed = cluster?.used_bytes ?? 0;
  const capFrac = capTotal > 0 ? capUsed / capTotal : 0;
  const capPct = capTotal > 0 ? (capFrac * 100).toFixed(1) : "—";

  // ---- 磁盘水位榜:全节点磁盘按 used/capacity 降序取前 N。
  const disks = useMemo(
    () =>
      nodes
        .flatMap((n) =>
          n.disks.map((d) => ({
            node: n.node_id,
            disk: d.disk_id,
            health: d.health,
            frac: d.capacity_bytes > 0 ? 1 - d.free_bytes / d.capacity_bytes : 0,
          })),
        )
        .sort((a, b) => b.frac - a.frac)
        .slice(0, TOP_N),
    [nodes],
  );

  const charts = useMemo(() => {
    // 单点数据会让 uPlot 的时间轴撑出无意义的跨度,两个点起再画。
    if (hist.length < 2) return null;
    const xs = hist.map((m) => m.ts_ms / 1000); // uPlot 时间轴用秒
    const pick = (f: (m: Metrics) => number) =>
      hist.map((m) => (m.master.is_leader ? f(m) : null));
    return {
      xs,
      used: pick((m) => m.master.cluster_used_bytes),
      writableUsed: pick((m) => m.master.writable_used_bytes),
    };
  }, [hist]);

  // 最近 1 分钟增速(前端滚动窗口差分;历史不足 1 分钟的一半时显示 "—")。
  const usedRate = recentRate(
    hist,
    (m) => (m.master.is_leader ? m.master.cluster_used_bytes : null),
    60,
  );

  return (
    <section>
      <div className={`health-banner ${alertLevel}`}>{alertText}</div>

      <div className="bigcards">
        <div className={`bigcard ${watermarkClass(capFrac) === "full" ? "danger" : watermarkClass(capFrac) === "hot" ? "warn" : ""}`}>
          <div className="card-label">{t("capacityWatermark")}</div>
          <div className="bigvalue">{capPct === "—" ? "—" : `${capPct}%`}</div>
          <div className={`prog ${watermarkClass(capFrac)}`}>
            <div style={{ width: `${Math.min(100, capFrac * 100)}%` }} />
          </div>
          <div className="bigmeta">
            {fmtBytes(capUsed)} / {fmtBytes(capTotal)}
          </div>
        </div>

        <div className="bigcard">
          <div className="card-label">{t("writableCapacity")}</div>
          <div className="bigvalue">
            {isLeader && latest ? fmtBytes(latest.master.writable_capacity_bytes) : "—"}
          </div>
          <div className="bigmeta">
            {t("writableUsed")} {isLeader && latest ? fmtBytes(latest.master.writable_used_bytes) : "—"} ·{" "}
            {t("writableFree")} {isLeader && latest ? fmtBytes(latest.master.writable_free_bytes) : "—"}
          </div>
        </div>

        <div className={`bigcard ${offline > 0 ? "warn" : ""}`}>
          <div className="card-label">{t("nodesOnline")}</div>
          <div className="bigvalue">
            {cluster ? `${cluster.nodes_up} / ${cluster.nodes_total}` : "—"}
          </div>
          <div className="bigmeta">{t("offlineN", { n: offline })}</div>
        </div>

        <div className={`bigcard ${under > 0 ? "warn" : ""}`}>
          <div className="card-label">{t("underReplicated")}</div>
          <div className="bigvalue">{String(under)}</div>
          <div className="bigmeta">
            {t("totalPartitions")} {total ?? "—"}
          </div>
        </div>
      </div>

      <div className="grid-2">
        <section>
          {charts ? (
            <Chart
              title={t("capacityTrend")}
              subtitle={t("lastMinRate", {
                rate: usedRate == null ? "—" : fmtBytes(usedRate * 60),
              })}
              labels={[t("oldUsed"), t("writableUsed")]}
              colors={PALETTE}
              data={[charts.xs, charts.used, charts.writableUsed] as uPlot.AlignedData}
              fmt={fmtBytes}
            />
          ) : (
            <p className="empty">{t("loadingMetrics")}</p>
          )}
          {latest != null && !isLeader && <p className="empty">{t("gaugeLeaderOnly")}</p>}
        </section>

        <section className="kv">
          <div className="kv-row">
            <span className="k">{t("leader")}</span>
            <span className="v">
              {cluster ? `${t("node")} ${cluster.leader ?? "—"}` : "—"}
            </span>
          </div>
          <div className="kv-row">
            <span className="k">{t("raftTerm")}</span>
            <span className="v">{cluster?.term ?? "—"}</span>
          </div>
          <div className="kv-row">
            <span className="k">{t("totalPartitions")}</span>
            <span className="v">{total ?? "—"}</span>
          </div>
          <div className="kv-row">
            <span className="k">{t("statesShort")}</span>
            <span className="v">
              {isLeader && latest
                ? `${latest.master.partitions_rw} / ${latest.master.partitions_ro} / ${latest.master.partitions_sealed}`
                : "—"}
            </span>
          </div>
          <div className="kv-row">
            <span className="k">{t("registeredNodes")}</span>
            <span className="v">{cluster?.registered_nodes ?? "—"}</span>
          </div>
          <div className="kv-row">
            <span className="k">{t("diskSkew")}</span>
            <span className="v">{isLeader && latest ? latest.master.disk_skew : "—"}</span>
          </div>
          {cluster?.grafana_url && (
            <div className="kv-row">
              <span className="k">{t("grafana")}</span>
              <span className="v">
                <a href={cluster.grafana_url} target="_blank" rel="noreferrer">
                  {t("viewAll")}
                </a>
              </span>
            </div>
          )}
        </section>
      </div>

      <div className="grid-2">
        <section>
          <div className="panel-head">
            <h3>{t("diskTop")}</h3>
          </div>
          <div className="topn">
            {disks.map((d) => (
              <div
                key={`${d.node}-${d.disk}`}
                className="topn-row clickable"
                onClick={() => onGotoNodes(d.node)}
              >
                <span className="topn-name">{t("diskRow", { node: d.node, disk: d.disk })}</span>
                <div className={`prog ${watermarkClass(d.frac)}`}>
                  <div style={{ width: `${Math.min(100, d.frac * 100)}%` }} />
                </div>
                <span className="topn-num">{Math.round(d.frac * 100)}%</span>
                {d.health && (
                  <span className={`pill small ${d.health === "HEALTHY" ? "ok" : "warn"}`}>
                    {d.health}
                  </span>
                )}
              </div>
            ))}
            {disks.length === 0 && <p className="empty">{t("noDisks")}</p>}
          </div>
        </section>

        <section>
          <div className="panel-head">
            <h3>{t("writableTop")}</h3>
            <div className="spacer" />
            <button className="btn ghost tiny" onClick={() => onGotoPartitions("RW")}>
              {t("viewAll")}
            </button>
          </div>
          <div className="topn">
            {rw.map((p) => {
              const h = partitionHealth(t, p);
              return (
                <div key={p.partition_id} className="topn-row">
                  <span className="topn-name">
                    #{p.partition_id} · {fmtBytes(p.used_bytes)}
                  </span>
                  <span className="mono">{p.replicas.map((r) => `n${r.node_id}`).join(" / ")}</span>
                  <span className="topn-num">
                    <span className={`pill small ${h.cls}`}>{h.text}</span>
                    {p.moving_out_nodes.length > 0 && (
                      <span className="pill warn small">{t("moving")}</span>
                    )}
                  </span>
                </div>
              );
            })}
            {rw.length === 0 && <p className="empty">{t("noWritable")}</p>}
          </div>
        </section>
      </div>
    </section>
  );
}

export function Card({ label, value, danger }: { label: string; value: string; danger?: boolean }) {
  return (
    <div className={`card ${danger ? "danger" : ""}`}>
      <div className="card-label">{label}</div>
      <div className="card-value">{value}</div>
    </div>
  );
}
