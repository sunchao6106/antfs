import { useEffect, useState } from "react";
import { api, Cluster, fmtBytes } from "../api";
import type { T } from "../App";

const REFRESH_MS = 5000;

export function OverviewTab({ t, cluster }: { t: T; cluster: Cluster | null }) {
  const [total, setTotal] = useState<number | null>(null);

  useEffect(() => {
    let alive = true;
    const poll = () =>
      api
        .partitions(0, 1)
        .then((p) => alive && setTotal(p.total))
        .catch(() => {});
    poll();
    const id = setInterval(poll, REFRESH_MS);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  const under = cluster?.under_replicated_partitions ?? 0;
  return (
    <section className="cards">
      <Card label={t("leader")} value={cluster ? `${t("node")} ${cluster.leader ?? "—"}` : "—"} />
      <Card label={t("raftTerm")} value={cluster ? String(cluster.term) : "—"} />
      <Card label={t("nodesUpTotal")} value={cluster ? `${cluster.nodes_up} / ${cluster.nodes_total}` : "—"} />
      <Card label={t("partitions")} value={total == null ? "—" : String(total)} />
      <Card
        label={t("capacity")}
        value={cluster ? `${fmtBytes(cluster.used_bytes)} / ${fmtBytes(cluster.total_capacity_bytes)}` : "—"}
      />
      <Card label={t("freeCapacity")} value={cluster ? fmtBytes(cluster.free_bytes) : "—"} />
      <Card label={t("underReplicated")} value={String(under)} danger={under > 0} />
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
