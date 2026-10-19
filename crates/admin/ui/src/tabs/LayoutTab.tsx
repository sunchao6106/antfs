import { useCallback, useEffect, useState } from "react";
import { api, LayoutReport } from "../api";
import type { T } from "../App";

const REFRESH_MS = 5000;

/** 违规徽标:一律红(kinds 全是"该看一眼"的事)。 */
const VIOLATION_PILL = "down";

/** target_state 徽标:ACTIVE 绿 / BOOTSTRAP 蓝 / 其余(OFFLINE·DROPPED)灰。 */
function targetPillClass(state: string): string {
  switch (state) {
    case "ACTIVE":
      return "ok";
    case "BOOTSTRAP":
      return "info";
    default:
      return "";
  }
}

/**
 * 布局反查 + 放置合规(排水/搬机的"清点"页):汇总 + 违规表 + 逐节点副本清单(行展开)。
 * 只读页:发现违例后的处置走分区页的 move/repair 运维弧(这里不做任何写操作)。
 */
export function LayoutTab({ t }: { t: T }) {
  const [report, setReport] = useState<LayoutReport | null>(null);
  const [loadErr, setLoadErr] = useState("");
  const [expanded, setExpanded] = useState<Set<number>>(new Set());

  const refresh = useCallback(async () => {
    try {
      setReport(await api.layout());
      setLoadErr("");
    } catch (e) {
      setLoadErr(String(e));
    }
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, REFRESH_MS);
    return () => clearInterval(id);
  }, [refresh]);

  const toggle = (nodeId: number) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(nodeId)) next.delete(nodeId);
      else next.add(nodeId);
      return next;
    });

  return (
    <section>
      {loadErr && <div className="msg err">{t("balanceLoadError", { err: loadErr })}</div>}

      <div className="toolbar">
        <span className="spacer" />
        <button className="btn ghost" onClick={refresh}>
          {t("refresh")}
        </button>
      </div>

      {report && (
        <>
          <div className="panel">
            <div className="panel-head">
              <h3>{t("layoutSummary")}</h3>
              <span className={`pill small ${report.compliant ? "ok" : VIOLATION_PILL}`}>
                {report.compliant ? t("layoutCompliant") : t("layoutNonCompliant")}
              </span>
            </div>
            <div className="disk-grid" style={{ marginTop: 10 }}>
              <div className="disk-cell">
                {t("layoutTotals", {
                  n: report.nodes.length,
                  p: report.total_partitions,
                  r: report.total_replicas,
                })}
              </div>
              <div className="disk-cell">
                {t("layoutUpdatedAt")}: {new Date(report.generated_at_ms).toLocaleString()}
              </div>
            </div>
          </div>

          <h2>{t("layoutViolations")}</h2>
          {report.violations.length === 0 ? (
            <div className="panel">
              <p className="empty">{t("layoutNoViolations")}</p>
            </div>
          ) : (
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>{t("layoutColKind")}</th>
                    <th>{t("balanceColPartition")}</th>
                    <th>{t("layoutColNode")}</th>
                    <th>{t("layoutColDisk")}</th>
                    <th>{t("layoutColDetail")}</th>
                  </tr>
                </thead>
                <tbody>
                  {report.violations.map((v, i) => (
                    <tr key={`${v.kind}-${i}`}>
                      <td>
                        <span className={`pill small ${VIOLATION_PILL}`}>
                          {t(`layoutKind_${v.kind}`)}
                        </span>
                      </td>
                      <td>{v.partition_id ?? "—"}</td>
                      <td>{v.node_id != null ? `n${v.node_id}` : "—"}</td>
                      <td className="mono">{v.disk_id ?? "—"}</td>
                      <td className="wrap">{v.detail}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          <h2>{t("nodes")}</h2>
          {report.nodes.length === 0 ? (
            <div className="panel">
              <p className="empty">{t("noNodes")}</p>
            </div>
          ) : (
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>{t("layoutColNode")}</th>
                    <th>{t("addr")}</th>
                    <th>{t("status")}</th>
                    <th>{t("layoutColReplicaCount")}</th>
                  </tr>
                </thead>
                <tbody>
                  {report.nodes.map((n) => (
                    <ReplicaRows
                      key={n.node_id}
                      t={t}
                      node={n}
                      expanded={expanded.has(n.node_id)}
                      onToggle={() => toggle(n.node_id)}
                    />
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </section>
  );
}

/** 单个节点行(可点击展开它承载的副本清单)。 */
function ReplicaRows({
  t,
  node,
  expanded,
  onToggle,
}: {
  t: T;
  node: import("../api").LayoutNode;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <>
      <tr className="clickable" onClick={onToggle}>
        <td className="mono">n{node.node_id}</td>
        <td className="mono">{node.addr}</td>
        <td>
          <span className={`pill small ${node.up ? "ok" : "down"}`}>
            {node.up ? t("up") : t("down")}
          </span>
        </td>
        <td>{node.replica_count}</td>
      </tr>
      {expanded &&
        node.replicas.map((e) => (
          <tr key={`${e.partition_id}-${e.disk_id}`}>
            <td colSpan={2} className="mono" style={{ paddingLeft: 28 }}>
              {t("balanceColPartition")} {e.partition_id} · {e.disk_id}
            </td>
            <td colSpan={2}>
              <span className={`pill small ${targetPillClass(e.target_state)}`}>
                {e.target_state}
              </span>
            </td>
          </tr>
        ))}
    </>
  );
}
