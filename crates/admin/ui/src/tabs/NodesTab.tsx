import { useMemo, useState } from "react";
import {
  api,
  DATA_OPS,
  fmtBytes,
  fmtSecs,
  Node,
  RequestControlBody,
  RequestControlEntry,
} from "../api";
import type { T } from "../App";

type Msg = { kind: "ok" | "err"; text: string } | null;

/** 限流默认有效期(分钟);留空 = 不传 `ttl_secs`,由 store 的默认时长决定。 */
const DEFAULT_TTL_MIN = "30";

function worstHealth(n: Node): string | null {
  const bad = n.disks.map((d) => d.health).filter((h) => h && h !== "HEALTHY");
  return bad.length > 0 ? bad[0] : null;
}

function usedFrac(n: Node): number {
  const cap = n.disks.reduce((s, d) => s + d.capacity_bytes, 0);
  const free = n.disks.reduce((s, d) => s + d.free_bytes, 0);
  return cap > 0 ? 1 - free / cap : 0;
}

export function NodesTab({
  t,
  nodes,
  isOperator,
  initialQuery,
}: {
  t: T;
  nodes: Node[];
  isOperator: boolean;
  /** 初值来自 App 的 hash query(概览点磁盘行跳过来时带上 node id)。 */
  initialQuery: string;
}) {
  const [query, setQuery] = useState(initialQuery);
  const [offlineOnly, setOfflineOnly] = useState(false);
  const [expanded, setExpanded] = useState<Set<number>>(new Set());
  const [rcFor, setRcFor] = useState<number | null>(null);
  const [rcList, setRcList] = useState<RequestControlEntry[]>([]);
  const [rcOp, setRcOp] = useState<string>(DATA_OPS[0]);
  const [rcTtl, setRcTtl] = useState(DEFAULT_TTL_MIN);
  const [rcMsg, setRcMsg] = useState<Msg>(null);
  const [rcBusy, setRcBusy] = useState(false);
  /** 该节点的复制开关(null = 未取到/取失败);`rcReplBusy` 防连点。 */
  const [rcPaused, setRcPaused] = useState<boolean | null>(null);
  const [rcReplBusy, setRcReplBusy] = useState(false);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return nodes.filter((n) => {
      if (offlineOnly && n.up) return false;
      if (!q) return true;
      return (
        String(n.node_id).includes(q) ||
        n.addr.toLowerCase().includes(q) ||
        n.dc.toLowerCase().includes(q) ||
        n.rack.toLowerCase().includes(q)
      );
    });
  }, [nodes, query, offlineOnly]);

  const toggle = (id: number) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  /** 展开/收起某节点的限流面板;展开时拉当前禁用集与复制开关(都存在 store 进程内存里)。 */
  const openRc = async (nodeId: number) => {
    if (rcFor === nodeId) {
      setRcFor(null);
      return;
    }
    setRcFor(nodeId);
    setRcList([]);
    setRcPaused(null);
    setRcMsg(null);
    try {
      setRcList(await api.storeRequestControl(nodeId));
      setRcPaused((await api.storeReplicationControl(nodeId)).paused);
    } catch (e) {
      setRcMsg({ kind: "err", text: String(e) });
    }
  };

  /** 开/关静默:`ttl_secs` 只在填了分钟数时才提交(留空 = 用 store 默认时长)。 */
  const applyRc = async (nodeId: number, enable: boolean) => {
    setRcBusy(true);
    const body: RequestControlBody = { op: rcOp, enable };
    const mins = rcTtl.trim();
    if (enable && mins !== "") body.ttl_secs = Math.round(Number(mins) * 60);
    try {
      setRcList(await api.setStoreRequestControl(nodeId, body));
      setRcMsg({
        kind: "ok",
        text: enable ? t("reqDisabled", { op: rcOp }) : t("reqEnabled", { op: rcOp }),
      });
    } catch (e) {
      setRcMsg({ kind: "err", text: String(e) });
    } finally {
      setRcBusy(false);
    }
  };

  /** 暂停/恢复该节点的主动拉复制(结果同时更新徽标与面板消息条)。 */
  const applyReplication = async (nodeId: number, paused: boolean) => {
    setRcReplBusy(true);
    try {
      setRcPaused((await api.setStoreReplicationControl(nodeId, paused)).paused);
      setRcMsg({
        kind: "ok",
        text: paused ? t("replPausedOk", { node: nodeId }) : t("replResumedOk", { node: nodeId }),
      });
    } catch (e) {
      setRcMsg({ kind: "err", text: String(e) });
    } finally {
      setRcReplBusy(false);
    }
  };

  const colSpan = isOperator ? 6 : 5;

  return (
    <section>
      <div className="toolbar">
        <input
          type="search"
          placeholder={t("searchNodes")}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <label className="check">
          <input type="checkbox" checked={offlineOnly} onChange={(e) => setOfflineOnly(e.target.checked)} />
          {t("offlineOnly")}
        </label>
      </div>
      <table>
        <thead>
          <tr>
            <th>{t("node")}</th>
            <th>{t("dcRack")}</th>
            <th>{t("addr")}</th>
            <th>{t("status")}</th>
            <th>{t("disks")}</th>
            {isOperator && <th>{t("actions")}</th>}
          </tr>
        </thead>
        <tbody>
          {filtered.map((n) => {
            const frac = usedFrac(n);
            const pct = Math.round(frac * 100);
            const worst = worstHealth(n);
            const open = expanded.has(n.node_id);
            const rcOpen = rcFor === n.node_id;
            const rows = [
              <tr key={n.node_id} className="clickable" onClick={() => toggle(n.node_id)}>
                <td>{n.node_id}</td>
                <td>{n.dc} / {n.rack}</td>
                <td className="mono">{n.addr}</td>
                <td>
                  <span className={`pill ${n.up ? "ok" : "down"}`}>{n.up ? t("up") : t("down")}</span>
                </td>
                <td>
                  {t("diskCount", { n: n.disks.length })}
                  <div className={`prog ${frac >= 0.9 ? "full" : frac >= 0.75 ? "hot" : ""}`}>
                    <div style={{ width: `${pct}%` }} />
                  </div>
                  <span className="card-sub">{t("usedPct", { pct })}</span>
                  {worst && <span className="pill warn small">{worst}</span>}
                </td>
                {isOperator && (
                  <td className="row-actions">
                    <button
                      className="btn tiny"
                      onClick={(e) => {
                        e.stopPropagation();
                        openRc(n.node_id);
                      }}
                    >
                      {t("requestControl")}
                    </button>
                  </td>
                )}
              </tr>,
            ];
            if (open) {
              rows.push(
                <tr key={`${n.node_id}-detail`} className="detail">
                  <td colSpan={colSpan}>
                    <div className="disk-grid">
                      {n.disks.map((d) => (
                        <div key={d.disk_id} className="disk-cell">
                          <span className="mono">{d.disk_id}</span>
                          <span>
                            {fmtBytes(d.free_bytes)} / {fmtBytes(d.capacity_bytes)}
                          </span>
                          {d.health && (
                            <span className={`pill small ${d.health === "HEALTHY" ? "ok" : "warn"}`}>{d.health}</span>
                          )}
                        </div>
                      ))}
                    </div>
                  </td>
                </tr>,
              );
            }
            if (rcOpen) {
              rows.push(
                <tr key={`${n.node_id}-rc`} className="detail">
                  <td colSpan={colSpan}>
                    <div className="inline-panel">
                      <div className="panel-head">
                        <h3>{t("requestControlTitle", { node: n.node_id })}</h3>
                      </div>
                      <label>
                        {t("opLabel")}
                        <select value={rcOp} onChange={(e) => setRcOp(e.target.value)}>
                          {DATA_OPS.map((op) => (
                            <option key={op} value={op}>
                              {op}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label>
                        {t("ttlLabel")}
                        <input
                          type="text"
                          inputMode="numeric"
                          value={rcTtl}
                          onChange={(e) => setRcTtl(e.target.value)}
                        />
                      </label>
                      <button className="btn danger tiny" disabled={rcBusy} onClick={() => applyRc(n.node_id, true)}>
                        {t("disableReq")}
                      </button>
                      <button className="btn tiny" disabled={rcBusy} onClick={() => applyRc(n.node_id, false)}>
                        {t("enableReq")}
                      </button>
                      <button className="btn ghost tiny" onClick={() => setRcFor(null)}>
                        {t("cancelBtn")}
                      </button>
                      <p className="card-sub">{t("ttlHint")}</p>
                    </div>

                    <div className="panel" style={{ marginTop: 8 }}>
                      <div className="card-label">{t("currentDisabled")}</div>
                      {rcList.length === 0 && <p className="empty">{t("nothingDisabled")}</p>}
                      {rcList.map((e, i) => (
                        <div key={`${e.op}-${e.partition_id ?? "node"}-${i}`} className="kv-row">
                          <span className="k">
                            <span className="pill small">{e.op}</span>{" "}
                            {e.partition_id == null
                              ? t("nodeScope")
                              : t("partitionScope", { id: e.partition_id })}
                          </span>
                          <span className="v">
                            {t("remaining", { v: fmtSecs(e.remaining_secs) })}
                          </span>
                        </div>
                      ))}
                    </div>

                    <div className="panel" style={{ marginTop: 8 }}>
                      <div className="panel-head">
                        <span className="card-label">{t("replication")}</span>
                        <span
                          className={`pill small ${rcPaused == null ? "" : rcPaused ? "warn" : "ok"}`}
                        >
                          {rcPaused == null ? t("replUnknown") : rcPaused ? t("replPaused") : t("replRunning")}
                        </span>
                        <div className="spacer" />
                        <button
                          className={`btn tiny ${rcPaused ? "" : "danger"}`}
                          disabled={rcReplBusy || rcPaused == null}
                          onClick={() => applyReplication(n.node_id, !rcPaused)}
                        >
                          {rcPaused ? t("resumeReplication") : t("pauseReplication")}
                        </button>
                      </div>
                      <p className="card-sub">{t("replHint")}</p>
                    </div>

                    {rcMsg && <div className={`msg ${rcMsg.kind}`}>{rcMsg.text}</div>}
                  </td>
                </tr>,
              );
            }
            return rows;
          })}
          {filtered.length === 0 && (
            <tr>
              <td colSpan={colSpan} className="empty">
                {nodes.length === 0 ? t("noNodes") : t("noMatch")}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </section>
  );
}
