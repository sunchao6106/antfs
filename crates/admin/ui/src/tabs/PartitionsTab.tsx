import { useCallback, useEffect, useMemo, useState } from "react";
import { api, fmtBytes, Node, Partition } from "../api";
import type { T } from "../App";

const REFRESH_MS = 5000;
const PAGE_SIZES = [10, 20, 50, 100];

type Msg = { kind: "ok" | "err"; text: string } | null;

export function PartitionsTab({ t, nodes, isOperator }: { t: T; nodes: Node[]; isOperator: boolean }) {
  const [pageSize, setPageSize] = useState(10);
  const [offset, setOffset] = useState(0);
  const [stateFilter, setStateFilter] = useState("");
  const [underOnly, setUnderOnly] = useState(false);
  const [partitions, setPartitions] = useState<Partition[]>([]);
  const [total, setTotal] = useState(0);
  const [err, setErr] = useState("");
  const [msg, setMsg] = useState<Msg>(null);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [moveFor, setMoveFor] = useState<number | null>(null);
  const [moveFrom, setMoveFrom] = useState<number | "">("");
  const [moveTo, setMoveTo] = useState<number | "">("");

  const filtering = stateFilter !== "" || underOnly;

  const refresh = useCallback(async () => {
    try {
      const p = filtering ? await api.partitions(0, 0) : await api.partitions(offset, pageSize);
      let items = p.items;
      let tot = p.total;
      if (filtering) {
        items = items.filter(
          (x) =>
            (stateFilter === "" || x.state === stateFilter) &&
            (!underOnly || !!x.under_replicated_reason),
        );
        tot = items.length;
        items = items.slice(offset, offset + pageSize);
      }
      setPartitions(items);
      setTotal(tot);
      setErr("");
    } catch (e) {
      setErr(String(e));
    }
  }, [filtering, stateFilter, underOnly, offset, pageSize]);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, REFRESH_MS);
    return () => clearInterval(id);
  }, [refresh]);

  const setState = async (id: number, state: string) => {
    try {
      await api.setPartitionState(id, state);
      setMsg(null);
      refresh();
    } catch (e) {
      setMsg({ kind: "err", text: String(e) });
    }
  };

  const batchSetState = async (state: string) => {
    const ids = [...selected];
    let failed = 0;
    for (const id of ids) {
      try {
        await api.setPartitionState(id, state);
      } catch {
        failed++;
      }
    }
    setMsg(failed > 0 ? { kind: "err", text: `${failed} / ${ids.length} failed` } : null);
    setSelected(new Set());
    refresh();
  };

  const repair = async (id: number) => {
    try {
      const r = await api.repairPartition(id);
      setMsg(
        r.ok
          ? { kind: "ok", text: t("repairOk", { node: r.added_node_id, disk: r.added_disk_id }) }
          : { kind: "err", text: t("repairErr", { err: r.error }) },
      );
      refresh();
    } catch (e) {
      setMsg({ kind: "err", text: String(e) });
    }
  };

  const openMove = (p: Partition) => {
    if (moveFor === p.partition_id) {
      setMoveFor(null);
      return;
    }
    setMoveFor(p.partition_id);
    setMoveFrom(p.replicas[0]?.node_id ?? "");
    setMoveTo("");
  };

  const doMove = async (p: Partition) => {
    if (moveFrom === "") return;
    try {
      const r = await api.movePartition(p.partition_id, Number(moveFrom), moveTo === "" ? 0 : Number(moveTo));
      setMsg(
        r.ok
          ? { kind: "ok", text: t("moveOk", { node: r.to_node_id, disk: r.to_disk_id }) }
          : { kind: "err", text: t("moveErr", { err: r.error }) },
      );
      setMoveFor(null);
      refresh();
    } catch (e) {
      setMsg({ kind: "err", text: String(e) });
    }
  };

  const pageNo = Math.floor(offset / pageSize) + 1;
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  const aliveNodes = useMemo(() => nodes.filter((n) => n.up), [nodes]);

  const health = (p: Partition): { cls: string; text: string } => {
    const frac = `${p.healthy_replicas}/${p.replication_factor}`;
    if (!p.under_replicated_reason) return { cls: "ok", text: `${frac} ${t("healthy")}` };
    const reason = t(`reason_${p.under_replicated_reason}`);
    if (!p.healable) return { cls: "down", text: `${frac} · ${t("stuck")} · ${reason}` };
    return { cls: "warn", text: `${frac} · ${t("recovering")} · ${reason}` };
  };

  const allPageSelected = partitions.length > 0 && partitions.every((p) => selected.has(p.partition_id));
  const toggleAll = () => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (allPageSelected) partitions.forEach((p) => next.delete(p.partition_id));
      else partitions.forEach((p) => next.add(p.partition_id));
      return next;
    });
  };
  const toggleOne = (id: number) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const colSpan = isOperator ? 7 : 5;

  return (
    <section>
      <div className="toolbar">
        <label className="check">
          {t("filterState")}
          <select
            value={stateFilter}
            onChange={(e) => {
              setStateFilter(e.target.value);
              setOffset(0);
            }}
          >
            <option value="">{t("all")}</option>
            <option value="RW">RW</option>
            <option value="RO">RO</option>
            <option value="SEALED">SEALED</option>
          </select>
        </label>
        <label className="check">
          <input
            type="checkbox"
            checked={underOnly}
            onChange={(e) => {
              setUnderOnly(e.target.checked);
              setOffset(0);
            }}
          />
          {t("underOnly")}
        </label>
        <div className="spacer" />
        <label className="check">
          {t("perPage")}
          <select
            value={pageSize}
            onChange={(e) => {
              setPageSize(Number(e.target.value));
              setOffset(0);
            }}
          >
            {PAGE_SIZES.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </label>
      </div>

      {isOperator && selected.size > 0 && (
        <div className="toolbar">
          <span className="pill small">{t("selectedN", { n: selected.size })}</span>
          <button className="btn tiny" onClick={() => batchSetState("RW")}>{t("batchRW")}</button>
          <button className="btn tiny" onClick={() => batchSetState("RO")}>{t("batchRO")}</button>
          <button className="btn tiny" onClick={() => batchSetState("SEALED")}>{t("batchSeal")}</button>
          <button className="btn ghost tiny" onClick={() => setSelected(new Set())}>{t("clearSelection")}</button>
        </div>
      )}

      {err && <div className="msg err">{err}</div>}
      {msg && <div className={`msg ${msg.kind}`}>{msg.text}</div>}

      <table>
        <thead>
          <tr>
            {isOperator && (
              <th>
                <input type="checkbox" checked={allPageSelected} onChange={toggleAll} />
              </th>
            )}
            <th>{t("partitions")}</th>
            <th>{t("state")}</th>
            <th>{t("health")}</th>
            <th>{t("rf")}</th>
            <th>{t("replicas")}</th>
            {isOperator && <th>{t("actions")}</th>}
          </tr>
        </thead>
        <tbody>
          {partitions.map((p) => {
            const h = health(p);
            const rows = [
              <tr key={p.partition_id}>
                {isOperator && (
                  <td>
                    <input
                      type="checkbox"
                      checked={selected.has(p.partition_id)}
                      onChange={() => toggleOne(p.partition_id)}
                    />
                  </td>
                )}
                <td>{p.partition_id}</td>
                <td><span className="pill small">{p.state}</span></td>
                <td>
                  <span className={`pill ${h.cls}`}>{h.text}</span>
                  {p.moving_out_nodes.length > 0 && (
                    <span className="pill warn small">{t("moving")}</span>
                  )}
                </td>
                <td>{p.replication_factor}</td>
                <td className="wrap">
                  {p.replicas.map((r) => {
                    const diverged = r.current_state != null && r.current_state !== r.target_state;
                    const err = r.current_state === "ERROR";
                    const title = `${r.addr} · ${r.disk_id} · ${r.up ? t("up") : t("down")}` +
                      (r.lag_bytes != null && r.lag_bytes > 0 ? ` · ${t("lag")} ${fmtBytes(r.lag_bytes)}` : "");
                    return (
                      <span
                        key={r.node_id}
                        className={`repl ${err ? "err" : !r.up ? "err" : diverged ? "warn" : ""}`}
                        title={title}
                      >
                        n{r.node_id}: {r.target_state}
                        {r.current_state != null && ` / ${r.current_state}`}
                        {!r.up && " ⚠"}
                        {r.lag_bytes != null && r.lag_bytes > 0 && ` (${t("lag")} ${fmtBytes(r.lag_bytes)})`}
                      </span>
                    );
                  })}
                </td>
                {isOperator && (
                  <td className="row-actions">
                    <button className="btn tiny" onClick={() => setState(p.partition_id, "RW")}>{t("markRW")}</button>
                    <button className="btn tiny" onClick={() => setState(p.partition_id, "RO")}>{t("markRO")}</button>
                    <button className="btn tiny" onClick={() => setState(p.partition_id, "SEALED")}>{t("markSealed")}</button>
                    <button className="btn tiny" onClick={() => repair(p.partition_id)}>{t("repair")}</button>
                    <button className="btn tiny" onClick={() => openMove(p)}>{t("move")}</button>
                  </td>
                )}
              </tr>,
            ];
            if (isOperator && moveFor === p.partition_id) {
              rows.push(
                <tr key={`${p.partition_id}-move`} className="detail">
                  <td colSpan={colSpan}>
                    <div className="inline-panel">
                      <label>
                        {t("fromNode")}
                        <select value={moveFrom} onChange={(e) => setMoveFrom(e.target.value ? +e.target.value : "")}>
                          {p.replicas.map((r) => (
                            <option key={r.node_id} value={r.node_id}>
                              {t("node")} {r.node_id} ({r.addr})
                            </option>
                          ))}
                        </select>
                      </label>
                      <label>
                        {t("toNode")}
                        <select value={moveTo} onChange={(e) => setMoveTo(e.target.value ? +e.target.value : "")}>
                          <option value="">{t("autoPick")}</option>
                          {aliveNodes.map((n) => (
                            <option key={n.node_id} value={n.node_id}>
                              {t("node")} {n.node_id} ({n.addr})
                            </option>
                          ))}
                        </select>
                      </label>
                      <button className="btn tiny" onClick={() => doMove(p)} disabled={moveFrom === ""}>
                        {t("confirmBtn")}
                      </button>
                      <button className="btn ghost tiny" onClick={() => setMoveFor(null)}>
                        {t("cancelBtn")}
                      </button>
                    </div>
                  </td>
                </tr>,
              );
            }
            return rows;
          })}
          {partitions.length === 0 && (
            <tr>
              <td colSpan={colSpan} className="empty">{t("noPartitions")}</td>
            </tr>
          )}
        </tbody>
      </table>
      <div className="pager">
        <button className="btn ghost" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - pageSize))}>
          {t("prev")}
        </button>
        <span>{t("page")} {pageNo} / {pageCount}</span>
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
