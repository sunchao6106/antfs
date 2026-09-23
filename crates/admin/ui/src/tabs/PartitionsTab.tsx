import { useCallback, useEffect, useMemo, useState } from "react";
import {
  api,
  CompactionJob,
  CompactJobsNodeResult,
  fmtBytes,
  Node,
  Partition,
  ScrubResult,
} from "../api";
import { partitionHealth } from "../health";
import type { T } from "../App";

const REFRESH_MS = 5000;
const PAGE_SIZES = [10, 20, 50, 100];
/** 后台 compact 的进度轮询间隔(store 台账是内存态,轮询足够)。 */
const JOB_POLL_MS = 2000;

type Msg = { kind: "ok" | "err"; text: string } | null;

/** 一行 = 一个副本节点的后台 compact 进度。 */
interface JobRow {
  node_id: number;
  /** 提交时拿到的 job_id(提交失败则没有)。 */
  job_id?: number;
  /** `running` / `done` / `failed`(提交即 running)。 */
  state: "running" | "done" | "failed";
  /** 该副本的 compact 结果(仅 done)。 */
  result?: CompactionJob["result"];
  /** 失败原因(提交失败 / 任务失败 / 节点不可达)。 */
  error?: string;
}

/** 该副本已到终态(失败或完成)→ 不必再等它。 */
function terminal(r: JobRow): boolean {
  return r.state === "done" || r.state === "failed";
}

/** 进度行状态 → i18n 文案与 pill 配色。 */
function jobState(t: T, state: JobRow["state"]): { text: string; cls: string } {
  switch (state) {
    case "done":
      return { text: t("jobDone"), cls: "ok" };
    case "failed":
      return { text: t("jobFailed"), cls: "down" };
    case "running":
      return { text: t("jobRunning"), cls: "" };
  }
}

/**
 * 用最新一轮 `compact-jobs` 更新进度行:只认**该行提交时的 job_id**(台账里同名分区的旧任务不算);
 * 节点本轮不可达(`ok:false`)且该行未到终态 → 记下原因,等下一轮。
 */
function mergeJobs(prev: JobRow[], results: CompactJobsNodeResult[]): JobRow[] {
  return prev.map((row) => {
    if (terminal(row)) return row;
    const node = results.find((r) => r.node_id === row.node_id);
    if (!node) return row;
    if (!node.ok) return { ...row, error: node.error ?? "" };
    const job = (node.jobs ?? []).find((j) => j.job_id === row.job_id);
    if (!job) return row;
    return {
      ...row,
      state: job.state,
      result: job.result,
      error: job.state === "failed" ? (job.error ?? "") : undefined,
    };
  });
}

export function PartitionsTab({
  t,
  nodes,
  isOperator,
  maintenance,
  stateFilter,
  onStateFilter,
}: {
  t: T;
  nodes: Node[];
  isOperator: boolean;
  /** 维护中:自动收敛(再复制/迁移)已暂停,迁移徽标旁给出提示。 */
  maintenance: boolean;
  /** 状态筛选(受控,来自 App 的 hash query;刷新后仍生效)。 */
  stateFilter: string;
  onStateFilter: (s: string) => void;
}) {
  const [pageSize, setPageSize] = useState(10);
  const [offset, setOffset] = useState(0);
  const [underOnly, setUnderOnly] = useState(false);
  const [partitions, setPartitions] = useState<Partition[]>([]);
  const [total, setTotal] = useState(0);
  const [err, setErr] = useState("");
  const [msg, setMsg] = useState<Msg>(null);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [moveFor, setMoveFor] = useState<number | null>(null);
  const [moveFrom, setMoveFrom] = useState<number | "">("");
  const [moveTo, setMoveTo] = useState<number | "">("");
  /** 正在跟进的「后台整理」分区(展开进度行的那个)与其逐副本进度。 */
  const [asyncFor, setAsyncFor] = useState<number | null>(null);
  // 一致性巡检(Wave3):逐分区展开面板 + 结果;deep 由 checkbox 控制。
  const [scrubFor, setScrubFor] = useState<number | null>(null);
  const [scrubDeep, setScrubDeep] = useState(false);
  const [scrubBusy, setScrubBusy] = useState(false);
  const [scrubResult, setScrubResult] = useState<ScrubResult | null>(null);
  const [asyncRows, setAsyncRows] = useState<JobRow[]>([]);
  /** 某个副本的 job 已到终态(或提交即失败)→ 整轮结束,停止轮询。 */
  const asyncDone = asyncRows.length > 0 && asyncRows.every(terminal);

  const refresh = useCallback(async () => {
    try {
      if (underOnly) {
        // 欠副本无服务端筛选参数:取全量(仍按 state 过滤)后本地筛 + 本地分页。
        const p = await api.partitions(0, 0, stateFilter);
        const items = p.items.filter((x) => !!x.under_replicated_reason);
        setPartitions(items.slice(offset, offset + pageSize));
        setTotal(items.length);
      } else {
        // state 过滤由 master 做(过滤先于分页),total 即过滤后的总数。
        const p = await api.partitions(offset, pageSize, stateFilter);
        setPartitions(p.items);
        setTotal(p.total);
      }
      setErr("");
    } catch (e) {
      setErr(String(e));
    }
  }, [stateFilter, underOnly, offset, pageSize]);

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

  /** 手动 compact:所有副本节点并发触发,逐节点结果拼进消息条(ok 节点报回收字节,失败报原因)。 */
  const compact = async (id: number) => {
    try {
      const r = await api.compactPartition(id);
      if (r.error) {
        setMsg({ kind: "err", text: t("compactErr", { err: r.error }) });
        return;
      }
      if (r.results.length === 0) {
        setMsg({ kind: "err", text: t("compactNoReplicas") });
        return;
      }
      const parts = r.results.map((x) =>
        x.ok
          ? t("compactNodeOk", { node: x.node_id, bytes: fmtBytes(x.dead_bytes ?? 0) })
          : t("compactNodeFail", { node: x.node_id, err: x.error ?? "" }),
      );
      setMsg({ kind: r.results.every((x) => x.ok) ? "ok" : "err", text: parts.join("; ") });
    } catch (e) {
      setMsg({ kind: "err", text: String(e) });
    }
  };

  /**
   * 后台整理:各副本节点提交任务并立即回 `job_id`,然后在行下方展开逐副本进度
   * (轮询 `compact-jobs` 直到全部终态,或用户收起)。
   */
  const compactAsync = async (id: number) => {
    try {
      const r = await api.compactPartitionAsync(id);
      const rows: JobRow[] = r.results.map((x) =>
        x.ok && x.job_id != null
          ? { node_id: x.node_id, job_id: x.job_id, state: "running" }
          : { node_id: x.node_id, state: "failed", error: x.error ?? r.error },
      );
      setAsyncRows(rows);
      setAsyncFor(rows.length > 0 ? id : null);
      if (r.error) setMsg({ kind: "err", text: t("compactErr", { err: r.error }) });
      else if (rows.length === 0) setMsg({ kind: "err", text: t("compactNoReplicas") });
      else setMsg(null);
    } catch (e) {
      setMsg({ kind: "err", text: String(e) });
    }
  };

  /** 收起进度行 = 不再跟进(轮询 effect 依赖 `asyncFor` 自动停)。 */
  const collapseAsync = () => {
    setAsyncFor(null);
    setAsyncRows([]);
  };

  // 轮询直到全部终态(或收起);失败只提示,下一轮继续。
  useEffect(() => {
    if (asyncFor == null || asyncDone) return;
    const timer = setInterval(() => {
      void (async () => {
        try {
          const r = await api.compactJobs(asyncFor);
          setAsyncRows((prev) => mergeJobs(prev, r.results));
        } catch (e) {
          setMsg({ kind: "err", text: t("compactJobsPollErr", { err: String(e) }) });
        }
      })();
    }, JOB_POLL_MS);
    return () => clearInterval(timer);
  }, [asyncFor, asyncDone, t]);

  const openMove = (p: Partition) => {    if (moveFor === p.partition_id) {
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
  const runScrub = useCallback(async (id: number, deep: boolean) => {
    setScrubFor(id);
    setScrubBusy(true);
    try {
      const r = await api.scrubPartition(id, deep);
      setScrubResult(r);
      setMsg(null);
    } catch (e) {
      setScrubResult(null);
      setMsg({ kind: "err", text: String(e) });
    } finally {
      setScrubBusy(false);
    }
  }, []);

  const aliveNodes = useMemo(() => nodes.filter((n) => n.up), [nodes]);

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
              onStateFilter(e.target.value);
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
            const h = partitionHealth(t, p);
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
                  {maintenance && p.moving_out_nodes.length > 0 && (
                    <span className="pill info small">{t("autoConvergePaused")}</span>
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
                    <button className="btn tiny" onClick={() => compact(p.partition_id)}>{t("compact")}</button>
                    <button className="btn tiny" onClick={() => compactAsync(p.partition_id)}>{t("compactAsync")}</button>
                    <button
                      className="btn tiny"
                      onClick={() =>
                        scrubFor === p.partition_id
                          ? (setScrubFor(null), setScrubResult(null))
                          : runScrub(p.partition_id, scrubDeep)
                      }
                    >
                      {t("scrub")}
                    </button>
                  </td>
                )}
              </tr>,
            ];
            if (isOperator && asyncFor === p.partition_id) {
              rows.push(
                <tr key={`${p.partition_id}-jobs`} className="detail">
                  <td colSpan={colSpan}>
                    <div className="panel">
                      <div className="panel-head">
                        <span className="card-label">
                          {t("compactJobsTitle", { id: p.partition_id })}
                        </span>
                        <div className="spacer" />
                        <button className="btn ghost tiny" onClick={collapseAsync}>
                          {t("collapseBtn")}
                        </button>
                      </div>
                      {asyncRows.map((r) => {
                        const s = jobState(t, r.state);
                        return (
                          <div key={r.node_id} className="kv-row">
                            <span className="k">
                              {t("node")} {r.node_id}
                            </span>
                            <span className="v">
                              <span className={`pill small ${s.cls}`}>{s.text}</span>
                              {r.result && ` ${t("compactReclaimed", { bytes: fmtBytes(r.result.dead_bytes) })}`}
                              {r.state === "failed" && ` ${r.error}`}
                              {!terminal(r) && r.error && ` (${r.error})`}
                            </span>
                          </div>
                        );
                      })}
                    </div>
                  </td>
                </tr>,
              );
            }
            if (scrubFor === p.partition_id) {
              rows.push(
                <tr key={`${p.partition_id}-scrub`} className="detail">
                  <td colSpan={colSpan}>
                    <div className="panel">
                      <div className="panel-head">
                        <span className="card-label">{t("scrubTitle", { id: p.partition_id })}</span>
                        <div className="spacer" />
                        <label className="inline-check">
                          <input
                            type="checkbox"
                            checked={scrubDeep}
                            onChange={(e) => setScrubDeep(e.target.checked)}
                          />
                          {t("scrubDeep")}
                        </label>
                        <button
                          className="btn tiny"
                          disabled={scrubBusy}
                          onClick={() => runScrub(p.partition_id, scrubDeep)}
                        >
                          {scrubBusy ? t("scrubRunning") : t("scrubRerun")}
                        </button>
                        <button
                          className="btn ghost tiny"
                          onClick={() => {
                            setScrubFor(null);
                            setScrubResult(null);
                          }}
                        >
                          {t("collapseBtn")}
                        </button>
                      </div>
                      {scrubBusy && !scrubResult && <p className="empty">{t("scrubRunning")}</p>}
                      {scrubResult && (
                        <>
                          <div className="kv-row">
                            <span className="k">{t("scrubVerdict")}</span>
                            <span className="v">
                              <span className={`pill small ${scrubVerdict(t, scrubResult).cls}`}>
                                {scrubVerdict(t, scrubResult).text}
                              </span>
                              {` · ${t("scrubCheckedAt")} ${new Date(scrubResult.checked_at_ms).toLocaleTimeString()}`}
                              {scrubResult.deep && ` · ${t("scrubDeepTag")}`}
                            </span>
                          </div>
                          {scrubResult.replicas.map((r) => (
                            <div key={r.node_id} className="kv-row">
                              <span className="k">{t("node")} {r.node_id}</span>
                              <span className="v">
                                {r.ok && r.report ? (
                                  <>
                                    {`records=${r.report.live_records} bytes=${fmtBytes(r.report.live_bytes)} tombstones=${r.report.tombstones} segs=${r.report.sealed_segments}`}
                                    {r.key_count != null && ` keys=${r.key_count}`}
                                    {r.partial && <span className="pill small warn">{t("scrubPartial")}</span>}
                                    {r.verify &&
                                      ` · ${t("scrubVerify")} bad=${r.verify.bad_records} idx=${r.verify.index_mismatch}${r.verify.truncated ? " (truncated)" : ""}`}
                                  </>
                                ) : (
                                  <span className="pill small down">{r.error || t("scrubUnavailable")}</span>
                                )}
                              </span>
                            </div>
                          ))}
                          <div className="kv-row">
                            <span className="k">{t("scrubDivergences")}</span>
                            <span className="v">
                              {scrubResult.verdict.divergences.length === 0 ? (
                                t("scrubNoDivergences")
                              ) : (
                                <ul className="scrub-divs">
                                  {scrubResult.verdict.divergences.map((d, i) => (
                                    <li key={i}>
                                      {d.kind === "summary" && `${d.field}: ${d.detail}`}
                                      {d.kind === "verify" && `n${d.node_id}: ${d.detail}`}
                                      {d.kind === "key" &&
                                        `${t("scrubKeyDiff")} ${d.key_hex.slice(0, 12)}… ${d.nodes
                                          .map((n) => `n${n.node_id}=${n.state}`)
                                          .join(" ")}`}
                                    </li>
                                  ))}
                                </ul>
                              )}
                            </span>
                          </div>
                        </>
                      )}
                    </div>
                  </td>
                </tr>,
              );
            }
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

/** scrub 结论 → 徽标文案(走 i18n)与配色(不一致红 / 无法判定或部分比对黄 / 一致绿)。 */
export function scrubVerdict(t: T, r: ScrubResult): { text: string; cls: string } {
  if (r.verdict.inconclusive) return { text: t("scrubVerdictInconclusive"), cls: "warn" };
  if (!r.verdict.consistent && r.verdict.divergences.length > 0) {
    return { text: t("scrubVerdictInconsistent"), cls: "down" };
  }
  if (r.verdict.partial) return { text: t("scrubVerdictPartial"), cls: "warn" };
  return { text: t("scrubVerdictConsistent"), cls: "ok" };
}
