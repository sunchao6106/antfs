import { useEffect, useState } from "react";
import { api,
  ScrubRun, Cluster, fmtBytes, Node, Partition } from "../api";
import type { T } from "../App";

export function OpsTab({
  t,
  nodes,
  cluster,
  onDone,
  setError,
}: {
  t: T;
  nodes: Node[];
  cluster: Cluster | null;
  onDone: () => void;
  setError: (s: string) => void;
}) {
  // 最近一致性巡检(Wave3):只读列表,10s 轮询(它由分区页触发,这里只做汇总展示)。
  const [scrubRuns, setScrubRuns] = useState<ScrubRun[]>([]);
  useEffect(() => {
    let alive = true;
    const poll = () =>
      api
        .scrubRuns()
        .then((r) => alive && setScrubRuns(r.runs))
        .catch(() => {});
    poll();
    const id = setInterval(poll, 10000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  const [count, setCount] = useState(1);
  const [rf, setRf] = useState(3);
  const [decomId, setDecomId] = useState<number | "">("");
  const [decomErr, setDecomErr] = useState("");
  const [lookupId, setLookupId] = useState("");
  const [lookupErr, setLookupErr] = useState("");
  const [found, setFound] = useState<Partition | null>(null);
  const [pauseAutogrow, setPauseAutogrow] = useState(false);
  const [maintErr, setMaintErr] = useState("");
  const [maintBusy, setMaintBusy] = useState(false);

  const maintenance = !!cluster?.maintenance;

  /** 维护模式开关:开启前二次确认;关闭时连带的 pause_autogrow 一并清零(master 侧语义)。 */
  const setMaintenance = async (paused: boolean) => {
    if (paused && !confirm(t("maintenanceConfirm"))) return;
    setMaintBusy(true);
    try {
      await api.setMaintenance(paused, paused && pauseAutogrow);
      setMaintErr("");
      onDone();
    } catch (e) {
      setMaintErr(String(e));
    } finally {
      setMaintBusy(false);
    }
  };

  const create = async () => {
    try {
      const r = await api.createPartitions(count, rf);
      setError("");
      alert(t("createdPartitions", { ids: r.partition_ids.join(", ") || "—" }));
      onDone();
    } catch (e) {
      setError(String(e));
    }
  };

  const decom = async (force: boolean) => {
    if (decomId === "") return;
    if (!force && !confirm(t("confirmDecom", { id: decomId }))) return;
    try {
      await api.decommission(Number(decomId), force);
      setError("");
      setDecomErr("");
      onDone();
    } catch (e) {
      if (!force) {
        // 被前置校验拒绝(错误文本含受影响分区列表)→ 提示区内给强制下线入口。
        setDecomErr(String(e));
      } else {
        setError(String(e));
      }
    }
  };

  const lookup = async () => {
    const id = Number(lookupId);
    if (!lookupId || Number.isNaN(id)) return;
    try {
      const p = await api.partition(id);
      setFound(p);
      setLookupErr("");
    } catch (e) {
      setFound(null);
      setLookupErr(String(e));
    }
  };

  return (
    <section className="ops">
      <div className="op-box">
        <h3>{t("maintenanceMode")}</h3>
        <div className="panel-head">
          <span className={`pill ${maintenance ? "maint" : "ok"}`}>
            {maintenance ? t("maintenanceOn") : t("maintenanceOff")}
          </span>
          {maintenance && cluster?.pause_autogrow && (
            <span className="pill info small">{t("pauseAutogrow")}</span>
          )}
        </div>
        <p className="card-sub">{t("maintenanceHint")}</p>
        <label className="check">
          <input
            type="checkbox"
            checked={maintenance ? !!cluster?.pause_autogrow : pauseAutogrow}
            disabled={maintenance}
            onChange={(e) => setPauseAutogrow(e.target.checked)}
          />
          {t("pauseAutogrow")}
        </label>
        <button
          className={`btn ${maintenance ? "" : "danger"}`}
          disabled={maintBusy}
          onClick={() => setMaintenance(!maintenance)}
        >
          {maintenance ? t("disableMaintenance") : t("enableMaintenance")}
        </button>
        {maintErr && <div className="msg err">{maintErr}</div>}
      </div>

      <div className="op-box">
        <h3>{t("createPartitions")}</h3>
        <label>
          {t("count")}
          <input type="number" min={1} value={count} onChange={(e) => setCount(+e.target.value)} />
        </label>
        <label>
          {t("replicationFactor")}
          <input type="number" min={1} value={rf} onChange={(e) => setRf(+e.target.value)} />
        </label>
        <button className="btn" onClick={create}>
          {t("create")}
        </button>
      </div>

      <div className="op-box">
        <h3>{t("decommissionNode")}</h3>
        <label>
          {t("node")}
          <select
            value={decomId}
            onChange={(e) => {
              setDecomId(e.target.value ? +e.target.value : "");
              setDecomErr("");
            }}
          >
            <option value="">{t("select")}</option>
            {nodes.map((n) => (
              <option key={n.node_id} value={n.node_id}>
                {t("node")} {n.node_id} ({n.addr})
              </option>
            ))}
          </select>
        </label>
        <button className="btn danger" onClick={() => decom(false)} disabled={decomId === ""}>
          {t("decommission")}
        </button>
        {decomErr && (
          <div className="msg err">
            <strong>{t("decomRejected")}</strong>
            {"\n"}
            {decomErr}
            <button className="btn danger tiny" onClick={() => decom(true)}>
              {t("forceDecommission")}
            </button>
          </div>
        )}
      </div>

      <div className="op-box">
        <h3>{t("lookupPartition")}</h3>
        <label>
          {t("partitionId")}
          <input
            type="number"
            min={0}
            value={lookupId}
            onChange={(e) => setLookupId(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && lookup()}
          />
        </label>
        <button className="btn" onClick={lookup} disabled={lookupId === ""}>
          {t("lookup")}
        </button>
        {lookupErr && <div className="msg err">{lookupErr}</div>}
        {found && (
          <div>
            <div className="disk-grid">
              <div className="disk-cell">
                {t("state")}: <span className="pill small">{found.state}</span>
              </div>
              <div className="disk-cell">
                {t("rf")}: {found.replication_factor} · {found.healthy_replicas}/{found.replication_factor}{" "}
                {t("healthy")}
              </div>
              {found.moving_out_nodes.length > 0 && (
                <div className="disk-cell">
                  {t("movingOut")}: {found.moving_out_nodes.map((n) => `n${n}`).join(", ")}
                </div>
              )}
            </div>
            <table>
              <thead>
                <tr>
                  <th>{t("node")}</th>
                  <th>{t("addr")}</th>
                  <th>{t("disks")}</th>
                  <th>{t("targetState")} / {t("currentState")}</th>
                  <th>{t("lag")}</th>
                </tr>
              </thead>
              <tbody>
                {found.replicas.map((r) => (
                  <tr key={r.node_id}>
                    <td>{r.node_id}</td>
                    <td className="mono">{r.addr}</td>
                    <td className="mono">{r.disk_id}</td>
                    <td>
                      {r.target_state} / {r.current_state ?? "—"} {!r.up && "⚠"}
                    </td>
                    <td>{r.lag_bytes != null && r.lag_bytes > 0 ? fmtBytes(r.lag_bytes) : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <section>
        <h2>{t("scrubRecentTitle")}</h2>
        {scrubRuns.length === 0 ? (
          <p className="empty">
            {t("scrubRecentEmpty")} — {t("scrubRecentHint")}
          </p>
        ) : (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>{t("scrubColPartition")}</th>
                  <th>{t("scrubColTime")}</th>
                  <th>{t("scrubColVerdict")}</th>
                  <th>{t("scrubColDivergences")}</th>
                  <th>{t("scrubColReplicas")}</th>
                </tr>
              </thead>
              <tbody>
                {scrubRuns.map((r, i) => (
                  <tr key={`${r.partition_id}-${r.checked_at_ms}-${i}`}>
                    <td>{r.partition_id}</td>
                    <td>{new Date(r.checked_at_ms).toLocaleString()}</td>
                    <td>
                      <span className={`pill small ${runVerdict(t, r).cls}`}>{runVerdict(t, r).text}</span>
                      {r.deep && <span className="pill small">{t("scrubDeepTag")}</span>}
                    </td>
                    <td>{r.divergence_count}</td>
                    <td className="mono">
                      {r.replicas.map((x) => `n${x.node_id}${x.ok ? `:${x.live_records}` : "!"}`).join(" ")}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </section>
  );
}

/** 最近巡检一行的结论徽标(走 i18n;与分区页 scrubVerdict 同口径)。 */
function runVerdict(t: T, r: ScrubRun): { text: string; cls: string } {
  if (r.inconclusive) return { text: t("scrubVerdictInconclusive"), cls: "warn" };
  if (!r.consistent) return { text: t("scrubVerdictInconsistent"), cls: "down" };
  if (r.partial) return { text: t("scrubVerdictPartial"), cls: "warn" };
  return { text: t("scrubVerdictConsistent"), cls: "ok" };
}
