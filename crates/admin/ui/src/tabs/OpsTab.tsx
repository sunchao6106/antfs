import { useState } from "react";
import { api, fmtBytes, Node, Partition } from "../api";
import type { T } from "../App";

export function OpsTab({
  t,
  nodes,
  onDone,
  setError,
}: {
  t: T;
  nodes: Node[];
  onDone: () => void;
  setError: (s: string) => void;
}) {
  const [count, setCount] = useState(1);
  const [rf, setRf] = useState(3);
  const [decomId, setDecomId] = useState<number | "">("");
  const [decomErr, setDecomErr] = useState("");
  const [lookupId, setLookupId] = useState("");
  const [lookupErr, setLookupErr] = useState("");
  const [found, setFound] = useState<Partition | null>(null);

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
    </section>
  );
}
