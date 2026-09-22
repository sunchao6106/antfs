import { useCallback, useEffect, useState } from "react";
import {
  api,
  Cluster,
  fmtBytes,
  getLang,
  getToken,
  Lang,
  Node,
  Partition,
  Role,
  setLang,
  setToken,
  Unauthorized,
} from "./api";
import { makeT } from "./i18n";

const REFRESH_MS = 5000;
const PAGE_SIZE = 10;

export function App() {
  const [lang, setLangState] = useState<Lang>(getLang());
  const t = makeT(lang);
  const [role, setRole] = useState<Role>("none");
  const [cluster, setCluster] = useState<Cluster | null>(null);
  const [nodes, setNodes] = useState<Node[]>([]);
  const [partitions, setPartitions] = useState<Partition[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [error, setError] = useState<string>("");
  const [needAuth, setNeedAuth] = useState(false);

  const isOperator = role === "operator" || role === "none";

  const refresh = useCallback(async () => {
    try {
      const [me, c, n, p] = await Promise.all([
        api.me(),
        api.cluster(),
        api.nodes(),
        api.partitions(offset, PAGE_SIZE),
      ]);
      setRole(me.role);
      setCluster(c);
      setNodes(n);
      setPartitions(p.items);
      setTotal(p.total);
      setError("");
      setNeedAuth(false);
    } catch (e) {
      if (e instanceof Unauthorized) setNeedAuth(true);
      else setError(String(e));
    }
  }, [offset]);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, REFRESH_MS);
    return () => clearInterval(id);
  }, [refresh]);

  const switchLang = (l: Lang) => {
    setLang(l);
    setLangState(l);
  };

  if (needAuth) return <Login t={t} onSaved={refresh} lang={lang} switchLang={switchLang} />;

  return (
    <div className="app">
      <header>
        <h1>{t("title")}</h1>
        <div className="spacer" />
        <LangToggle lang={lang} switchLang={switchLang} />
        {role !== "none" && (
          <span className="pill small">
            {t("role")}: {role === "viewer" ? t("viewerReadOnly") : role}
          </span>
        )}
        {cluster?.grafana_url && (
          <a className="btn ghost" href={cluster.grafana_url} target="_blank" rel="noreferrer">
            📈 {t("grafana")}
          </a>
        )}
        <button className="btn ghost" onClick={() => { setToken(""); setNeedAuth(true); }}>
          {t("signOut")}
        </button>
      </header>

      {error && <div className="banner err">{error}</div>}

      <Overview t={t} cluster={cluster} total={total} />
      {isOperator && <Ops t={t} nodes={nodes} onDone={refresh} setError={setError} />}
      <Nodes t={t} nodes={nodes} />
      <Partitions
        t={t}
        partitions={partitions}
        total={total}
        offset={offset}
        setOffset={setOffset}
        isOperator={isOperator}
        onDone={refresh}
        setError={setError}
      />
    </div>
  );
}

type T = ReturnType<typeof makeT>;

function LangToggle({ lang, switchLang }: { lang: Lang; switchLang: (l: Lang) => void }) {
  return (
    <div className="lang">
      <button className={`lang-btn ${lang === "en" ? "on" : ""}`} onClick={() => switchLang("en")}>
        EN
      </button>
      <button className={`lang-btn ${lang === "zh" ? "on" : ""}`} onClick={() => switchLang("zh")}>
        中
      </button>
    </div>
  );
}

function Overview({ t, cluster, total }: { t: T; cluster: Cluster | null; total: number }) {
  const under = cluster?.under_replicated_partitions ?? 0;
  return (
    <section className="cards">
      <Card label={t("leader")} value={cluster ? `${t("node")} ${cluster.leader ?? "—"}` : "—"} />
      <Card label={t("raftTerm")} value={cluster ? String(cluster.term) : "—"} />
      <Card label={t("nodesUpTotal")} value={cluster ? `${cluster.nodes_up} / ${cluster.nodes_total}` : "—"} />
      <Card label={t("partitions")} value={String(total)} />
      <Card
        label={t("capacity")}
        value={cluster ? `${fmtBytes(cluster.used_bytes)} / ${fmtBytes(cluster.total_capacity_bytes)}` : "—"}
      />
      <Card label={t("freeCapacity")} value={cluster ? fmtBytes(cluster.free_bytes) : "—"} />
      <Card label={t("underReplicated")} value={String(under)} danger={under > 0} />
    </section>
  );
}

function Card({ label, value, danger }: { label: string; value: string; danger?: boolean }) {
  return (
    <div className={`card ${danger ? "danger" : ""}`}>
      <div className="card-label">{label}</div>
      <div className="card-value">{value}</div>
    </div>
  );
}

function Ops({ t, nodes, onDone, setError }: { t: T; nodes: Node[]; onDone: () => void; setError: (s: string) => void }) {
  const [count, setCount] = useState(1);
  const [rf, setRf] = useState(3);
  const [decomId, setDecomId] = useState<number | "">("");

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
  const decom = async () => {
    if (decomId === "") return;
    if (!confirm(t("confirmDecom", { id: decomId }))) return;
    try {
      await api.decommission(Number(decomId));
      setError("");
      onDone();
    } catch (e) {
      // 被前置校验拒绝(含受影响分区)→ 询问是否强制下线。
      const msg = String(e);
      if (confirm(t("decomForce", { err: msg }))) {
        try {
          await api.decommission(Number(decomId), true);
          setError("");
          onDone();
        } catch (e2) {
          setError(String(e2));
        }
      } else {
        setError(msg);
      }
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
          <select value={decomId} onChange={(e) => setDecomId(e.target.value ? +e.target.value : "")}>
            <option value="">{t("select")}</option>
            {nodes.map((n) => (
              <option key={n.node_id} value={n.node_id}>
                {t("node")} {n.node_id} ({n.addr})
              </option>
            ))}
          </select>
        </label>
        <button className="btn danger" onClick={decom} disabled={decomId === ""}>
          {t("decommission")}
        </button>
      </div>
    </section>
  );
}

function Nodes({ t, nodes }: { t: T; nodes: Node[] }) {
  return (
    <section>
      <h2>{t("nodes")}</h2>
      <table>
        <thead>
          <tr>
            <th>{t("node")}</th>
            <th>{t("dcRack")}</th>
            <th>{t("addr")}</th>
            <th>{t("status")}</th>
            <th>{t("disks")}</th>
          </tr>
        </thead>
        <tbody>
          {nodes.map((n) => (
            <tr key={n.node_id}>
              <td>{n.node_id}</td>
              <td>{n.dc} / {n.rack}</td>
              <td className="mono">{n.addr}</td>
              <td>
                <span className={`pill ${n.up ? "ok" : "down"}`}>{n.up ? t("up") : t("down")}</span>
              </td>
              <td>
                {n.disks.map((d) => (
                  <div key={d.disk_id} className="disk">
                    <span className="mono">{d.disk_id}</span> {fmtBytes(d.free_bytes)} /{" "}
                    {fmtBytes(d.capacity_bytes)}
                    {d.health && <span className="pill small">{d.health}</span>}
                  </div>
                ))}
              </td>
            </tr>
          ))}
          {nodes.length === 0 && (
            <tr>
              <td colSpan={5} className="empty">{t("noNodes")}</td>
            </tr>
          )}
        </tbody>
      </table>
    </section>
  );
}

function Partitions({
  t,
  partitions,
  total,
  offset,
  setOffset,
  isOperator,
  onDone,
  setError,
}: {
  t: T;
  partitions: Partition[];
  total: number;
  offset: number;
  setOffset: (n: number) => void;
  isOperator: boolean;
  onDone: () => void;
  setError: (s: string) => void;
}) {
  const setState = async (id: number, state: string) => {
    try {
      await api.setPartitionState(id, state);
      setError("");
      onDone();
    } catch (e) {
      setError(String(e));
    }
  };
  const pageNo = Math.floor(offset / PAGE_SIZE) + 1;
  const pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE));

  const health = (p: Partition): { cls: string; text: string } => {
    const frac = `${p.healthy_replicas}/${p.replication_factor}`;
    if (!p.under_replicated_reason) return { cls: "ok", text: `${frac} ${t("healthy")}` };
    const reason = t(`reason_${p.under_replicated_reason}`);
    if (!p.healable) return { cls: "down", text: `${frac} · ${t("stuck")} · ${reason}` };
    return { cls: "warn", text: `${frac} · ${t("recovering")} · ${reason}` };
  };

  return (
    <section>
      <h2>{t("partitions")}</h2>
      <table>
        <thead>
          <tr>
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
            return (
              <tr key={p.partition_id}>
                <td>{p.partition_id}</td>
                <td><span className="pill small">{p.state}</span></td>
                <td><span className={`pill ${h.cls}`}>{h.text}</span></td>
                <td>{p.replication_factor}</td>
                <td>
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
                  </td>
                )}
              </tr>
            );
          })}
          {partitions.length === 0 && (
            <tr>
              <td colSpan={isOperator ? 6 : 5} className="empty">{t("noPartitions")}</td>
            </tr>
          )}
        </tbody>
      </table>
      <div className="pager">
        <button className="btn ghost" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}>
          {t("prev")}
        </button>
        <span>{t("page")} {pageNo} / {pageCount}</span>
        <button
          className="btn ghost"
          disabled={offset + PAGE_SIZE >= total}
          onClick={() => setOffset(offset + PAGE_SIZE)}
        >
          {t("next")}
        </button>
      </div>
    </section>
  );
}

function Login({
  t,
  onSaved,
  lang,
  switchLang,
}: {
  t: T;
  onSaved: () => void;
  lang: Lang;
  switchLang: (l: Lang) => void;
}) {
  const [val, setVal] = useState(getToken());
  return (
    <div className="login">
      <div className="login-box">
        <div className="login-head">
          <h2>{t("title")}</h2>
          <LangToggle lang={lang} switchLang={switchLang} />
        </div>
        <p>{t("tokenRequired")}</p>
        <input
          type="password"
          placeholder={t("operatorToken")}
          value={val}
          onChange={(e) => setVal(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && (setToken(val), onSaved())}
        />
        <button className="btn" onClick={() => (setToken(val), onSaved())}>
          {t("signIn")}
        </button>
      </div>
    </div>
  );
}
