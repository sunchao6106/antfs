import { useCallback, useEffect, useState } from "react";
import {
  api,
  Cluster,
  getLang,
  getToken,
  Lang,
  Node,
  Role,
  setLang,
  setToken,
  Unauthorized,
} from "./api";
import { makeT } from "./i18n";
import { OverviewTab } from "./tabs/OverviewTab";
import { CapacityTab } from "./tabs/CapacityTab";
import { NodesTab } from "./tabs/NodesTab";
import { PartitionsTab } from "./tabs/PartitionsTab";
import { MetricsTab } from "./tabs/MetricsTab";
import { BalanceTab } from "./tabs/BalanceTab";
import { OpsTab } from "./tabs/OpsTab";
import { AuditTab } from "./tabs/AuditTab";

const REFRESH_MS = 5000;

type Tab = "overview" | "capacity" | "nodes" | "partitions" | "metrics" | "balance" | "ops" | "audit";
const ALL_TABS: Tab[] = ["overview", "capacity", "nodes", "partitions", "metrics", "balance", "ops", "audit"];

/** 仅 operator 可见的 tab(viewer 即使手改 hash 也回落到概览)。 */
const OPERATOR_TABS: Tab[] = ["ops", "audit"];

/**
 * tab 与筛选条件都存在 hash 里(`#partitions?state=RW`、`#nodes?q=101`),故刷新/分享链接后仍生效;
 * 跨 tab 跳转(概览「查看全部」、容量「查看全部 →」)也走这里。
 */
type Hash = { tab: Tab; params: URLSearchParams };

function parseHash(): Hash {
  const [name, query = ""] = window.location.hash.replace(/^#/, "").split("?");
  const tab = (ALL_TABS as string[]).includes(name) ? (name as Tab) : "overview";
  return { tab, params: new URLSearchParams(query) };
}

export type T = ReturnType<typeof makeT>;

export function App() {
  const [lang, setLangState] = useState<Lang>(getLang());
  const t = makeT(lang);
  const [role, setRole] = useState<Role>("none");
  const [cluster, setCluster] = useState<Cluster | null>(null);
  const [nodes, setNodes] = useState<Node[]>([]);
  const [error, setError] = useState<string>("");
  const [needAuth, setNeedAuth] = useState(false);
  const [hash, setHash] = useState<Hash>(parseHash);

  const isOperator = role === "operator" || role === "none";

  const refresh = useCallback(async () => {
    try {
      const [me, c, n] = await Promise.all([api.me(), api.cluster(), api.nodes()]);
      setRole(me.role);
      setCluster(c);
      setNodes(n);
      setError("");
      setNeedAuth(false);
    } catch (e) {
      if (e instanceof Unauthorized) setNeedAuth(true);
      else setError(String(e));
    }
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, REFRESH_MS);
    return () => clearInterval(id);
  }, [refresh]);

  useEffect(() => {
    const onHash = () => setHash(parseHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  /** 切 tab / 带筛选条件跳转(写 hash,刷新后仍生效)。 */
  const goto = (tb: Tab, params: Record<string, string> = {}) => {
    const query = new URLSearchParams(params).toString();
    window.location.hash = query ? `${tb}?${query}` : tb;
    setHash({ tab: tb, params: new URLSearchParams(params) });
  };

  const setTab = (tb: Tab) => goto(tb);

  const switchLang = (l: Lang) => {
    setLang(l);
    setLangState(l);
  };

  if (needAuth) return <Login t={t} onSaved={refresh} lang={lang} switchLang={switchLang} />;

  const tabs = ALL_TABS.filter((tb) => !OPERATOR_TABS.includes(tb) || isOperator);
  const active = OPERATOR_TABS.includes(hash.tab) && !isOperator ? "overview" : hash.tab;

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
        {cluster?.maintenance && <span className="pill maint">{t("maintenanceBadge")}</span>}
        {cluster?.grafana_url && (
          <a className="btn ghost" href={cluster.grafana_url} target="_blank" rel="noreferrer">
            📈 {t("grafana")}
          </a>
        )}
        <button className="btn ghost" onClick={() => { setToken(""); setNeedAuth(true); }}>
          {t("signOut")}
        </button>
      </header>

      <nav className="tabs">
        {tabs.map((tb) => (
          <button
            key={tb}
            className={`tab-btn ${active === tb ? "on" : ""}`}
            onClick={() => setTab(tb)}
          >
            {t(`tab_${tb}`)}
          </button>
        ))}
      </nav>

      {error && <div className="banner err">{error}</div>}

      {active === "overview" && (
        <OverviewTab
          t={t}
          cluster={cluster}
          nodes={nodes}
          onGotoNodes={(id) => goto("nodes", { q: String(id) })}
          onGotoPartitions={(state) => goto("partitions", { state })}
        />
      )}
      {active === "capacity" && (
        <CapacityTab
          t={t}
          cluster={cluster}
          onGotoPartitions={(state) => goto("partitions", { state })}
        />
      )}
      {active === "nodes" && <NodesTab t={t} nodes={nodes} isOperator={isOperator} initialQuery={hash.params.get("q") ?? ""} />}
      {active === "partitions" && (
        <PartitionsTab
          t={t}
          nodes={nodes}
          isOperator={isOperator}
          maintenance={!!cluster?.maintenance}
          stateFilter={hash.params.get("state") ?? ""}
          onStateFilter={(s) => goto("partitions", s ? { state: s } : {})}
        />
      )}
      {active === "metrics" && <MetricsTab t={t} cluster={cluster} />}
      {active === "balance" && <BalanceTab t={t} isOperator={isOperator} />}
      {active === "ops" && isOperator && (
        <OpsTab t={t} nodes={nodes} cluster={cluster} onDone={refresh} setError={setError} />
      )}
      {active === "audit" && isOperator && <AuditTab t={t} />}
    </div>
  );
}

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
