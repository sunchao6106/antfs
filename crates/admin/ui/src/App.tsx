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
import { NodesTab } from "./tabs/NodesTab";
import { PartitionsTab } from "./tabs/PartitionsTab";
import { MetricsTab } from "./tabs/MetricsTab";
import { OpsTab } from "./tabs/OpsTab";

const REFRESH_MS = 5000;

type Tab = "overview" | "nodes" | "partitions" | "metrics" | "ops";
const ALL_TABS: Tab[] = ["overview", "nodes", "partitions", "metrics", "ops"];

function tabFromHash(): Tab {
  const h = window.location.hash.replace(/^#/, "");
  return (ALL_TABS as string[]).includes(h) ? (h as Tab) : "overview";
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
  const [tab, setTabState] = useState<Tab>(tabFromHash());

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
    const onHash = () => setTabState(tabFromHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const setTab = (tb: Tab) => {
    window.location.hash = tb;
    setTabState(tb);
  };

  const switchLang = (l: Lang) => {
    setLang(l);
    setLangState(l);
  };

  if (needAuth) return <Login t={t} onSaved={refresh} lang={lang} switchLang={switchLang} />;

  const tabs = ALL_TABS.filter((tb) => tb !== "ops" || isOperator);
  const active = tab === "ops" && !isOperator ? "overview" : tab;

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

      {active === "overview" && <OverviewTab t={t} cluster={cluster} />}
      {active === "nodes" && <NodesTab t={t} nodes={nodes} />}
      {active === "partitions" && <PartitionsTab t={t} nodes={nodes} isOperator={isOperator} />}
      {active === "metrics" && <MetricsTab t={t} cluster={cluster} />}
      {active === "ops" && isOperator && <OpsTab t={t} nodes={nodes} onDone={refresh} setError={setError} />}
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
