import { useCallback, useEffect, useState } from "react";
import { api, AuditEntry } from "../api";
import type { T } from "../App";

/** 可选条数(后端上限 2000)。 */
const LIMITS = [50, 100, 200, 500, 1000, 2000];
const DEFAULT_LIMIT = 200;

/** HTTP 状态码 → pill 配色(2xx 正常、4xx 警告、5xx 错误)。 */
function statusCls(status: number): string {
  if (status >= 500) return "down";
  if (status >= 400) return "warn";
  return "ok";
}

/** 后端与前端同机时本地时间即可;不做时区推断。 */
function fmtTime(ms: number): string {
  return new Date(ms).toLocaleString();
}

export function AuditTab({ t }: { t: T }) {
  const [limit, setLimit] = useState(DEFAULT_LIMIT);
  const [entries, setEntries] = useState<AuditEntry[]>([]);
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    setBusy(true);
    try {
      setEntries((await api.audit(limit)).entries);
      setErr("");
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  }, [limit]);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <section>
      <div className="toolbar">
        <label className="check">
          {t("auditLimit")}
          <select value={limit} onChange={(e) => setLimit(Number(e.target.value))}>
            {LIMITS.map((n) => (
              <option key={n} value={n}>{n}</option>
            ))}
          </select>
        </label>
        <div className="spacer" />
        <button className="btn" disabled={busy} onClick={() => void load()}>
          {t("auditRefresh")}
        </button>
      </div>

      {err && <div className="msg err">{t("auditError", { err })}</div>}

      <table>
        <thead>
          <tr>
            <th>{t("auditTime")}</th>
            <th>{t("auditActor")}</th>
            <th>{t("auditRole")}</th>
            <th>{t("auditAction")}</th>
            <th>{t("auditStatus")}</th>
            <th>{t("auditSummary")}</th>
            <th>{t("auditIp")}</th>
          </tr>
        </thead>
        <tbody>
          {entries.map((e, i) => (
            <tr key={`${e.ts_ms}-${i}`}>
              <td className="mono">{fmtTime(e.ts_ms)}</td>
              <td>{e.actor}</td>
              <td><span className="pill small">{e.role}</span></td>
              <td className="mono">{e.method}</td>
              <td><span className={`pill small ${statusCls(e.status)}`}>{e.status}</span></td>
              <td className="mono wrap">{e.summary}</td>
              <td className="mono">{e.remote_addr || "—"}</td>
            </tr>
          ))}
          {entries.length === 0 && (
            <tr>
              <td colSpan={7} className="empty">{t("auditEmpty")}</td>
            </tr>
          )}
        </tbody>
      </table>
    </section>
  );
}
