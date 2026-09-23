import { useCallback, useEffect, useState } from "react";
import { api, BalancePlan, BalanceState } from "../api";
import type { T } from "../App";

const REFRESH_MS = 5000;

/** 计划状态徽标:PREVIEW 待确认(黄)/ RUNNING 执行中(蓝)/ DONE 已完成(绿)/ CANCELLED 已取消(灰)。 */
function planPillClass(status: string): string {
  switch (status) {
    case "PREVIEW":
      return "warn";
    case "RUNNING":
      return "info";
    case "DONE":
      return "ok";
    default:
      return ""; // CANCELLED 用默认灰
  }
}

/** move 状态徽标:done 绿 / in_flight 蓝 / pending 灰 / failed·infeasible 红。 */
function movePillClass(status: string): string {
  switch (status) {
    case "done":
      return "ok";
    case "in_flight":
      return "info";
    case "failed":
    case "infeasible":
      return "down";
    default:
      return ""; // pending
  }
}

export function BalanceTab({ t, isOperator }: { t: T; isOperator: boolean }) {
  const [state, setState] = useState<BalanceState | null>(null);
  const [loadErr, setLoadErr] = useState("");
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [maxMoves, setMaxMoves] = useState("");
  const [busy, setBusy] = useState(false);

  const refresh = useCallback(async () => {
    try {
      setState(await api.balanceState());
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

  const evalPlan = async () => {
    const trimmed = maxMoves.trim();
    const mm = trimmed === "" ? undefined : Number(trimmed);
    if (mm !== undefined && (!Number.isInteger(mm) || mm <= 0)) {
      setMsg({ ok: false, text: t("balanceInvalidMaxMoves") });
      return;
    }
    setBusy(true);
    try {
      const r = await api.balanceEval(mm);
      setMsg(
        r.ok
          ? { ok: true, text: t("balanceEvalOk", { id: r.plan?.plan_id ?? "—", n: r.plan?.total ?? 0 }) }
          : { ok: false, text: r.error },
      );
      await refresh();
    } catch (e) {
      setMsg({ ok: false, text: String(e) });
    } finally {
      setBusy(false);
    }
  };

  const start = async (plan: BalancePlan) => {
    if (!confirm(t("balanceStartConfirm", { id: plan.plan_id }))) return;
    setBusy(true);
    try {
      const r = await api.balanceStart(plan.plan_id);
      setMsg(
        r.ok
          ? { ok: true, text: t("balanceStarted", { id: plan.plan_id }) }
          : { ok: false, text: r.error },
      );
      await refresh();
    } catch (e) {
      setMsg({ ok: false, text: String(e) });
    } finally {
      setBusy(false);
    }
  };

  const cancel = async (plan: BalancePlan) => {
    if (!confirm(t("balanceCancelConfirm", { id: plan.plan_id }))) return;
    setBusy(true);
    try {
      const r = await api.balanceCancel(plan.plan_id);
      setMsg(
        r.ok
          ? { ok: true, text: t("balanceCancelled", { id: plan.plan_id }) }
          : { ok: false, text: r.error },
      );
      await refresh();
    } catch (e) {
      setMsg({ ok: false, text: String(e) });
    } finally {
      setBusy(false);
    }
  };

  const plan = state?.present ? state.plan : null;
  const pct = plan && plan.total > 0 ? Math.round((plan.done / plan.total) * 100) : 0;

  return (
    <section>
      {msg && <div className={`msg ${msg.ok ? "ok" : "err"}`}>{msg.text}</div>}
      {loadErr && <div className="msg err">{t("balanceLoadError", { err: loadErr })}</div>}

      <div className="toolbar">
        {isOperator && (
          <>
            <label className="check">
              {t("balanceMaxMoves")}
              <input
                type="number"
                min={1}
                value={maxMoves}
                onChange={(e) => setMaxMoves(e.target.value)}
                placeholder={t("autoPick")}
              />
            </label>
            <button className="btn" onClick={evalPlan} disabled={busy}>
              {t("balanceEval")}
            </button>
          </>
        )}
        {isOperator && plan && plan.status === "PREVIEW" && (
          <button className="btn" onClick={() => start(plan)} disabled={busy}>
            {t("balanceStart")}
          </button>
        )}
        {isOperator && plan && (plan.status === "PREVIEW" || plan.status === "RUNNING") && (
          <button className="btn danger" onClick={() => cancel(plan)} disabled={busy}>
            {t("balanceCancelPlan")}
          </button>
        )}
        <span className="spacer" />
        <button className="btn ghost" onClick={refresh} disabled={busy}>
          {t("refresh")}
        </button>
      </div>

      {!plan && (
        <div className="panel">
          <p className="empty">{t("balanceEmpty")}</p>
        </div>
      )}

      {plan && (
        <>
          <div className="panel">
            <div className="panel-head">
              <h3>
                {t("balancePlanId")} #{plan.plan_id}
              </h3>
              <span className={`pill ${planPillClass(plan.status)}`}>
                {t(`balanceStatus_${plan.status}`)}
              </span>
            </div>
            <div className="disk-grid" style={{ marginTop: 10 }}>
              <div className="disk-cell">
                {t("balanceCreatedAt")}: {new Date(plan.created_at_ms).toLocaleString()}
              </div>
              <div className="disk-cell">
                {t("balanceMaxInFlight")}: {plan.max_in_flight}
              </div>
              <div className="disk-cell">
                {t("balanceTotal")}: {plan.total} · {t("balancePending")}: {plan.pending} ·{" "}
                {t("balanceInFlight")}: {plan.in_flight} · {t("balanceDone")}: {plan.done} ·{" "}
                {t("balanceFailed")}: {plan.failed}
              </div>
            </div>
            <div className="card-sub" style={{ marginTop: 10 }}>
              {t("balanceProgress")}: {plan.done} / {plan.total} ({pct}%)
            </div>
            <div className="prog">
              <div style={{ width: `${pct}%` }} />
            </div>
          </div>

          <h2>{t("balanceMoves")}</h2>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>{t("balanceColPartition")}</th>
                  <th>{t("balanceColFrom")}</th>
                  <th>{t("balanceColTo")}</th>
                  <th>{t("status")}</th>
                  <th>{t("balanceColError")}</th>
                </tr>
              </thead>
              <tbody>
                {plan.moves.map((m, i) => (
                  <tr key={`${m.partition_id}-${m.from_node_id}-${i}`}>
                    <td>{m.partition_id}</td>
                    <td className="mono">
                      n{m.from_node_id} · {m.from_disk_id}
                    </td>
                    <td className="mono">
                      n{m.to_node_id} · {m.to_disk_id}
                    </td>
                    <td>
                      <span className={`pill small ${movePillClass(m.status)}`}>
                        {t(`moveStatus_${m.status}`)}
                      </span>
                    </td>
                    <td className="wrap">{m.error || "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </section>
  );
}
