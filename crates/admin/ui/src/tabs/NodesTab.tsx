import { useMemo, useState } from "react";
import { fmtBytes, Node } from "../api";
import type { T } from "../App";

function worstHealth(n: Node): string | null {
  const bad = n.disks.map((d) => d.health).filter((h) => h && h !== "HEALTHY");
  return bad.length > 0 ? bad[0] : null;
}

function usedFrac(n: Node): number {
  const cap = n.disks.reduce((s, d) => s + d.capacity_bytes, 0);
  const free = n.disks.reduce((s, d) => s + d.free_bytes, 0);
  return cap > 0 ? 1 - free / cap : 0;
}

export function NodesTab({ t, nodes }: { t: T; nodes: Node[] }) {
  const [query, setQuery] = useState("");
  const [offlineOnly, setOfflineOnly] = useState(false);
  const [expanded, setExpanded] = useState<Set<number>>(new Set());

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
          </tr>
        </thead>
        <tbody>
          {filtered.map((n) => {
            const frac = usedFrac(n);
            const pct = Math.round(frac * 100);
            const worst = worstHealth(n);
            const open = expanded.has(n.node_id);
            return [
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
              </tr>,
              open && (
                <tr key={`${n.node_id}-detail`} className="detail">
                  <td colSpan={5}>
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
                </tr>
              ),
            ];
          })}
          {filtered.length === 0 && (
            <tr>
              <td colSpan={5} className="empty">
                {nodes.length === 0 ? t("noNodes") : t("noMatch")}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </section>
  );
}
