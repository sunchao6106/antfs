import { useEffect, useRef } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

const HEIGHT = 220;

export interface ChartProps {
  title: string;
  /** y 序列名(与 data[1..] 对应)。 */
  labels: string[];
  colors: string[];
  data: uPlot.AlignedData;
  /** y 轴 / 图例数值格式化。 */
  fmt?: (v: number) => string;
}

/** uPlot 薄封装:实例随组件创建/销毁,数据更新走 setData,宽度随容器自适应。 */
export function Chart({ title, labels, colors, data, fmt }: ChartProps) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const plotRef = useRef<uPlot | null>(null);
  const dataRef = useRef(data);
  dataRef.current = data;
  const fmtRef = useRef(fmt);
  fmtRef.current = fmt;

  const seriesKey = labels.join("|");

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const opts: uPlot.Options = {
      title,
      width: Math.max(el.clientWidth, 320),
      height: HEIGHT,
      legend: { show: true },
      scales: { x: { time: true } },
      axes: [
        { stroke: "#8b949e", grid: { stroke: "#2d333b" }, ticks: { stroke: "#2d333b" } },
        {
          stroke: "#8b949e",
          grid: { stroke: "#2d333b" },
          ticks: { stroke: "#2d333b" },
          values: (_u, splits) =>
            splits.map((v) => (v == null ? "" : fmtRef.current ? fmtRef.current(v) : String(v))),
        },
      ],
      series: [
        {},
        ...labels.map((label, i) => ({
          label,
          stroke: colors[i % colors.length],
          width: 1.5,
          spanGaps: false,
          value: (_u: uPlot, v: number | null) =>
            v == null ? "—" : fmtRef.current ? fmtRef.current(v) : String(v),
        })),
      ],
    };
    const u = new uPlot(opts, dataRef.current, el);
    plotRef.current = u;
    const ro = new ResizeObserver(() => {
      u.setSize({ width: Math.max(el.clientWidth, 320), height: HEIGHT });
    });
    ro.observe(el);
    return () => {
      ro.disconnect();
      u.destroy();
      plotRef.current = null;
    };
    // 序列集合(seriesKey)变化时重建实例;数据本身走下面的 setData。
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [seriesKey, title]);

  useEffect(() => {
    plotRef.current?.setData(data);
  }, [data, seriesKey]);

  return <div className="chart-card" ref={wrapRef} />;
}
