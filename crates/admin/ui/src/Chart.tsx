import { useEffect, useRef } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

const HEIGHT = 220;

/** 图表配色(series 按序取色)。 */
export const PALETTE = [
  "#2f81f7",
  "#2ea043",
  "#d29922",
  "#f85149",
  "#a371f7",
  "#39c5cf",
  "#f778ba",
  "#e3b341",
];

export interface ChartProps {
  title: string;
  /** 标题右侧的补充读数(如"最近 1 分钟 xx/min"),可省。 */
  subtitle?: string;
  /** y 序列名(与 data[1..] 对应)。 */
  labels: string[];
  colors: string[];
  data: uPlot.AlignedData;
  /** y 轴 / 图例数值格式化。 */
  fmt?: (v: number) => string;
}

/** uPlot 薄封装:实例随组件创建/销毁,数据更新走 setData,宽度随容器自适应。 */
export function Chart({ title, subtitle, labels, colors, data, fmt }: ChartProps) {
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
      scales: {
        x: {
          time: true,
          // 首屏只有 1~2 个样本时 x 跨度可能不足 1 秒:uPlot 会切到亚秒格式(标签又长又难读),
          // 单点时区间还会退化成 [t, t] 撑出无意义的刻度。这里统一给一个最小 1 分钟窗口,
          // 最新点贴右缘;数据跨度超过 1 分钟后按真实数据走。
          range: (_u, min, max) => {
            if (min == null || max == null) return [0, 1];
            const MIN_SPAN_SECS = 60;
            if (max - min >= MIN_SPAN_SECS) return [min, max];
            return [max - MIN_SPAN_SECS, max];
          },
        },
      },
      axes: [
        { stroke: "#8b949e", grid: { stroke: "#2d333b" }, ticks: { stroke: "#2d333b" } },
        {
          stroke: "#8b949e",
          grid: { stroke: "#2d333b" },
          ticks: { stroke: "#2d333b" },
          // 自定义 `values` 后 uPlot 不再按默认标签预留宽度,必须给足,否则长标签(如 "768.0 MiB")被裁到只剩 ".7 MiB"。
          size: 96,
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

  return (
    <div className="chart-card">
      {subtitle && <div className="chart-sub">{subtitle}</div>}
      {/* uPlot 自己往宿主里 append DOM,故宿主只交给它,不放 React 子节点。 */}
      <div ref={wrapRef} />
    </div>
  );
}
