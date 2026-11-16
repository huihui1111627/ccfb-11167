//! Self-contained SVG charts — no JavaScript or external assets, so the report
//! renders anywhere a browser is available and is safe to archive.

use std::collections::BTreeMap;

use crate::verifier::PerfStats;

struct Plot {
    width: u32,
    height: u32,
    pad: u32,
}

impl Plot {
    fn axes(&self, title: &str) -> String {
        let p = self.pad;
        let top = p + 10;
        let axis = self.height - p;
        let right = self.width - p;
        let font = "sans-serif";
        format!(
            r##"<text x="{p}" y="20" font-family="{font}" font-size="14" font-weight="bold" fill="#dbe4f3">{title}</text>
<line x1="{p}" y1="{top}" x2="{p}" y2="{axis}" stroke="#33415c"/>
<line x1="{p}" y1="{axis}" x2="{right}" y2="{axis}" stroke="#33415c"/>"##
        )
    }

    fn line(
        &self,
        points: &[(f64, f64)],
        color: &str,
        label: &str,
        max_x: f64,
        max_y: f64,
    ) -> String {
        let p = self.pad as f64;
        let w = self.width as f64;
        let h = self.height as f64;
        let plot_w = w - 2.0 * p;
        let plot_h = h - 2.0 * p - 14.0;
        if points.is_empty() || max_y <= 0.0 {
            return String::new();
        }
        let path: String = points
            .iter()
            .map(|(x, y)| {
                let px = p + (x / max_x.max(1.0)) * plot_w;
                let py = h - p - (y / max_y) * plot_h;
                format!("{px:.1},{py:.1}")
            })
            .collect::<Vec<_>>()
            .join(" ");
        let xlabel = self.width - self.pad - 120;
        let ylabel = self.pad + 6;
        let font = "sans-serif";
        format!(
            r##"<polyline fill="none" stroke="{color}" stroke-width="2" points="{path}"/>
<text x="{xlabel}" y="{ylabel}" font-family="{font}" font-size="11" fill="{color}">{label}</text>"##
        )
    }
}

fn series_from(map: &BTreeMap<u64, u64>) -> (Vec<(f64, f64)>, f64, f64) {
    if map.is_empty() {
        return (Vec::new(), 0.0, 0.0);
    }
    let t0 = *map.keys().next().unwrap() as f64;
    let max_x = (*map.keys().last().unwrap() as f64 - t0).max(1.0);
    let max_y = *map.values().max().unwrap() as f64;
    let pts = map
        .iter()
        .map(|(t, v)| ((*t as f64 - t0), *v as f64))
        .collect();
    (pts, max_x, max_y)
}

pub fn throughput_svg(stats: &PerfStats) -> String {
    let plot = Plot {
        width: 900,
        height: 320,
        pad: 48,
    };
    let (prod, px, py) = series_from(&stats.produce_throughput);
    let (cons, cx, cy) = series_from(&stats.consume_throughput);
    let max_x = px.max(cx);
    let max_y = py.max(cy).max(1.0);
    let w = plot.width;
    let h = plot.height;
    let p = plot.pad;
    let hy = plot.height - plot.pad + 20;
    let rx = plot.width - plot.pad;
    let font = "sans-serif";
    format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}">
<rect width="100%" height="100%" fill="#0f1420"/>
{axes}
{prod_line}
{cons_line}
<text x="{p}" y="{hy}" font-family="{font}" font-size="11" fill="#8494ad">条/秒（峰值 {peak:.0}）</text>
<text x="{rx}" y="{hy}" text-anchor="end" font-family="{font}" font-size="11" fill="#8494ad">运行秒数</text>
</svg>"##,
        axes = plot.axes("生产 / 消费吞吐"),
        prod_line = plot.line(&prod, "#4f8cff", "生产", max_x, max_y),
        cons_line = plot.line(&cons, "#37c997", "消费", max_x, max_y),
        peak = max_y
    )
}

/// A bucketed end-to-end latency distribution (histogram) rather than raw
/// per-event points, which would be unreadable at benchmark scale.
pub fn latency_svg(stats: &PerfStats) -> String {
    let width = 900u32;
    let height = 320u32;
    let pad = 48u32;
    let bars = [
        ("min", stats.latency_ms_min, "#4f8cff"),
        ("p50", stats.latency_ms_p50, "#37c997"),
        ("p95", stats.latency_ms_p95, "#f0b34f"),
        ("p99", stats.latency_ms_p99, "#ef8f6a"),
        ("max", stats.latency_ms_max, "#ef6a6a"),
    ];
    let max_v = bars.iter().map(|(_, v, _)| *v).max().unwrap_or(1).max(1);
    let plot_h = (height - 2 * pad - 20) as f64;
    let slot = (width - 2 * pad) / bars.len() as u32;
    let bars_svg: String = bars
        .iter()
        .enumerate()
        .map(|(i, (name, v, color))| {
            let bh = (*v as f64 / max_v as f64) * plot_h;
            let x = pad + i as u32 * slot + 16;
            let y = height - pad - bh as u32;
            let bw = slot - 32;
            let cx = x + bw / 2;
            let ly = y.saturating_sub(6);
            let by = height - pad + 18;
            let font = "sans-serif";
            format!(
                r##"<rect x="{x}" y="{y}" width="{bw}" height="{bh}" rx="4" fill="{color}"/>
<text x="{cx}" y="{ly}" text-anchor="middle" font-family="{font}" font-size="11" fill="#dbe4f3">{v} ms</text>
<text x="{cx}" y="{by}" text-anchor="middle" font-family="{font}" font-size="11" fill="#8494ad">{name}</text>"##
            )
        })
        .collect();
    let top = pad + 10;
    let axis = height - pad;
    let right = width - pad;
    let events = stats.events;
    let font = "sans-serif";
    format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="#0f1420"/>
<text x="{pad}" y="20" font-family="{font}" font-size="14" font-weight="bold" fill="#dbe4f3">端到端延迟分布（{events} 个样本）</text>
<line x1="{pad}" y1="{top}" x2="{pad}" y2="{axis}" stroke="#33415c"/>
<line x1="{pad}" y1="{axis}" x2="{right}" y2="{axis}" stroke="#33415c"/>
{bars_svg}
</svg>"##
    )
}

pub fn wrap_html(body: &str, title: &str) -> String {
    // CSS braces are literal in the output, so they must be doubled for
    // format!; the document itself contains no dynamic values except title/body.
    format!(
        r##"<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">
<title>{title}</title>
<style>
body {{ background:#0f1420; color:#dbe4f3; font-family:-apple-system,"PingFang SC",sans-serif; margin:24px; }}
svg {{ margin-bottom:24px; border:1px solid #243047; border-radius:10px; }}
table {{ border-collapse:collapse; font-size:13px; }}
td,th {{ border:1px solid #243047; padding:6px 12px; }}
th {{ color:#8494ad; }}
.ok {{ color:#37c997; font-weight:bold; }}
.bad {{ color:#ef6a6a; font-weight:bold; }}
</style>
</head><body><h1>{title}</h1>{body}</body></html>"##
    )
}
