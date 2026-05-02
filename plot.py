#!/usr/bin/env python3
import sys
import glob as _glob
import pygal
from PIL import Image


def read_data(files):
    benchs = {}
    names = ["crossfire", "crossfire-async", "tokio-oneshot", "bounded-spsc-queue", "multiqueue2"]
    for f in files:
        if "_latency" in f:
            continue
        with open(f) as fp:
            lines = fp.readlines()
            if len(lines) < 2:
                continue
            name = lines[0].strip("\n ")
            if name not in names:
                names.append(name)
            for line in lines[1:]:
                if line.startswith("---"):
                    break
                parts = line.strip("\n ").split(",")
                if len(parts) < 3:
                    continue
                test, nsecs, ops = parts
                splt = test.split('_')
                test_cat = splt[0]
                test_name = '_'.join(splt[1:])
                if test_cat not in benchs:
                    benchs[test_cat] = {}
                if test_name not in benchs[test_cat]:
                    benchs[test_cat][test_name] = {}
                benchs[test_cat][test_name][name] = float(ops)
    return benchs, names


def read_latency_data(files):
    benchs = {}
    names = ["crossfire", "crossfire-async", "tokio-oneshot", "bounded-spsc-queue", "multiqueue2"]
    for f in files:
        if "_latency" not in f:
            continue
        with open(f) as fp:
            lines = fp.readlines()
            if len(lines) < 2:
                continue
            name = lines[0].strip("\n ")
            if name not in names:
                names.append(name)
            for line in lines[1:]:
                parts = line.strip("\n ").split(",")
                if len(parts) < 7:
                    continue
                test = parts[0]
                min_ns, p50_ns, p90_ns, p99_ns, p999_ns, max_ns = [float(x) for x in parts[1:7]]
                # Parse: bounded1_spsc(empty) -> pattern=spsc, payload=empty
                # Split by '(' to get pattern part and payload
                if '(' in test:
                    before_paren = test.split('(')[0]  # bounded1_spsc
                    payload = test.split('(')[1].rstrip(')')  # empty
                    # Get pattern from last underscore-separated part
                    pattern = before_paren.split('_')[-1]  # spsc
                else:
                    pattern = test
                    payload = ""
                if pattern not in benchs:
                    benchs[pattern] = {}
                if payload not in benchs[pattern]:
                    benchs[pattern][payload] = {}
                benchs[pattern][payload][name] = {
                    'min': min_ns, 'p50': p50_ns, 'p90': p90_ns,
                    'p99': p99_ns, 'p999': p999_ns, 'max': max_ns
                }
    return benchs, names


labels = ["seq", "spsc", "mpsc", "mpmc"]
variants = ["empty", "usize", "big"]


def sortFn(key):
    label, variant = key.split("(")
    variant = variant[:-1]
    v = labels.index(label)*10+variants.index(variant)*1
    return v


color_set = {
    'indigo':    "#6366f1",
    'violet':    "#a78bfa",
    'teal':      "#2dd4bf",
    'sky':       "#38bdf8",
    'rose':      "#fb7185",
    'amber':     "#fbbf24",
    'emerald':   "#34d399",
    'fuchsia':   "#e879f9",
}

colors = {
    "crossfire":          "indigo",
    "crossfire-async":    "teal",
    "tokio-oneshot":      "rose",
    "bounded-spsc-queue": "amber",
    "multiqueue2":        "fuchsia",
}


def get_color(name):
    if name in colors:
        return color_set[colors[name]]
    used_colors = set(colors.values())
    for k in color_set:
        if k not in used_colors:
            colors[name] = k
            return color_set[k]
    return "#888888"


def make_rows(bench_name, bench, names):
    bench_keys = list(bench.keys())
    bench_keys.sort(key=sortFn)
    x_labels = bench_keys

    rows = []
    for name in names:
        row = []
        for label in x_labels:
            if name in bench[label]:
                row.append(bench[label][name])
            else:
                row.append(None)
        rows.append((name, row))
    return rows, x_labels


def set_zero_none_fields(rows):
    for idx, (name, row) in enumerate(rows):
        rows[idx] = (name, [i if i is not None else 0 for i in row])


def concat_vertical(imgs, to):
    height_sum = 0
    for img in imgs:
        height_sum += img.height+5
    output = Image.new('RGB', (imgs[0].width, height_sum))
    last_height = 0
    for img in imgs:
        output.paste(img, (0, last_height))
        last_height += img.height+5
    output.save(to)


def get_range_excluding_outliers(rows, outlier_threshold=2.0):
    """Calculate y-axis max from non-outlier values.
    Outliers are values > outlier_threshold * p90 (90th percentile).
    """
    all_values = []
    for (name, row) in rows:
        all_values.extend([v for v in row if v is not None and v > 0])

    if not all_values:
        return rows, None

    sorted_vals = sorted(all_values)
    p90_idx = int(len(sorted_vals) * 0.90)
    p90 = sorted_vals[p90_idx]

    # Get max from non-outlier values only
    normal_values = [v for v in all_values if v <= p90 * outlier_threshold]
    if normal_values:
        max_y = max(normal_values) * 1.1  # Add 10% padding
    else:
        max_y = p90 * outlier_threshold

    return rows, max_y


def chart(benchs, names):
    for bench_name in benchs:
        bench = benchs[bench_name]
        rows, x_labels = make_rows(bench_name, bench, names)
        bar_colors = []
        for (name, _) in rows:
            bar_colors.append(get_color(name))

        rows, max_y = get_range_excluding_outliers(rows)

        custom_style = pygal.style.Style(
            colors=bar_colors, value_font_size=9,
            value_colors=bar_colors,
            legend_font_size=10,
            title_font_size=12,
        )
        chart = pygal.Bar(
            margin=5,
            width=1200,
            height=350,
            legend_at_bottom=True,
            print_values=True,
            print_values_position='top',
            value_formatter=lambda x: '{}M'.format(
                round(x/1e6, 1) if x > 1e5 else round(x/1e6, 2)
            ) if x > 0 else "N/A",
            style=custom_style,
            range=(0, max_y) if max_y else None,
        )
        chart.title = f"{bench_name} Bounded(1) Channel Benchmark\n(Operations per second, Higher is better)"
        chart.x_labels = x_labels
        set_zero_none_fields(rows)
        for (name, row) in rows:
            chart.add(name, row)
        chart.render_to_png(f"target/plot_{bench_name}.png")
        chart.render_to_file(f"target/plot_{bench_name}.svg")


def chart_latency(benchs, names):
    latency_patterns = ["spsc", "mpsc", "mpmc"]
    latency_payloads = ["empty", "usize", "big"]

    # Only use channels that have latency data
    latency_names = []
    for name in names:
        for pattern in benchs:
            for payload in benchs[pattern]:
                if name in benchs[pattern][payload]:
                    if name not in latency_names:
                        latency_names.append(name)
                    break

    for percentile in ['p50', 'p99', 'max']:
        rows = []
        x_labels = []
        for pattern in latency_patterns:
            for payload in latency_payloads:
                x_labels.append(f"{pattern}({payload})")

        for name in latency_names:
            row = []
            for pattern in latency_patterns:
                for payload in latency_payloads:
                    if pattern in benchs and payload in benchs[pattern] and name in benchs[pattern][payload]:
                        row.append(benchs[pattern][payload][name][percentile])
                    else:
                        row.append(None)
            rows.append((name, row))

        # Calculate reasonable y-axis max excluding outliers
        _, max_y = get_range_excluding_outliers(rows)

        bar_colors = []
        for (name, _) in rows:
            bar_colors.append(get_color(name))
        custom_style = pygal.style.Style(
            colors=bar_colors, value_font_size=9,
            value_colors=bar_colors,
            legend_font_size=10,
            title_font_size=12,
        )
        chart = pygal.Bar(
            margin=5,
            width=1200,
            height=350,
            legend_at_bottom=True,
            print_values=True,
            print_values_position='top',
            value_formatter=lambda x: '{}µs'.format(round(x/1000, 1)) if x > 1000 else '{}ns'.format(round(x, 0)) if x > 0 else "N/A",
            style=custom_style,
            range=(0, max_y) if max_y else None,
        )
        chart.title = f"Bounded(1) Channel Latency Benchmark - {percentile}\n(Lower is better)"
        chart.x_labels = x_labels
        set_zero_none_fields(rows)
        for (name, row) in rows:
            chart.add(name, row)
        chart.render_to_png(f"target/plot_latency_{percentile}.png")
        chart.render_to_file(f"target/plot_latency_{percentile}.svg")


def main():
    files = [f for pattern in sys.argv[1:] for f in (_glob.glob(pattern) or [pattern])]
    benchs, names = read_data(files)
    latency_benchs, latency_names = read_latency_data(files)
    chart(benchs, names)
    chart_latency(latency_benchs, names)

    imgs = []
    for bench_name in ["bounded1"]:
        imgs.append(Image.open(f"target/plot_{bench_name}.png"))
    for percentile in ['p50', 'p99', 'max']:
        imgs.append(Image.open(f"target/plot_latency_{percentile}.png"))
    concat_vertical(imgs, "target/results.png")


if __name__ == '__main__':
    main()