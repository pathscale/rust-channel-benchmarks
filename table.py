#!/usr/bin/env python3
"""Generate human-readable text tables from benchmark CSV files."""

import sys
import glob as _glob
from pathlib import Path


def read_throughput_data(files):
    """Read throughput benchmark data from CSV files."""
    benchs = {}
    names = []
    for f in files:
        if "_latency" in f:
            continue
        with open(f) as fp:
            lines = fp.readlines()
            if len(lines) < 2:
                continue
            name = lines[0].strip("\n ")
            names.append(name)
            for line in lines[1:]:
                if line.startswith("---") or not line.strip():
                    break
                parts = line.strip("\n ").split(",")
                if len(parts) < 3:
                    continue
                test, nsecs, ops = parts
                splt = test.split('_')
                test_cat = splt[0]  # bounded1, unbounded, etc.
                test_name = '_'.join(splt[1:])  # spsc(empty), mpsc(usize), etc.
                if test_cat not in benchs:
                    benchs[test_cat] = {}
                if test_name not in benchs[test_cat]:
                    benchs[test_cat][test_name] = {}
                benchs[test_cat][test_name][name] = float(ops)
    return benchs, names


def read_latency_data(files):
    """Read latency benchmark data from CSV files."""
    benchs = {}
    names = []
    for f in files:
        if "_latency" not in f:
            continue
        with open(f) as fp:
            lines = fp.readlines()
            if len(lines) < 2:
                continue
            name = lines[0].strip("\n ")
            names.append(name)
            for line in lines[1:]:
                parts = line.strip("\n ").split(",")
                if len(parts) < 7:
                    continue
                test = parts[0]
                min_ns, p50_ns, p90_ns, p99_ns, p999_ns, max_ns = [float(x) for x in parts[1:7]]

                if '(' in test:
                    before_paren = test.split('(')[0]
                    payload = test.split('(')[1].rstrip(')')
                    pattern = before_paren.split('_')[-1]
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


def format_ops(ops):
    """Format operations per second in human-readable form."""
    if ops >= 1e9:
        return f"{ops/1e9:.2f}G"
    elif ops >= 1e6:
        return f"{ops/1e6:.2f}M"
    elif ops >= 1e3:
        return f"{ops/1e3:.2f}K"
    else:
        return f"{ops:.0f}"


def format_latency(ns):
    """Format latency in human-readable form."""
    if ns >= 1e6:
        return f"{ns/1e6:.2f}ms"
    elif ns >= 1e3:
        return f"{ns/1e3:.2f}µs"
    else:
        return f"{ns:.0f}ns"


def print_throughput_table(benchs, names, category="bounded1", top_n=None):
    """Print throughput results as a text table."""
    if category not in benchs:
        return

    bench = benchs[category]

    # Get all test names and sort them
    labels = ["seq", "spsc", "mpsc", "mpmc"]
    variants = ["empty", "usize", "big"]

    def sort_key(test_name):
        if '(' in test_name:
            label = test_name.split('(')[0]
            variant = test_name.split('(')[1].rstrip(')')
            try:
                return labels.index(label) * 10 + variants.index(variant)
            except ValueError:
                return 999
        return 999

    test_names = sorted(bench.keys(), key=sort_key)

    # Calculate column widths
    name_width = max(len(n) for n in names) + 2
    test_width = max(len(t) for t in test_names) + 2
    value_width = 10  # "XX.XXM/s"

    # Print header
    header = f"{'Test':<{test_width}}"
    for name in names:
        header += f" {name:<{name_width}}"
    print("=" * len(header))
    print(f"THROUGHPUT BENCHMARK - {category} (ops/sec, higher is better)")
    print("=" * len(header))
    print(header)
    print("-" * len(header))

    # Print rows
    for test in test_names:
        row = f"{test:<{test_width}}"
        values = []
        for name in names:
            if name in bench[test]:
                val = format_ops(bench[test][name])
                values.append(bench[test][name])
            else:
                val = "N/A"
            row += f" {val:<{name_width}}"
        print(row)

    print()


def print_latency_table(benchs, names, percentile="p50", top_n=None):
    """Print latency results as a text table."""
    patterns = ["spsc", "mpsc", "mpmc"]
    payloads = ["empty", "usize", "big"]

    # Filter names that have latency data
    latency_names = []
    for name in names:
        for pattern in patterns:
            if pattern in benchs:
                for payload in payloads:
                    if payload in benchs[pattern] and name in benchs[pattern][payload]:
                        if name not in latency_names:
                            latency_names.append(name)
                        break

    if not latency_names:
        return

    # Calculate column widths
    name_width = max(len(n) for n in latency_names) + 2
    test_width = 16  # "spsc(empty)"
    value_width = 12  # "XX.XXµs"

    # Print header
    header = f"{'Test':<{test_width}}"
    for name in latency_names:
        header += f" {name:<{name_width}}"
    print("=" * len(header))
    print(f"LATENCY BENCHMARK - {percentile} (lower is better)")
    print("=" * len(header))
    print(header)
    print("-" * len(header))

    # Print rows
    for pattern in patterns:
        for payload in payloads:
            test = f"{pattern}({payload})"
            row = f"{test:<{test_width}}"
            for name in latency_names:
                if pattern in benchs and payload in benchs[pattern] and name in benchs[pattern][payload]:
                    val = format_latency(benchs[pattern][payload][name][percentile])
                else:
                    val = "N/A"
                row += f" {val:<{name_width}}"
            print(row)

    print()


def print_summary_comparison(benchs, names, category="bounded1"):
    """Print a summary comparison showing best performer for each test."""
    if category not in benchs:
        return

    bench = benchs[category]

    print("=" * 60)
    print(f"SUMMARY - Best performers for {category}")
    print("=" * 60)

    for test in sorted(bench.keys()):
        best_name = None
        best_ops = 0
        for name in bench[test]:
            if bench[test][name] > best_ops:
                best_ops = bench[test][name]
                best_name = name

        if best_name:
            # Find second best for comparison
            sorted_results = sorted(
                [(n, bench[test][n]) for n in bench[test]],
                key=lambda x: x[1],
                reverse=True
            )
            if len(sorted_results) > 1:
                second_name, second_ops = sorted_results[1]
                diff_pct = ((best_ops - second_ops) / second_ops) * 100
                print(f"{test}: {best_name} ({format_ops(best_ops)}) - {diff_pct:.1f}% faster than {second_name}")
            else:
                print(f"{test}: {best_name} ({format_ops(best_ops)})")

    print()


def main():
    files = [f for pattern in sys.argv[1:] for f in (_glob.glob(pattern) or [pattern])]
    if not files:
        files = list(Path("target").glob("*.csv"))

    if not files:
        print("No CSV files found. Run benchmarks first.")
        sys.exit(1)

    benchs, names = read_throughput_data(files)
    latency_benchs, latency_names = read_latency_data(files)

    output_path = Path("target/results.txt")

    with open(output_path, 'w') as f:
        # Redirect stdout to file temporarily
        import io
        import contextlib

        with contextlib.redirect_stdout(f):
            # Print throughput tables
            for category in benchs:
                print_throughput_table(benchs, names, category)

            # Print latency tables for each percentile
            for percentile in ['p50', 'p99', 'max']:
                print_latency_table(latency_benchs, names, percentile)

            # Print summary comparison
            for category in benchs:
                print_summary_comparison(benchs, names, category)

    print(f"Results saved to {output_path}")


if __name__ == '__main__':
    main()