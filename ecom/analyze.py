"""Parallel E-Commerce Customer Purchase Pattern Analysis (MapReduce style).

Architecture
------------
  Stage 0  PARTITION : the CSV is split into N byte-range chunks aligned to line boundaries.
  Stage 1  MAP       : each worker process parses its chunk and builds partial aggregates
                       (per-product, per-invoice, per-customer, baskets).
           REDUCE    : the coordinator merges partial aggregates (sums / dict merges).
  Stage 2  MAP       : baskets are re-partitioned; workers count co-purchased product pairs.
           REDUCE    : coordinator merges pair counters.
  Stage 3  ANALYSE   : top products, time patterns, high-value invoices, customer stats + RFM.

Usage
-----
  python analyze.py --input data/online_retail.csv --workers 4 --outdir output
  python analyze.py --input data/online_retail.csv --benchmark        # speedup table + chart
"""
import argparse
import csv
import json
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from itertools import combinations
from multiprocessing import Pool, cpu_count

DATE_FORMATS = ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M")


def parse_dt(s):
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    return None


def is_valid_transaction(invoice_no: str, stock_code: str, quantity: int, unit_price: float) -> bool:
    """Check whether a transaction row represents a valid order record."""
    cancelled = invoice_no.startswith("C")
    if not cancelled and (quantity <= 0 or unit_price <= 0 or stock_code.isalpha()):
        return False
    return True


# --------------------------------------------------------------------------- partitioning
def make_chunks(path, n):
    size = os.path.getsize(path)
    bounds = [0]
    with open(path, "rb") as f:
        for i in range(1, n):
            f.seek(size * i // n)
            f.readline()                       # advance to next line boundary
            bounds.append(f.tell())
    bounds.append(size)
    return [(path, bounds[i], bounds[i + 1], i == 0) for i in range(n) if bounds[i] < bounds[i + 1]]


# --------------------------------------------------------------------------- stage 1: MAP
def map_chunk(args):
    path, start, end, has_header = args
    products = {}               # code -> [qty, revenue, n_lines, description]
    invoices = {}               # inv  -> [total, cust, iso_date, country, n_lines, cancelled]
    baskets = defaultdict(list)
    bad = rows = 0
    with open(path, "rb") as f:
        f.seek(start)
        text = f.read(end - start).decode("utf-8", errors="replace")
    lines = text.splitlines()
    if has_header and lines:
        lines = lines[1:]
    for row in csv.reader(lines):
        rows += 1
        if len(row) < 8:
            bad += 1
            continue
        inv, code, desc, q, dts, pr, cust, ctry = row[:8]
        try:
            q = int(float(q))
            pr = float(pr)
        except ValueError:
            bad += 1
            continue
        dt = parse_dt(dts)
        if dt is None:
            bad += 1
            continue
        cust = cust.split(".")[0].strip()
        cancelled = inv.startswith("C")
        if not is_valid_transaction(inv, code, q, pr):
            continue                           # adjustments, postage, fees, etc.
        value = abs(q) * pr
        e = invoices.get(inv)
        if e is None:
            invoices[inv] = [value, cust, dt.isoformat(), ctry, 1, cancelled]
        else:
            e[0] += value
            e[4] += 1
        if cancelled:
            continue
        p = products.get(code)
        if p is None:
            products[code] = [q, value, 1, desc]
        else:
            p[0] += q
            p[1] += value
            p[2] += 1
        baskets[inv].append(code)
    return {"products": products, "invoices": invoices, "baskets": dict(baskets),
            "rows": rows, "bad": bad}


def reduce_stage1(parts):
    products, invoices, baskets = {}, {}, {}
    rows = bad = 0
    for part in parts:
        rows += part["rows"]
        bad += part["bad"]
        for code, v in part["products"].items():
            p = products.get(code)
            if p is None:
                products[code] = list(v)
            else:
                p[0] += v[0]; p[1] += v[1]; p[2] += v[2]
        for inv, v in part["invoices"].items():
            e = invoices.get(inv)
            if e is None:
                invoices[inv] = list(v)
            else:
                e[0] += v[0]; e[4] += v[4]
        for inv, items in part["baskets"].items():
            baskets.setdefault(inv, []).extend(items)
    return products, invoices, baskets, rows, bad


# --------------------------------------------------------------------------- stage 2: MAP
def map_pairs(baskets):
    c = Counter()
    for b in baskets:
        c.update(combinations(b, 2))
    return c


# --------------------------------------------------------------------------- pipeline
def run_pipeline(path, workers, outdir=None):
    timings = {}
    t0 = time.perf_counter()
    n_chunks = 1 if workers == 1 else workers * 4        # over-partition for load balance
    chunks = make_chunks(path, n_chunks)
    timings["partition"] = time.perf_counter() - t0

    t = time.perf_counter()
    if workers == 1:
        parts = [map_chunk(c) for c in chunks]
    else:
        with Pool(workers) as pool:
            parts = pool.map(map_chunk, chunks, chunksize=1)
    timings["map1"] = time.perf_counter() - t

    t = time.perf_counter()
    products, invoices, baskets, rows, bad = reduce_stage1(parts)
    timings["reduce1"] = time.perf_counter() - t

    t = time.perf_counter()
    bl = [sorted(set(b)) for b in baskets.values() if 2 <= len(set(b)) <= 30]
    k = 1 if workers == 1 else workers * 4
    slices = [bl[i::k] for i in range(k)]
    if workers == 1:
        pair_parts = [map_pairs(s) for s in slices]
    else:
        with Pool(workers) as pool:
            pair_parts = pool.map(map_pairs, slices, chunksize=1)
    pairs = Counter()
    for pp in pair_parts:
        pairs.update(pp)
    timings["pairs"] = time.perf_counter() - t

    t = time.perf_counter()
    result = analyse(products, invoices, baskets, pairs, rows, bad)
    timings["analyse"] = time.perf_counter() - t
    timings["total"] = time.perf_counter() - t0
    result["timings"] = timings
    result["workers"] = workers
    return result


# --------------------------------------------------------------------------- stage 3: analysis
def analyse(products, invoices, baskets, pairs, rows, bad):
    sales = {i: v for i, v in invoices.items() if not v[5]}
    cancels = {i: v for i, v in invoices.items() if v[5]}
    totals = sorted(v[0] for v in sales.values())
    n_inv = len(totals)
    revenue = sum(totals)

    # --- top products
    prod_rows = [{"stock_code": c, "description": v[3], "units_sold": v[0],
                  "revenue": round(v[1], 2), "times_ordered": v[2]} for c, v in products.items()]
    top_qty = sorted(prod_rows, key=lambda r: -r["units_sold"])[:15]
    top_rev = sorted(prod_rows, key=lambda r: -r["revenue"])[:15]

    # --- purchase patterns (invoice-level time analysis)
    by_hour, by_wd, by_month = Counter(), Counter(), Counter()
    rev_hour, rev_wd, rev_month = Counter(), Counter(), Counter()
    by_country = Counter()
    for v in sales.values():
        dt = datetime.fromisoformat(v[2])
        by_hour[dt.hour] += 1; rev_hour[dt.hour] += v[0]
        by_wd[dt.strftime("%a")] += 1; rev_wd[dt.strftime("%a")] += v[0]
        m = dt.strftime("%Y-%m")
        by_month[m] += 1; rev_month[m] += v[0]
        by_country[v[3]] += v[0]
    wd_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    patterns = {
        "orders_by_hour": {h: by_hour[h] for h in sorted(by_hour)},
        "revenue_by_hour": {h: round(rev_hour[h], 2) for h in sorted(rev_hour)},
        "orders_by_weekday": {d: by_wd[d] for d in wd_order if d in by_wd},
        "revenue_by_weekday": {d: round(rev_wd[d], 2) for d in wd_order if d in rev_wd},
        "orders_by_month": dict(sorted(by_month.items())),
        "revenue_by_month": {m: round(r, 2) for m, r in sorted(rev_month.items())},
        "revenue_by_country_top10": [(c, round(r, 2)) for c, r in by_country.most_common(10)],
        "avg_items_per_order": round(sum(v[4] for v in sales.values()) / n_inv, 2),
        "avg_order_value": round(revenue / n_inv, 2),
        "median_order_value": round(totals[n_inv // 2], 2),
    }
    desc = {c: v[3] for c, v in products.items()}
    patterns["top_copurchased_pairs"] = [
        {"product_a": a, "desc_a": desc.get(a, ""), "product_b": b, "desc_b": desc.get(b, ""),
         "baskets_together": n} for (a, b), n in pairs.most_common(15)]

    # --- high-value transactions
    p99 = totals[int(0.99 * n_inv)]
    hv_all = sorted(((i, v) for i, v in sales.items()), key=lambda x: -x[1][0])
    hv = [{"invoice": i, "total": round(v[0], 2), "customer": v[1] or "GUEST",
           "date": v[2], "country": v[3], "line_items": v[4]} for i, v in hv_all[:25]]
    top1_rev = sum(v[0] for v in sales.values() if v[0] >= p99)
    high_value = {"p99_threshold": round(p99, 2), "n_invoices_above_p99": sum(1 for t in totals if t >= p99),
                  "share_of_revenue_from_top1pct_invoices": round(top1_rev / revenue, 4),
                  "top_25": hv}

    # --- customer statistics + RFM
    cust = {}
    max_dt = max(datetime.fromisoformat(v[2]) for v in sales.values()) + timedelta(days=1)
    for i, v in sales.items():
        if not v[1]:
            continue
        c = cust.setdefault(v[1], {"orders": 0, "spend": 0.0, "last": datetime.min, "country": v[3]})
        c["orders"] += 1
        c["spend"] += v[0]
        d = datetime.fromisoformat(v[2])
        if d > c["last"]:
            c["last"] = d
    n_c = len(cust)
    for c in cust.values():
        c["recency"] = (max_dt - c["last"]).days

    def score(key, reverse=False):                     # rank-based quartile score 1..4
        order = sorted(cust, key=lambda k: cust[k][key], reverse=reverse)
        return {k: min(4, 1 + 4 * r // n_c) for r, k in enumerate(order)}
    R = score("recency", reverse=True)                 # recent = high score
    F, M = score("orders"), score("spend")
    seg_counts, seg_rev = Counter(), Counter()
    cust_rows = []
    for k, c in cust.items():
        r, f, m = R[k], F[k], M[k]
        if r >= 3 and f >= 3 and m >= 3:   seg = "Champions"
        elif r >= 3 and (f >= 3 or m >= 3): seg = "Loyal"
        elif r <= 2 and (f >= 3 or m >= 3): seg = "At Risk"
        elif r == 1 and f == 1:             seg = "Lost"
        else:                               seg = "Regular"
        seg_counts[seg] += 1
        seg_rev[seg] += c["spend"]
        cust_rows.append({"customer": k, "orders": c["orders"], "total_spend": round(c["spend"], 2),
                          "avg_order_value": round(c["spend"] / c["orders"], 2),
                          "recency_days": c["recency"], "country": c["country"],
                          "R": r, "F": f, "M": m, "segment": seg})
    cust_rows.sort(key=lambda r: -r["total_spend"])
    spend_sorted = sorted((c["spend"] for c in cust.values()), reverse=True)
    top10pct = sum(spend_sorted[: max(1, n_c // 10)])
    guest_rev = sum(v[0] for v in sales.values() if not v[1])
    customer_stats = {
        "unique_customers": n_c,
        "repeat_customer_rate": round(sum(1 for c in cust.values() if c["orders"] > 1) / n_c, 4),
        "avg_orders_per_customer": round(sum(c["orders"] for c in cust.values()) / n_c, 2),
        "avg_spend_per_customer": round(sum(spend_sorted) / n_c, 2),
        "top10pct_customers_revenue_share": round(top10pct / sum(spend_sorted), 4),
        "guest_checkout_revenue_share": round(guest_rev / revenue, 4),
        "segments": {s: {"customers": seg_counts[s], "revenue": round(seg_rev[s], 2)} for s in seg_counts},
        "top_10_customers": cust_rows[:10],
    }
    summary = {"rows_processed": rows, "malformed_rows": bad, "valid_invoices": n_inv,
               "cancelled_invoices": len(cancels),
               "cancellation_rate": round(len(cancels) / (n_inv + len(cancels)), 4),
               "distinct_products": len(products), "total_revenue": round(revenue, 2)}
    return {"summary": summary, "top_products_by_units": top_qty, "top_products_by_revenue": top_rev,
            "patterns": patterns, "high_value": high_value, "customer_stats": customer_stats,
            "_customer_rows": cust_rows, "_prod_rows": sorted(prod_rows, key=lambda r: -r["units_sold"])}


# --------------------------------------------------------------------------- output
def write_outputs(res, outdir):
    os.makedirs(outdir, exist_ok=True)

    def dump_csv(name, rows):
        if not rows:
            return
        with open(os.path.join(outdir, name), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader(); w.writerows(rows)
    dump_csv("top_products.csv", res.pop("_prod_rows")[:100])
    dump_csv("customer_stats.csv", res.pop("_customer_rows"))
    dump_csv("high_value_transactions.csv", res["high_value"]["top_25"])
    dump_csv("copurchase_pairs.csv", res["patterns"]["top_copurchased_pairs"])
    with open(os.path.join(outdir, "results.json"), "w") as f:
        json.dump(res, f, indent=2, default=str)

    s, p, h, c = res["summary"], res["patterns"], res["high_value"], res["customer_stats"]
    L = ["# E-Commerce Purchase Pattern Analysis - Report", "",
         f"Processed with **{res['workers']} worker process(es)** in {res['timings']['total']:.2f}s.", "",
         "## Dataset summary", ""]
    L += [f"- {k.replace('_', ' ')}: {v:,}" if isinstance(v, int) else f"- {k.replace('_', ' ')}: {v}"
          for k, v in s.items()]
    L += ["", "## Top 10 products by units sold", "", "| # | Code | Description | Units | Revenue |", "|---|---|---|---|---|"]
    for i, r in enumerate(res["top_products_by_units"][:10], 1):
        L.append(f"| {i} | {r['stock_code']} | {r['description']} | {r['units_sold']:,} | {r['revenue']:,.2f} |")
    L += ["", "## Top 10 products by revenue", "", "| # | Code | Description | Revenue | Units |", "|---|---|---|---|---|"]
    for i, r in enumerate(res["top_products_by_revenue"][:10], 1):
        L.append(f"| {i} | {r['stock_code']} | {r['description']} | {r['revenue']:,.2f} | {r['units_sold']:,} |")
    peak_h = max(p["orders_by_hour"], key=p["orders_by_hour"].get)
    peak_d = max(p["orders_by_weekday"], key=p["orders_by_weekday"].get)
    peak_m = max(p["revenue_by_month"], key=p["revenue_by_month"].get)
    L += ["", "## Purchase patterns", "",
          f"- Peak hour: **{peak_h}:00** ({p['orders_by_hour'][peak_h]:,} orders)",
          f"- Busiest weekday: **{peak_d}**",
          f"- Highest-revenue month: **{peak_m}** ({p['revenue_by_month'][peak_m]:,.2f})",
          f"- Average order value: {p['avg_order_value']:,.2f} (median {p['median_order_value']:,.2f})",
          f"- Average items per order: {p['avg_items_per_order']}",
          "", "Top co-purchased product pairs:", "", "| Product A | Product B | Baskets |", "|---|---|---|"]
    for r in p["top_copurchased_pairs"][:10]:
        L.append(f"| {r['desc_a']} | {r['desc_b']} | {r['baskets_together']} |")
    L += ["", "## High-value transactions", "",
          f"- 99th-percentile invoice value: {h['p99_threshold']:,.2f}",
          f"- Top 1% of invoices generate **{h['share_of_revenue_from_top1pct_invoices']*100:.1f}%** of revenue", "",
          "| Invoice | Total | Customer | Date | Country |", "|---|---|---|---|---|"]
    for r in h["top_25"][:10]:
        L.append(f"| {r['invoice']} | {r['total']:,.2f} | {r['customer']} | {r['date']} | {r['country']} |")
    L += ["", "## Customer statistics", "",
          f"- Unique identified customers: {c['unique_customers']:,}",
          f"- Repeat-customer rate: {c['repeat_customer_rate']*100:.1f}%",
          f"- Avg orders / customer: {c['avg_orders_per_customer']}",
          f"- Avg spend / customer: {c['avg_spend_per_customer']:,.2f}",
          f"- Top 10% of customers account for {c['top10pct_customers_revenue_share']*100:.1f}% of identified revenue",
          f"- Guest checkouts: {c['guest_checkout_revenue_share']*100:.1f}% of total revenue", "",
          "| RFM segment | Customers | Revenue |", "|---|---|---|"]
    for seg, v in sorted(c["segments"].items(), key=lambda x: -x[1]["revenue"]):
        L.append(f"| {seg} | {v['customers']:,} | {v['revenue']:,.2f} |")
    with open(os.path.join(outdir, "report.md"), "w") as f:
        f.write("\n".join(L) + "\n")
    make_charts(res, outdir)


def make_charts(res, outdir):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed - skipping charts")
        return
    p = res["patterns"]
    fig, ax = plt.subplots(2, 2, figsize=(13, 9))
    top = res["top_products_by_units"][:10][::-1]
    ax[0, 0].barh([r["description"][:28] for r in top], [r["units_sold"] for r in top], color="#3b6fb6")
    ax[0, 0].set_title("Top 10 products by units sold")
    ax[0, 1].bar([str(h) for h in p["orders_by_hour"]], list(p["orders_by_hour"].values()), color="#e08a1e")
    ax[0, 1].set_title("Orders by hour of day")
    ax[1, 0].plot(list(p["revenue_by_month"]), list(p["revenue_by_month"].values()), marker="o", color="#2a9d5c")
    ax[1, 0].set_title("Revenue by month"); ax[1, 0].tick_params(axis="x", rotation=60)
    seg = res["customer_stats"]["segments"]
    ax[1, 1].bar(list(seg), [v["customers"] for v in seg.values()], color="#8a5fbf")
    ax[1, 1].set_title("Customers per RFM segment")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "overview.png"), dpi=120)
    plt.close(fig)


def benchmark(path, outdir, max_workers):
    ws = sorted({1, 2, 4, max_workers} | ({8} if max_workers >= 8 else set()))
    ws = [w for w in ws if w <= max(max_workers, 1)]
    rows, base = [], None
    for w in ws:
        best = min((run_pipeline(path, w) for _ in range(2)), key=lambda r: r["timings"]["total"])
        t = best["timings"]["total"]
        base = base or t
        rows.append((w, t, base / t, base / t / w))
        print(f"workers={w:<2} time={t:6.2f}s speedup={base/t:5.2f}x efficiency={base/t/w*100:5.1f}%")
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "benchmark.csv"), "w") as f:
        f.write("workers,seconds,speedup,efficiency\n")
        for r in rows:
            f.write(f"{r[0]},{r[1]:.3f},{r[2]:.3f},{r[3]:.3f}\n")
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot([r[0] for r in rows], [r[2] for r in rows], marker="o", label="measured")
        ax.plot([r[0] for r in rows], [r[0] for r in rows], "--", color="gray", label="ideal")
        ax.set_xlabel("worker processes"); ax.set_ylabel("speedup"); ax.legend(); ax.set_title("Strong scaling")
        fig.tight_layout(); fig.savefig(os.path.join(outdir, "speedup.png"), dpi=120)
    except ImportError:
        pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/online_retail.csv")
    ap.add_argument("--workers", type=int, default=cpu_count())
    ap.add_argument("--outdir", default="output")
    ap.add_argument("--benchmark", action="store_true")
    a = ap.parse_args()
    res = run_pipeline(a.input, a.workers)
    write_outputs(res, a.outdir)
    t = res["timings"]
    print(f"done: {res['summary']['rows_processed']:,} rows, {a.workers} workers, "
          + ", ".join(f"{k}={v:.2f}s" for k, v in t.items()))
    if a.benchmark:
        benchmark(a.input, a.outdir, a.workers)
