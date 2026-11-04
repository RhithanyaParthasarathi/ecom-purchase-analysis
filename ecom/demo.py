"""Live demo: shows the parallel MapReduce pipeline step by step, then builds dashboard.html.

    python demo.py                       # uses data/online_retail.csv, 4 workers
    python demo.py --workers 2 --pause   # --pause waits for ENTER between stages (good for presenting)
"""
import argparse
import csv
import json
import os
import time
from collections import Counter
from multiprocessing import Pool, cpu_count

import analyze as A


def timed_map(args):
    """Wrapper so we can show which worker process handled which chunk."""
    t = time.perf_counter()
    part = A.map_chunk(args)
    return os.getpid(), args[1], args[2], part, time.perf_counter() - t


def banner(txt):
    print("\n" + "=" * 72 + f"\n {txt}\n" + "=" * 72)


def pause(on):
    if on:
        input("   [press ENTER to continue] ")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/online_retail.csv")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--outdir", default="output")
    ap.add_argument("--pause", action="store_true")
    a = ap.parse_args()
    W = a.workers

    banner("STEP 0  Dataset")
    size = os.path.getsize(a.input)
    with open(a.input) as f:
        head = [next(f).strip() for _ in range(4)]
    print(f" file: {a.input}  ({size/1e6:.1f} MB)")
    for h in head:
        print("   ", h)
    print(f" machine cores: {cpu_count()}   worker processes: {W}")
    pause(a.pause)

    banner("STEP 1  Partition the file into line-aligned chunks")
    chunks = A.make_chunks(a.input, W * 4)
    for i, c in enumerate(chunks):
        print(f"   chunk {i:2d}: bytes {c[1]:>10,} - {c[2]:>10,}  ({(c[2]-c[1])/1e6:.2f} MB)")
    pause(a.pause)

    banner(f"STEP 2  MAP: {W} workers process chunks in parallel")
    t0 = time.perf_counter()
    parts, per_worker = [], Counter()
    with Pool(W) as pool:
        for pid, s, e, part, dt in pool.imap_unordered(timed_map, chunks):
            parts.append(part)
            per_worker[pid] += part["rows"]
            print(f"   worker PID {pid}: finished bytes {s:>10,}-{e:<10,} "
                  f"{part['rows']:>7,} rows in {dt:.2f}s")
    t_map = time.perf_counter() - t0
    print(f"\n rows handled per worker process: " + ", ".join(f"{p}: {n:,}" for p, n in per_worker.items()))
    print(f" MAP wall-clock: {t_map:.2f}s")
    pause(a.pause)

    banner("STEP 3  REDUCE: merge partial results from all workers")
    t = time.perf_counter()
    products, invoices, baskets, rows, bad = A.reduce_stage1(parts)
    print(f"   merged {rows:,} rows -> {len(products)} products, {len(invoices):,} invoices "
          f"({time.perf_counter()-t:.2f}s)")
    pause(a.pause)

    banner("STEP 4  MAP+REDUCE #2: co-purchase pairs (second parallel stage)")
    t = time.perf_counter()
    bl = [sorted(set(b)) for b in baskets.values() if 2 <= len(set(b)) <= 30]
    slices = [bl[i::W * 4] for i in range(W * 4)]
    with Pool(W) as pool:
        pp = pool.map(A.map_pairs, slices, chunksize=1)
    pairs = Counter()
    for p in pp:
        pairs.update(p)
    print(f"   {len(bl):,} baskets -> {len(pairs):,} distinct pairs ({time.perf_counter()-t:.2f}s)")
    pause(a.pause)

    banner("STEP 5  RESULTS")
    res = A.analyse(products, invoices, baskets, pairs, rows, bad)
    s = res["summary"]
    print(f" revenue {s['total_revenue']:,.2f} | invoices {s['valid_invoices']:,} | "
          f"products {s['distinct_products']} | cancellation rate {s['cancellation_rate']*100:.1f}%")
    print("\n TOP 5 PRODUCTS (units)")
    for r in res["top_products_by_units"][:5]:
        print(f"   {r['description']:<28} {r['units_sold']:>9,} units   {r['revenue']:>12,.2f}")
    print("\n TOP 5 HIGH-VALUE TRANSACTIONS")
    for r in res["high_value"]["top_25"][:5]:
        print(f"   invoice {r['invoice']}  {r['total']:>10,.2f}  customer {r['customer']:<6} {r['country']}")
    cs = res["customer_stats"]
    print(f"\n CUSTOMERS: {cs['unique_customers']:,} | repeat rate {cs['repeat_customer_rate']*100:.1f}% | "
          f"avg spend {cs['avg_spend_per_customer']:,.2f}")
    print(" RFM segments:", {k: v["customers"] for k, v in cs["segments"].items()})
    pause(a.pause)

    banner("STEP 6  Serial (1 process) vs parallel comparison")
    r1 = A.run_pipeline(a.input, 1)["timings"]["total"]
    rN = A.run_pipeline(a.input, W)["timings"]["total"]
    print(f"   1 worker : {r1:.2f}s\n   {W} workers: {rN:.2f}s\n   speedup  : {r1/rN:.2f}x "
          f"(machine has {cpu_count()} cores)")

    # write outputs + dashboard
    res["timings"] = {"total": rN}
    res["workers"] = W
    A.write_outputs(res, a.outdir)
    bench = []
    bp = os.path.join(a.outdir, "benchmark.csv")
    if os.path.exists(bp):
        bench = list(csv.DictReader(open(bp)))
    build_dashboard(os.path.join(a.outdir, "results.json"), bench, os.path.join(a.outdir, "dashboard.html"))
    print(f"\n Dashboard written: {os.path.join(a.outdir, 'dashboard.html')}  (open in a browser)")


def build_dashboard(results_json, bench, out):
    data = json.load(open(results_json))
    data["bench"] = bench
    html = DASH.replace("/*DATA*/", json.dumps(data, default=str))
    open(out, "w", encoding="utf-8").write(html)


DASH = r"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>E-Commerce Purchase Pattern Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
body{font-family:system-ui,sans-serif;margin:0;background:#f4f6fa;color:#1c2333}
header{background:#1f3a68;color:#fff;padding:18px 28px}header h1{margin:0;font-size:22px}header p{margin:4px 0 0;opacity:.8;font-size:13px}
main{padding:20px 28px;max-width:1300px;margin:auto}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:18px}
.kpi{background:#fff;border-radius:10px;padding:14px;box-shadow:0 1px 3px #0001}.kpi b{display:block;font-size:22px}.kpi span{font-size:12px;color:#667}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:16px}
.card{background:#fff;border-radius:10px;padding:14px;box-shadow:0 1px 3px #0001}.card h3{margin:0 0 8px;font-size:15px}
table{border-collapse:collapse;width:100%;font-size:12.5px}th,td{padding:5px 6px;border-bottom:1px solid #eee;text-align:left}th{color:#667}td.n{text-align:right}
</style></head><body>
<header><h1>E-Commerce Customer Purchase Pattern Analysis</h1><p>Parallel MapReduce pipeline &middot; results dashboard</p></header>
<main><div class="kpis" id="kpis"></div><div class="grid" id="grid"></div></main>
<script>
const D=/*DATA*/;
const f=n=>Number(n).toLocaleString(undefined,{maximumFractionDigits:0}),f2=n=>Number(n).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
const s=D.summary,c=D.customer_stats,p=D.patterns,h=D.high_value;
[["Total revenue",f(s.total_revenue)],["Valid invoices",f(s.valid_invoices)],["Rows processed",f(s.rows_processed)],["Customers",f(c.unique_customers)],
 ["Repeat-customer rate",(c.repeat_customer_rate*100).toFixed(1)+"%"],["Avg order value",f2(p.avg_order_value)],["Cancellation rate",(s.cancellation_rate*100).toFixed(1)+"%"],
 ["Top 1% invoices share",(h.share_of_revenue_from_top1pct_invoices*100).toFixed(1)+"% of revenue"]]
 .forEach(([k,v])=>kpis.insertAdjacentHTML("beforeend",`<div class="kpi"><b>${v}</b><span>${k}</span></div>`));
function chart(title,type,labels,data,opts={}){const id="c"+Math.random().toString(36).slice(2);
 grid.insertAdjacentHTML("beforeend",`<div class="card"><h3>${title}</h3><canvas id="${id}"></canvas></div>`);
 new Chart(document.getElementById(id),{type,data:{labels,datasets:[{data,backgroundColor:opts.color||"#3b6fb6",borderColor:opts.color||"#3b6fb6",tension:.3}]},
 options:{indexAxis:opts.h?"y":"x",plugins:{legend:{display:false}}}});}
function table(title,heads,rows){grid.insertAdjacentHTML("beforeend",`<div class="card"><h3>${title}</h3><table><tr>${heads.map(x=>`<th>${x}</th>`).join("")}</tr>${rows.map(r=>`<tr>${r.map((x,i)=>`<td class="${i>0&&/^[\d,.]+$/.test(x)?"n":""}">${x}</td>`).join("")}</tr>`).join("")}</table></div>`);}
const tp=D.top_products_by_units.slice(0,10);
chart("Top 10 products by units sold","bar",tp.map(r=>r.description),tp.map(r=>r.units_sold),{h:true});
const tr=D.top_products_by_revenue.slice(0,10);
chart("Top 10 products by revenue","bar",tr.map(r=>r.description),tr.map(r=>r.revenue),{h:true,color:"#2a9d5c"});
chart("Orders by hour of day","bar",Object.keys(p.orders_by_hour),Object.values(p.orders_by_hour),{color:"#e08a1e"});
chart("Orders by weekday","bar",Object.keys(p.orders_by_weekday),Object.values(p.orders_by_weekday),{color:"#8a5fbf"});
chart("Revenue by month","line",Object.keys(p.revenue_by_month),Object.values(p.revenue_by_month),{color:"#d1495b"});
chart("Customers per RFM segment","bar",Object.keys(c.segments),Object.values(c.segments).map(v=>v.customers),{color:"#00798c"});
table("Top co-purchased product pairs",["Product A","Product B","Baskets"],p.top_copurchased_pairs.slice(0,8).map(r=>[r.desc_a,r.desc_b,f(r.baskets_together)]));
table("High-value transactions (top 10)",["Invoice","Total","Customer","Country"],h.top_25.slice(0,10).map(r=>[r.invoice,f2(r.total),r.customer,r.country]));
table("Top 10 customers",["Customer","Orders","Spend","Segment"],c.top_10_customers.map(r=>[r.customer,r.orders,f2(r.total_spend),r.segment]));
if(D.bench&&D.bench.length){chart("Speedup vs worker processes","line",D.bench.map(b=>b.workers+" worker(s)"),D.bench.map(b=>+b.speedup),{color:"#1f3a68"});}
</script></body></html>
"""

if __name__ == "__main__":
    main()
