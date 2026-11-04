# E-Commerce Customer Purchase Pattern Analysis
Parallel & Distributed Systems mini project (MapReduce-style, Python `multiprocessing`)

## 1. Problem
Analyse customer transactions to find top-selling products, purchase patterns, high-value
transactions and customer statistics - on a dataset too large to process efficiently serially.

## 2. Dataset
**Recommended real dataset:** UCI *Online Retail* (UK online retailer, 541,909 rows, 2010-2011)
https://archive.ics.uci.edu/dataset/352/online-retail (also on Kaggle as "Online Retail").
Columns: `InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country`.

Convert the Excel file to CSV once, then run the pipeline on it (no code change needed; it handles
the `12/1/2010 8:26` date format, `17850.0`-style customer IDs, cancellations `C...` and
non-product codes such as POST/M/D):

```python
import pandas as pd
pd.read_excel("Online Retail.xlsx").to_csv("data/online_retail.csv", index=False)
```

For development, `generate_dataset.py` creates a **synthetic dataset with the identical schema**
(465k rows, Zipf popularity, product themes, wholesalers, cancellations, guest checkouts,
seasonality). Results in `output/` come from this synthetic data.

## 3. Design (MapReduce on a multi-process "cluster")
```
            +-- chunk 1 --> worker --+
CSV --split-+-- chunk 2 --> worker --+--> REDUCE (merge) --> baskets --split--> workers --> REDUCE --> ANALYSE
 (line-     +-- ...      --> worker --+   products/invoices                   (co-purchase
 aligned)   +-- chunk 4N --> worker --+                                         pair counts)
```
- **Partition:** the file is cut into `4 x workers` byte ranges aligned to line boundaries
  (over-partitioning gives dynamic load balancing).
- **Map 1:** each worker parses its range and emits partial aggregates: per-product
  (units, revenue), per-invoice (total, customer, date, country) and baskets.
  Invoices that straddle a chunk boundary are combined in the reduce step.
- **Reduce 1:** the coordinator merges partial dictionaries (associative sums).
- **Map 2 / Reduce 2:** baskets are re-partitioned; workers count co-purchased product pairs;
  counters are merged.
- **Analyse:** top products, time patterns, high-value invoices (P99 + top 25), customer
  statistics and RFM segmentation.

To go from one machine to a real cluster, replace `multiprocessing.Pool` with
Spark/Dask/MPI - the map and reduce functions stay the same.

## 4. Run
```bash
python generate_dataset.py 60000 data/online_retail.csv    # skip if using the real dataset
python analyze.py --input data/online_retail.csv --workers 4 --outdir output
python analyze.py --input data/online_retail.csv --benchmark   # speedup table + chart
```
Requires Python 3.8+; `matplotlib` only for the charts.

## 5. Outputs (`output/`)
| File | Content |
|---|---|
| `report.md` | Human-readable report of all results |
| `results.json` | All results in structured form |
| `top_products.csv` | Top 100 products by units and revenue |
| `high_value_transactions.csv` | Top 25 invoices |
| `customer_stats.csv` | Per-customer orders, spend, recency, RFM segment |
| `copurchase_pairs.csv` | Frequently co-purchased product pairs |
| `overview.png`, `speedup.png` | Charts |
| `benchmark.csv` | Workers vs time/speedup/efficiency |

## 6. Validation
Total revenue, invoice count, customer count, top products and the largest invoice were
cross-checked against an independent pandas computation: all identical.

## 7. Performance (465,674 rows, 2-core sandbox)
| Workers | Time | Speedup |
|---|---|---|
| 1 | 5.75 s | 1.00x |
| 2 | 4.24 s | 1.36x |
| 4 | 4.72 s | 1.22x |

The test machine has only 2 cores, so more than 2 workers cannot help. Speedup is also limited by
the serial parts (reduce, analysis - Amdahl's law) and by inter-process pickling overhead of
partial results. Re-run `--benchmark` on a machine with more cores and a larger dataset
(`generate_dataset.py 600000`) to see better scaling.

## 8. Possible extensions
Spark/PySpark version, Apriori/FP-Growth association rules, customer clustering (k-means on RFM),
a dashboard, streaming ingestion.

## 9. Demo
```bash
python demo.py --workers 4 --pause     # step-by-step live demo (ENTER between stages)
# then open output/dashboard.html in a browser (needs internet for Chart.js CDN)
```
The demo prints each worker's PID and the chunks it handled, the merge step, the results, and a
serial-vs-parallel timing, then regenerates the dashboard.
