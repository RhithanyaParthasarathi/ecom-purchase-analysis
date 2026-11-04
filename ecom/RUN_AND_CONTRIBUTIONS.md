# How to run

```bash
# 1. unzip and enter the folder
unzip ecom_purchase_analysis.zip && cd ecom

# 2. (optional) install extras for charts
pip install -r requirements.txt

# 3. get data  -> either synthetic:
python generate_dataset.py 60000 data/online_retail.csv
#    or the real UCI Online Retail dataset converted to CSV (see README section 2)

# 4. run the analysis (parallel, 4 worker processes)
python analyze.py --input data/online_retail.csv --workers 4 --outdir output

# 5. speedup benchmark (1,2,4.. workers) -> output/benchmark.csv, speedup.png
python analyze.py --input data/online_retail.csv --workers 4 --benchmark

# 6. live demo for presentation + dashboard
python demo.py --workers 4 --pause
#    then open output/dashboard.html in a browser
```
Windows: use `python` instead of `python3`; run from a terminal/PowerShell (multiprocessing needs the
`if __name__ == "__main__"` guard, which the scripts already have).

# Team contribution (4 members)

| Member | Module | Files / functions | Responsibilities |
|---|---|---|---|
| 1 | Data & partitioning | `generate_dataset.py`, `parse_dt`, `make_chunks` | Dataset selection (UCI Online Retail), schema, synthetic data generator, cleaning rules (cancellations, non-product codes), line-aligned file partitioning |
| 2 | Parallel processing core | `map_chunk`, `reduce_stage1`, `run_pipeline`, `Pool` usage | Map/reduce design, worker processes, over-partitioning for load balance, merging partial aggregates across chunk boundaries |
| 3 | Analytics | `map_pairs`, `analyse` | Top-selling products, purchase patterns (hour/weekday/month/country), co-purchase pairs, high-value transactions (P99, top 25), customer stats and RFM segmentation |
| 4 | Output, evaluation & docs | `write_outputs`, `make_charts`, `benchmark`, `demo.py`, dashboard, `README.md` | Reports/CSV/JSON/charts, speedup & efficiency benchmark, correctness validation against pandas, live demo, dashboard, documentation and presentation |

Everyone: review each other's modules, test on the full dataset, and prepare the viva answers
for their own part.
