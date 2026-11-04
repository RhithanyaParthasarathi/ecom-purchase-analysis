# E-Commerce Customer Purchase Pattern Analysis
## Parallel vs Sequential Execution Demonstration & Faculty Viva Guide

---

## 4-Member Team Role & Topic Allocation

| Member | Topic & Demonstration Sections | Core Files / Modules |
| :--- | :--- | :--- |
| **Member 1** | **1. Problem & Objective**<br>**2. Dataset / Input**<br>**3. Architecture / Workflow** | `generate_dataset.py`, `data/online_retail.csv`, System Architecture |
| **Member 2** | **4. Methodology / Algorithm**<br>**5. Implementation / Code Walkthrough** | `analyze.py` (`make_chunks`, `map_chunk`, `reduce_stage1`, `multiprocessing.Pool`) |
| **Member 3** | **6. Live Execution & Worker Demo**<br>**7. Output / Results & Dashboard** | `demo.py`, `output/dashboard.html`, `output/report.md`, `output/*.csv` |
| **Member 4** | **8. Performance Analysis (Serial vs Parallel)**<br>**9. Conclusion**<br>**10. Individual Contribution & 11. Viva Defense** | `analyze.py` (`--benchmark`), `output/benchmark.csv`, `output/speedup.png` |

---

## Member 1: Problem, Dataset & Architecture

### Section 1: Problem & Objective
- **The Core Problem:**
  Modern e-commerce platforms process millions of transactions daily. Computing aggregates—such as top-selling stock items, customer lifetime value, RFM customer segmentation, and market-basket co-purchase pairs—using single-threaded sequential scripts quickly becomes a performance bottleneck ($O(N)$ scanning + $O(B \cdot L^2)$ pair combinations).
- **The Objective:**
  Design, implement, and benchmark a distributed **MapReduce-style multi-process processing engine** in Python using native `multiprocessing` to:
  1. Ingest and parse massive CSV transaction logs concurrently.
  2. Perform 2-stage distributed map-reduce for row aggregation and market basket analysis.
  3. Compare the wall-clock execution time, speedup factor ($S$), and parallel efficiency ($E$) against a purely sequential baseline on the exact same dataset.

### Section 2: Dataset / Input
- **Dataset Specification:**
  - Standard UCI Machine Learning Repository *Online Retail* schema.
  - Size: **465,674 rows (~36.8 MB CSV)**.
  - Attributes:
    1. `InvoiceNo`: 6-digit transaction identifier (prefixed with `'C'` if cancelled).
    2. `StockCode`: Unique product SKU.
    3. `Description`: Product name.
    4. `Quantity`: Integer count of items bought (negative for returns/cancellations).
    5. `InvoiceDate`: Timestamp of transaction.
    6. `UnitPrice`: Price per unit in GBP sterling.
    7. `CustomerID`: 5-digit customer ID (blank for guest checkouts).
    8. `Country`: Origin country of purchaser.
- **Data Cleaning Rules Handled in Parallel:**
  - Filter out invalid entries, postage/bank charges (`code.isalpha()`), zero/negative prices.
  - Separate cancellations ($C$-prefix) to compute cancellation rates without polluting revenue totals.
  - Account for guest checkouts without losing total transaction value.

### Section 3: Architecture / Workflow
```
                              [ CSV Dataset: 36.8 MB, 465k rows ]
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      ▼                                               ▼
            [ Sequential Mode (1 Worker) ]                [ Parallel Mode (N Workers) ]
                      │                                               │
             Single Process parses                     Line-aligned Byte Chunking (N x 4 chunks)
             entire file linearly                                     │
                      │                                ┌──────────────┼──────────────┐
                      ▼                                ▼              ▼              ▼
             Serial Map & Aggregate                Worker 1       Worker 2       Worker N
                      │                            (Map 1)        (Map 1)        (Map 1)
                      ▼                                └──────────────┬──────────────┘
             Serial Basket Pairs                                      │
                      │                                    REDUCE 1: Associative Merge
                      ▼                                               │
             Final Serial Report                           Basket Redistribution
                                                                      │
                                                       ┌──────────────┼──────────────┐
                                                       ▼              ▼              ▼
                                                   Worker 1       Worker 2       Worker N
                                                   (Map 2)        (Map 2)        (Map 2)
                                                   Co-purchase    Co-purchase    Co-purchase
                                                       └──────────────┬──────────────┘
                                                                      │
                                                           REDUCE 2: Global Counter Merge
                                                                      │
                                                           ANALYSE & Reporting Engine
```

---

## Member 2: Methodology & Implementation Code

### Section 4: Methodology / Algorithm
1. **Line-Aligned Binary Chunking (Zero-Copy Partitioning):**
   - Naive byte splitting breaks CSV rows across chunk boundaries.
   - We calculate equidistant byte offsets (`size * i // n`), seek to that byte, and call `.readline()` to snap to the exact newline boundary `\n`.
   - Over-partitioning strategy: We generate $K = 4 \times W$ chunks (e.g., 16 chunks for 4 workers) to ensure dynamic load balancing. Faster workers grab remaining chunks via `multiprocessing.Pool`.
2. **Two-Stage MapReduce Pipeline:**
   - **Stage 1 (Row Mapping & Partial Aggregation):**
     Each worker extracts product totals (`[qty, revenue, orders]`), invoice totals (`[total, customer, date, country]`), and basket itemsets.
   - **Stage 1 (Coordinator Reduction):**
     Coordinator combines partial dictionaries using associative addition.
   - **Stage 2 (Market Basket Co-Purchase Mining):**
     Extracts combinations of pairs $\binom{k}{2}$ from transactions with $2 \le k \le 30$ items. Baskets are interleaved (`slices = bl[i::K]`) and dispatched to worker pools to count pairs in parallel.

### Section 5: Implementation / Code Walkthrough

#### 1. Binary Chunking (`make_chunks`):
```python
def make_chunks(path, n):
    size = os.path.getsize(path)
    bounds = [0]
    with open(path, "rb") as f:
        for i in range(1, n):
            f.seek(size * i // n)
            f.readline()  # advance to next complete newline
            bounds.append(f.tell())
    bounds.append(size)
    return [(path, bounds[i], bounds[i + 1], i == 0) for i in range(n) if bounds[i] < bounds[i + 1]]
```

#### 2. Worker Map Stage (`map_chunk`):
```python
def map_chunk(args):
    path, start, end, has_header = args
    with open(path, "rb") as f:
        f.seek(start)
        text = f.read(end - start).decode("utf-8", errors="replace")
    lines = text.splitlines()
    if has_header and lines:
        lines = lines[1:]
    for row in csv.reader(lines):
        # Parses records, filters bad rows, updates local products, invoices, and baskets
        ...
    return {"products": products, "invoices": invoices, "baskets": dict(baskets), "rows": rows, "bad": bad}
```

#### 3. Execution Coordinator (`run_pipeline`):
```python
if workers == 1:
    parts = [map_chunk(c) for c in chunks]  # Sequential execution
else:
    with Pool(workers) as pool:             # Parallel execution
        parts = pool.map(map_chunk, chunks, chunksize=1)
```

---

## Member 3: Live Execution & Results

### Section 6: Live Execution
Run the live terminal demo:
```powershell
python demo.py --workers 4
```
**Key points to highlight during live execution:**
1. **Dataset Ingestion:** 465,674 rows (~36.8 MB).
2. **Chunk Partitioning:** 16 line-aligned chunks (~2.3 MB each).
3. **Worker PIDs:** Show different Process IDs (e.g., PIDs `18140`, `31148`, `3204`, `18676`) processing chunks independently and concurrently.
4. **Load Balancing:** Each worker handles approximately equal portions (~116,400 rows each).

### Section 7: Output / Results
Show the generated files in `output/` and open `output/dashboard.html`:
- **Financial & Order KPIs:**
  - Total Revenue: **£8,691,325.12** across **60,000 valid invoices**.
  - Distinct Products: **600 products**.
  - Cancellation Rate: **1.7%**.
- **Top Product (Units):** `RED STORAGE TIN 15` (139,360 units, £125,424.00).
- **High-Value Pareto Analysis:** Top 1% of invoices account for high-revenue share; 99th percentile threshold is £8,000+.
- **RFM Customer Segmentation:** 4,478 identified customers partitioned into *Champions*, *Loyal*, *At Risk*, *Lost*, and *Regular* based on Recency, Frequency, and Monetary scores.
- **Market Basket Co-purchases:** 55,230 multi-item baskets evaluated, producing 166,079 co-purchased product pairs.

---

## Member 4: Performance Analysis & Viva Defense

### Section 8: Performance Analysis (Sequential vs Parallel)

#### Measured Benchmark Data (Online Retail Dataset, 465,674 rows, 12-Core System):

| Worker Processes ($P$) | Execution Time ($T$) | Speedup ($S = T_1 / T_P$) | Parallel Efficiency ($E = S / P$) |
| :---: | :---: | :---: | :---: |
| **1 (Sequential)** | **14.90 s** | **1.00x** | **100.0%** |
| **2** | **10.05 s** | **1.48x** | **74.1%** |
| **4 (Parallel)** | **7.72 s** | **1.93x** | **48.2%** |
| **8** | **7.05 s** | **2.11x** | **26.4%** |
| **12** | **7.41 s** | **2.01x** | **16.7%** |

*(In live single-run comparisons, sequential runs take 18-22s vs 7.5-8.6s in 4-worker mode, yielding up to **2.61x speedup**).*

#### Stage-by-Stage Latency Breakdown:
- **Map 1 Stage:** Drops from **~13.2s** (1 worker) to **~3.5s** (4 workers) — a **~3.8x speedup** on the embarrassingly parallel data parsing phase!
- **Reduce 1 Stage:** **~0.12s - 0.15s** (strictly sequential merge on coordinator).
- **Stage 2 Basket Pairs:** Drops from **~2.2s** to **~1.1s**.
- **Stage 3 Analytics:** **~1.4s - 1.8s** (sequential RFM calculation and percentile sort).

#### Amdahl's Law Explanation:
$$\text{Speedup} = \frac{1}{(1 - P_{frac}) + \frac{P_{frac}}{N}}$$
- The parallelizable fraction $P_{frac}$ is approximately $78\% - 82\%$ (data parsing and pair counting).
- The remaining $18\% - 22\%$ represents serial overhead: file chunking, coordinator dictionary reductions, RFM sorting, and Inter-Process Communication (IPC serialization/pickling via OS pipes).
- As $N$ increases beyond 8 workers, IPC serialization overhead and CPU context switching balance out core compute gains, causing efficiency to taper.

### Section 9: Conclusion
1. **Parallel Execution Superiority:** Parallel multiprocessing cuts execution time substantially from **14.90s down to 7.05s** (and over 22s down to 8.6s in live single runs).
2. **Architecture Portability:** The pure MapReduce design pattern separates independent Map computations from associative Reductions, meaning this exact logic can be scaled out to Apache Spark, Ray, or Dask across multi-node server clusters.

---

## Section 10: Individual Contribution Matrix

| Team Member | Functional Role | Specific Code Artifacts & Deliverables |
| :--- | :--- | :--- |
| **Member 1** | **Data Architect & Partitioning Lead** | - Created dataset generation (`generate_dataset.py`) & schema validation.<br>- Built line-aligned binary partitioner (`make_chunks`).<br>- Handled data cleaning rules & edge cases. |
| **Member 2** | **Parallel Engine & MapReduce Developer** | - Implemented Stage 1 worker mapper (`map_chunk`) & Stage 1 reducer (`reduce_stage1`).<br>- Built `multiprocessing.Pool` coordinator & over-partitioning logic.<br>- Solved OS-level process boundary and chunk isolation issues. |
| **Member 3** | **Analytics & Presentation Lead** | - Developed Stage 2 market-basket mining (`map_pairs`) & RFM customer segmentation.<br>- Created interactive presentation demo (`demo.py`) with PID tracking.<br>- Designed the Chart.js visual dashboard (`output/dashboard.html`). |
| **Member 4** | **Benchmarking & Performance Analyst** | - Built automated benchmarking harness (`benchmark` in `analyze.py`).<br>- Generated speedup/efficiency scaling plots (`speedup.png`) and metrics.<br>- Evaluated Amdahl's Law, IPC overhead, and sequential vs parallel bottlenecks. |

---

## Section 11: Faculty Viva Questions & Model Answers

### Questions for Member 1 (Data & Partitioning):
1. **Q: Why can't you just split a CSV file by dividing file size by $N$?**
   - *A:* Dividing raw byte offsets will almost always land in the middle of a text line, splitting a row into two corrupt fragments. We solve this by seeking to the approximate byte offset and then executing `.readline()`, which scans forward to the exact newline delimiter `\n`, ensuring every worker receives complete records.
2. **Q: What happens if an invoice has line items that span across two chunks?**
   - *A:* Our MapReduce design handles this gracefully. Each worker aggregates invoice rows within its chunk into an invoice dictionary. During the Reduce step, the coordinator merges dictionaries by adding quantities and combining basket item sets for identical `InvoiceNo` keys.

### Questions for Member 2 (Parallel Architecture):
1. **Q: Why did you use `multiprocessing` instead of `threading` in Python?**
   - *A:* Standard Python (CPython) has the Global Interpreter Lock (GIL), which prevents multiple native OS threads from executing Python bytecode simultaneously. For CPU-bound tasks like CSV parsing and integer arithmetic, multithreading runs sequentially. `multiprocessing` creates distinct OS processes with independent memory spaces and their own GIL, enabling true parallel execution across multi-core CPUs.
2. **Q: What is over-partitioning, and why use $4 \times W$ chunks instead of $W$ chunks?**
   - *A:* If we assign exactly 1 chunk per worker, a slower worker or a chunk with complex rows will cause other workers to sit idle (straggler problem). By splitting into $4 \times W$ smaller chunks, we achieve dynamic load balancing: whenever a worker finishes a chunk, it immediately pulls the next chunk from the queue.

### Questions for Member 3 (Analytics & Results):
1. **Q: How does the co-purchase basket analysis work, and why is it separated into Stage 2?**
   - *A:* Baskets cannot be finalized until all chunks from Stage 1 are fully merged, because an invoice might span chunk boundaries. Once merged, Stage 2 extracts items per invoice, filters them ($2 \le \text{len} \le 30$), generates 2-item combinations $\binom{k}{2}$, and distributes basket slices across workers to count co-occurrences in parallel.
2. **Q: What is RFM segmentation and how did you score it?**
   - *A:* RFM stands for Recency (days since last purchase), Frequency (order count), and Monetary value (total spend). We rank-score each customer into quartiles (1 to 4) across R, F, and M, categorizing customers into business segments like Champions, Loyal, At Risk, Lost, and Regular.

### Questions for Member 4 (Performance & Scalability):
1. **Q: Why does speedup plateau at 2.11x instead of 12x on a 12-core CPU?**
   - *A:* By Amdahl's Law, maximum speedup is strictly bounded by the serial fraction ($1 - P_{frac}$). In our pipeline, file partition indexing, coordinator dictionary merges, sorting for percentiles, and IPC pickling/unpickling over OS pipes are inherently sequential. Additionally, Windows process spawning overhead (`spawn` rather than `fork`) introduces inter-process communication latency.
2. **Q: What is the difference between Speedup and Parallel Efficiency?**
   - *A:* Speedup is the ratio of sequential execution time to parallel execution time ($S = T_1 / T_P$). Parallel Efficiency is speedup divided by the number of worker cores ($E = S / P$). While speedup increases as we add workers, efficiency decreases due to communication overhead and serial bottlenecks.
