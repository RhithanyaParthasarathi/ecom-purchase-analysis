"""Generate a synthetic e-commerce transaction dataset with the same schema as the
UCI "Online Retail" dataset:

    InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country

Features mimicking the real data: Zipf-like product/customer popularity, product
"themes" that tend to be bought together, wholesale buyers, cancellations
(InvoiceNo starting with 'C', negative quantity), guest checkouts (blank CustomerID),
no Saturday trading, seasonal peak in Nov-Dec.

Usage: python generate_dataset.py [n_invoices] [output_csv]
"""
import csv
import math
import os
import random
import sys
from datetime import datetime, timedelta

ADJ = ["Vintage", "Retro", "Ceramic", "Wooden", "Glass", "Pink", "Blue", "Red", "Green",
       "Floral", "Spotty", "Heart", "Paper", "Metal", "Woolly", "Giant", "Mini", "Rustic"]
NOUN = ["Lantern", "Mug", "Cushion", "Candle Holder", "Bunting", "Tea Set", "Photo Frame",
        "Bag", "Notebook", "Jar", "Doormat", "Clock", "Teapot", "Bowl", "Hot Water Bottle",
        "Lunch Box", "Storage Tin", "Wall Art", "Napkins", "Ornament"]
COUNTRIES = ["United Kingdom", "Germany", "France", "EIRE", "Spain", "Netherlands",
             "Belgium", "Switzerland", "Portugal", "Australia", "Norway", "Italy"]
C_WEIGHTS = [88, 2.5, 2.2, 2, 0.9, 0.8, 0.6, 0.5, 0.4, 0.4, 0.3, 0.3]


def main(n_invoices=60000, out="data/online_retail.csv", seed=42):
    rnd = random.Random(seed)
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)

    # ---- products: 600 items grouped into themes of 6 ----
    n_prod = 600
    products = []
    for i in range(n_prod):
        code = str(20000 + i * 7 % 70000 + i)
        if rnd.random() < 0.25:
            code += rnd.choice("ABC")
        desc = f"{rnd.choice(ADJ)} {rnd.choice(NOUN)} {i % 97}".upper()
        price = max(0.29, round(math.exp(rnd.gauss(1.0, 0.8)), 2))
        products.append((code, desc, price))
    pop = [1 / (r + 1) ** 0.9 for r in range(n_prod)]
    rnd.shuffle(pop)
    themes = [list(range(i, min(i + 6, n_prod))) for i in range(0, n_prod, 6)]

    # ---- customers ----
    n_cust = 4500
    cust_ids = [str(12347 + i) for i in range(n_cust)]
    c_w = [1 / (r + 1) ** 0.7 for r in range(n_cust)]
    rnd.shuffle(c_w)
    wholesalers = set(rnd.sample(cust_ids, int(0.05 * n_cust)))
    cust_country = {c: rnd.choices(COUNTRIES, C_WEIGHTS)[0] for c in cust_ids}

    # ---- invoice timestamps (seasonality, no Saturdays, business hours) ----
    start = datetime(2025, 10, 1)
    days = [start + timedelta(days=d) for d in range(365)]
    d_w = []
    for d in days:
        w = 1.0
        if d.weekday() == 5:
            w = 0.0
        elif d.weekday() == 6:
            w = 0.7
        if d.month in (11, 12):
            w *= 1.8
        d_w.append(w)
    h_w = {8: 2, 9: 4, 10: 8, 11: 9, 12: 12, 13: 11, 14: 10, 15: 8, 16: 5, 17: 3, 18: 1.5, 19: 1}
    hours = list(h_w)
    stamps = []
    for d in rnd.choices(days, d_w, k=n_invoices):
        stamps.append(d.replace(hour=rnd.choices(hours, [h_w[h] for h in hours])[0],
                                minute=rnd.randrange(60), second=rnd.randrange(60)))
    stamps.sort()

    qty_choices = [1, 2, 3, 4, 6, 12, 24]
    qty_w = [30, 22, 14, 10, 12, 8, 4]
    rows = 0
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
                    "UnitPrice", "CustomerID", "Country"])
        for i, ts in enumerate(stamps):
            inv = str(500000 + i)
            guest = rnd.random() < 0.10
            cust = "" if guest else rnd.choices(cust_ids, c_w)[0]
            country = rnd.choices(COUNTRIES, C_WEIGHTS)[0] if guest else cust_country[cust]
            mult = 10 if cust in wholesalers else 1
            n_items = min(40, 1 + int(rnd.expovariate(1 / 7)))
            chosen = set()
            if rnd.random() < 0.5:
                th = rnd.choices(themes, k=1)[0]
                chosen.update(rnd.sample(th, min(len(th), rnd.randint(2, 4))))
            while len(chosen) < n_items:
                chosen.add(rnd.choices(range(n_prod), pop)[0])
            lines = []
            for p in chosen:
                code, desc, price = products[p]
                q = rnd.choices(qty_choices, qty_w)[0] * mult
                lines.append((inv, code, desc, q, ts.strftime("%Y-%m-%d %H:%M:%S"),
                              f"{price:.2f}", cust, country))
            for ln in lines:
                w.writerow(ln)
                rows += 1
            # ~2% of invoices get a cancellation shortly afterwards
            if cust and rnd.random() < 0.02:
                cts = ts + timedelta(hours=rnd.randint(1, 48))
                cinv = "C" + inv
                for ln in rnd.sample(lines, max(1, len(lines) // 2)):
                    w.writerow((cinv, ln[1], ln[2], -ln[3], cts.strftime("%Y-%m-%d %H:%M:%S"),
                                ln[5], cust, country))
                    rows += 1
    print(f"wrote {rows:,} rows to {out}")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 60000
    o = sys.argv[2] if len(sys.argv) > 2 else "data/online_retail.csv"
    main(n, o)
