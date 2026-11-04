# E-Commerce Purchase Pattern Analysis - Report

Processed with **4 worker process(es)** in 8.68s.

## Dataset summary

- rows processed: 465,674
- malformed rows: 0
- valid invoices: 60,000
- cancelled invoices: 1,055
- cancellation rate: 0.0173
- distinct products: 600
- total revenue: 8691325.12

## Top 10 products by units sold

| # | Code | Description | Units | Revenue |
|---|---|---|---|---|
| 1 | 22448C | RED STORAGE TIN 15 | 139,360 | 125,424.00 |
| 2 | 22696 | GLASS STORAGE TIN 46 | 96,468 | 172,677.72 |
| 3 | 21040 | VINTAGE CUSHION 33 | 70,733 | 147,124.64 |
| 4 | 21520 | PINK LUNCH BOX 93 | 59,565 | 68,499.75 |
| 5 | 24320B | PINK BUNTING 55 | 50,420 | 115,461.80 |
| 6 | 20648 | RETRO CLOCK 81 | 45,997 | 137,991.00 |
| 7 | 21712 | GIANT TEAPOT 20 | 38,555 | 42,796.05 |
| 8 | 20752 | SPOTTY BOWL 94 | 35,204 | 45,061.12 |
| 9 | 22488 | HEART CUSHION 20 | 30,695 | 29,160.25 |
| 10 | 21368 | PAPER LUNCH BOX 74 | 28,752 | 56,066.40 |

## Top 10 products by revenue

| # | Code | Description | Revenue | Units |
|---|---|---|---|---|
| 1 | 20792 | FLORAL STORAGE TIN 2 | 324,439.75 | 28,585 |
| 2 | 23640 | GREEN ORNAMENT 67 | 173,506.23 | 21,447 |
| 3 | 22696 | GLASS STORAGE TIN 46 | 172,677.72 | 96,468 |
| 4 | 20416 | RUSTIC CANDLE HOLDER 52 | 161,764.98 | 3,302 |
| 5 | 21888 | PAPER NOTEBOOK 42 | 150,505.50 | 11,190 |
| 6 | 21040 | VINTAGE CUSHION 33 | 147,124.64 | 70,733 |
| 7 | 20648 | RETRO CLOCK 81 | 137,991.00 | 45,997 |
| 8 | 22448C | RED STORAGE TIN 15 | 125,424.00 | 139,360 |
| 9 | 24320B | PINK BUNTING 55 | 115,461.80 | 50,420 |
| 10 | 21232 | GREEN WALL ART 57 | 112,619.85 | 5,943 |

## Purchase patterns

- Peak hour: **12:00** (9,402 orders)
- Busiest weekday: **Wed**
- Highest-revenue month: **2025-12** (1,214,242.15)
- Average order value: 144.86 (median 68.18)
- Average items per order: 7.7

Top co-purchased product pairs:

| Product A | Product B | Baskets |
|---|---|---|
| RED STORAGE TIN 15 | GLASS STORAGE TIN 46 | 9568 |
| VINTAGE CUSHION 33 | RED STORAGE TIN 15 | 7457 |
| PINK LUNCH BOX 93 | RED STORAGE TIN 15 | 6083 |
| VINTAGE CUSHION 33 | GLASS STORAGE TIN 46 | 5259 |
| RED STORAGE TIN 15 | PINK BUNTING 55 | 5249 |
| RETRO CLOCK 81 | RED STORAGE TIN 15 | 4566 |
| PINK LUNCH BOX 93 | GLASS STORAGE TIN 46 | 4338 |
| GIANT TEAPOT 20 | RED STORAGE TIN 15 | 4072 |
| GLASS STORAGE TIN 46 | PINK BUNTING 55 | 3744 |
| SPOTTY BOWL 94 | RED STORAGE TIN 15 | 3674 |

## High-value transactions

- 99th-percentile invoice value: 1,482.50
- Top 1% of invoices generate **18.4%** of revenue

| Invoice | Total | Customer | Date | Country |
|---|---|---|---|---|
| 511416 | 19,360.50 | 14836 | 2025-11-27T10:40:32 | United Kingdom |
| 522930 | 16,150.20 | 12906 | 2026-01-19T11:41:16 | United Kingdom |
| 529926 | 8,144.40 | 12701 | 2026-03-06T15:48:19 | United Kingdom |
| 504900 | 8,027.00 | 16283 | 2025-11-03T09:18:14 | United Kingdom |
| 554164 | 7,715.50 | 15149 | 2026-08-21T17:51:36 | United Kingdom |
| 511087 | 7,454.20 | 16628 | 2025-11-26T11:02:05 | United Kingdom |
| 536294 | 7,396.10 | 13132 | 2026-04-21T13:22:19 | France |
| 509388 | 7,386.60 | 12972 | 2025-11-19T13:03:52 | United Kingdom |
| 516285 | 7,230.80 | 13613 | 2025-12-16T11:43:11 | United Kingdom |
| 554686 | 6,118.40 | 13613 | 2026-08-26T10:27:13 | United Kingdom |

## Customer statistics

- Unique identified customers: 4,478
- Repeat-customer rate: 97.5%
- Avg orders / customer: 12.03
- Avg spend / customer: 1,797.45
- Top 10% of customers account for 59.0% of identified revenue
- Guest checkouts: 7.4% of total revenue

| RFM segment | Customers | Revenue |
|---|---|---|
| Champions | 1,305 | 5,505,778.05 |
| At Risk | 968 | 1,475,111.66 |
| Regular | 1,273 | 490,284.20 |
| Loyal | 388 | 438,991.17 |
| Lost | 544 | 138,816.83 |
