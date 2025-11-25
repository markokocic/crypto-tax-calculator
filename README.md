Crypto tax calculator

Overview
- Single-file Go program (main.go) that parses CSV transaction exports and computes FIFO cost-basis, per-wallet and per-commodity short/long gains and income.
- Current parser is tailored for Kraken-style CSVs. At the moment the program reliably supports Kraken-format exports (grouped refid rows, fiat rows paired with crypto rows, "earn"/"reward"/"autoallocation" subtypes). Other exchanges may require adding a small, format-specific parser.

Build / run
- Ensure Go is installed and module mode is enabled.
- Build / run:
  - go run main.go test_kraken.csv
  - go build -o cryptotax . && ./cryptotax test_kraken.csv

Flags
- -year YYYY
    restrict printed summary to a single tax year (0 = all years)
- -wallet W1,W2
    comma-separated wallet names to include (default: none = all). Values are trimmed.
- -commodity C1,C2
    comma-separated commodity symbols to include (default: none = all). Values are trimmed.
- -v
    verbose logging; prints the list of transactions that match provided filters and additional processing logs.

Notes about formats and behavior
- Currently supports Kraken CSV exports. Kraken-style rows often include paired fiat and crypto lines that share the same refid; the parser groups rows by refid and:
  - Allocates fiat cost/fees proportionally to crypto rows when fiat lines are present.
  - Detects income/reward groups and records only the receiving (positive) crypto rows as income (avoids spurious sells).
  - Detects allocation/autoallocation groups and synthesizes "transfer" transactions that move FIFO basis between wallets (no gain).
- The program skips fiat-only rows (fiat is treated only as price/currency, not a tracked commodity).
- If you want support for another exchange, add one representative CSV for that exchange and I can add a dedicated parser hook.

Precision & dependencies
- All monetary/amount calculations use exact decimal arithmetic (github.com/shopspring/decimal).
- The program only formats and rounds to two decimal places in the final summary output.

Limitations / recommended improvements
- Income valuation: many reward/earn rows lack fiat valuation. To produce accurate income figures you should provide historical price data (or the code can be extended to lookup or accept a -pricefile).
- Wallet name normalization: wallet names must match exactly for filtering; consider normalizing or providing a mapping if you have multiple naming variants.
- Coverage: only Kraken-format parsing is included. More exchanges can be supported by adding parsers.

Example usage
- Default run (all years, all wallets/commodities):
  go run main.go test_kraken.csv
- Filter by year and wallet, verbose:
  go run main.go -year 2025 -wallet "spot / main" -v test_kraken.csv
- Filter by commodity:
  go run main.go -commodity ETH test_kraken.csv

Contact / extending
- If you paste a representative CSV from another exchange (Binance, Coinbase, Trade Republic, etc.) I can provide the small parser changes to add support for that format.

License
- This project is licensed under the Eclipse Public License Version 2.0. See the LICENSE file for the full license text.
- Copyright (c) 2025-present Marko Kocić <marko@euptera.com>
- SPDX-License-Identifier: EPL-2.0
