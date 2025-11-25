// Copyright (c) 2025-present Marko Kocić <marko@euptera.com>
// SPDX-License-Identifier: EPL-2.0
// See LICENSE for full license text.

package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// Minimal crypto tax calculator in one file (meets requirements from requirements.txt).
// Usage: go run main.go [-year YYYY] [-wallet WALLET1,WALLET2] [-commodity C1,C2] [-v] file1.csv file2.csv ...

// Data models
type Tx struct {
	Wallet        string
	Time          time.Time
	Type          string
	Commodity     string
	Currency      string // price currency if present
	Amount        decimal.Decimal
	Cost          decimal.Decimal // total cost/consideration (including fees when appropriate)
	PricePerUnit  decimal.Decimal // cost per unit (Cost / AmountAbs) when applicable
	Fee           decimal.Decimal
	Raw           map[string]string
	SourceFile    string
	ReferenceID   string
	PairedComment string
}

type InventoryEntry struct {
	Time        time.Time
	Amount      decimal.Decimal // positive amount
	UnitCost    decimal.Decimal // cost per unit
	TotalCost   decimal.Decimal // Amount * UnitCost (keeps rounding)
	SourceFiles []string
}

type Gains struct {
	Short  decimal.Decimal
	Long   decimal.Decimal
	Income decimal.Decimal
}

type State struct {
	Inventories     map[string]map[string][]InventoryEntry // wallet -> commodity -> FIFO sorted by Time (oldest first)
	TaxYears        map[int]map[string]map[string]*Gains   // year -> wallet -> commodity -> Gains
	Verbose         bool
	WalletFilter    map[string]bool
	CommodityFilter map[string]bool
}

func NewState(verbose bool, walletFilters []string, commodityFilters []string) *State {
	wf := map[string]bool{}
	for _, w := range walletFilters {
		w = strings.TrimSpace(w)
		if w != "" {
			wf[w] = true
		}
	}
	cf := map[string]bool{}
	for _, c := range commodityFilters {
		c = strings.ToLower(strings.TrimSpace(c))
		if c != "" {
			cf[c] = true
		}
	}
	return &State{
		Inventories:     make(map[string]map[string][]InventoryEntry),
		TaxYears:        make(map[int]map[string]map[string]*Gains),
		Verbose:         verbose,
		WalletFilter:    wf,
		CommodityFilter: cf,
	}
}

// Utilities
func parseFloat(s string) float64 {
	s = strings.TrimSpace(strings.ReplaceAll(s, ",", ""))
	if s == "" {
		return 0
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		// try strip any non-digit characters
		clean := ""
		for _, r := range s {
			if (r >= '0' && r <= '9') || r == '.' || r == '-' {
				clean += string(r)
			}
		}
		f, _ = strconv.ParseFloat(clean, 64)
	}
	return f
}

var timeLayouts = []string{
	time.RFC3339,
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05 MST",
	"2006-01-02",
	"1/2/2006 15:04",
	"1/2/2006 3:04PM",
	"2006-01-02T15:04:05",
}

func parseTimeGuess(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	for _, l := range timeLayouts {
		if t, err := time.Parse(l, s); err == nil {
			return t, nil
		}
	}
	// try trimming timezone part if endswith '+00:00' style
	if idx := strings.LastIndex(s, "+"); idx > 0 {
		if t, err := time.Parse(time.RFC3339, s[:idx]); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unable to parse time: %q", s)
}

func isFiat(asset string) bool {
	a := strings.ToLower(strings.TrimSpace(asset))
	if a == "" {
		return false
	}
	switch a {
	case "eur", "usd", "gbp", "chf", "cad", "aud", "jpy":
		return true
	}
	return false
}

func parseDecimal(s string) decimal.Decimal {
	s = strings.TrimSpace(strings.ReplaceAll(s, ",", ""))
	if s == "" {
		return decimal.Zero
	}
	// try direct parse
	if d, err := decimal.NewFromString(s); err == nil {
		return d
	}
	// strip non-numeric (fallback)
	clean := ""
	for _, r := range s {
		if (r >= '0' && r <= '9') || r == '.' || r == '-' {
			clean += string(r)
		}
	}
	d, _ := decimal.NewFromString(clean)
	return d
}

func minDecimal(a, b decimal.Decimal) decimal.Decimal {
	if a.Cmp(b) <= 0 {
		return a
	}
	return b
}

// CSV parsing pass (supports multiple formats)
func parseCSVFile(path string, defaultWallets []string, verbose bool) ([]Tx, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1

	headerRow, err := r.Read()
	if err != nil {
		return nil, err
	}
	// map header -> index (lowercased)
	headerIdx := map[string]int{}
	for i, h := range headerRow {
		headerIdx[strings.ToLower(strings.TrimSpace(h))] = i
	}
	format := detectFormat(headerIdx)

	// read all rows into memory first
	type rawRow struct {
		rec map[string]string
		idx int
	}
	var rows []rawRow
	rowIdx := 0
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		record := make(map[string]string)
		for k, i := range headerIdx {
			if i >= 0 && i < len(row) {
				record[k] = row[i]
			} else {
				record[k] = ""
			}
		}
		rows = append(rows, rawRow{rec: record, idx: rowIdx})
		rowIdx++
	}

	var txs []Tx

	if format == "kraken" {
		// group by reference id (refid or txid). fallback to index key if none.
		groups := map[string][]rawRow{}
		for _, rr := range rows {
			key := firstNonEmpty(rr.rec, "refid", "txid")
			if key == "" {
				key = fmt.Sprintf("ridx-%d", rr.idx)
			}
			groups[key] = append(groups[key], rr)
		}

		for _, group := range groups {
			// detect income-like group (earn/reward/staking) and transfer-like group (autoallocation/allocation)
			isIncomeGroup := false
			isTransferGroup := false
			for _, rr := range group {
				typ := strings.ToLower(firstNonEmpty(rr.rec, "type", "tx_type"))
				sub := strings.ToLower(firstNonEmpty(rr.rec, "subtype"))
				if strings.Contains(typ, "earn") || strings.Contains(typ, "reward") || strings.Contains(typ, "staking") {
					isIncomeGroup = true
				}
				if strings.Contains(sub, "autoallocation") || strings.Contains(sub, "allocation") {
					// treat allocation/autoallocation as transfer between wallets (preserve basis)
					isTransferGroup = true
				}
			}
			// find fiat rows and crypto rows
			fiatAsset := ""
			totalFiat := decimal.Zero
			fiatFee := decimal.Zero
			cryptoTotalAbs := decimal.Zero
			// collect parsed crypto rows first (without fiat allocation)
			var cryptoRows []map[string]string
			for _, rr := range group {
				asset := firstNonEmpty(rr.rec, "asset", "pair", "symbol")
				amt := parseDecimal(firstNonEmpty(rr.rec, "vol", "amount", "qty"))
				if isFiat(asset) {
					fiatAsset = asset
					totalFiat = totalFiat.Add(amt.Abs())
					fiatFee = fiatFee.Add(parseDecimal(firstNonEmpty(rr.rec, "fee")))
				} else {
					cryptoRows = append(cryptoRows, rr.rec)
					cryptoTotalAbs = cryptoTotalAbs.Add(amt.Abs())
				}
			}

			// If this is a transfer group (autoallocation/allocation), synthesize transfer transactions
			if isTransferGroup && len(cryptoRows) > 0 {
				// build maps of negative (source) and positive (dest) rows grouped by asset
				type rowInfo struct {
					rec map[string]string
					amt decimal.Decimal
				}
				posMap := map[string][]rowInfo{}
				negMap := map[string][]rowInfo{}
				for _, rec := range cryptoRows {
					asset := firstNonEmpty(rec, "asset", "pair", "symbol")
					amt := parseDecimal(firstNonEmpty(rec, "vol", "amount", "qty"))
					ri := rowInfo{rec: rec, amt: amt}
					if amt.Cmp(decimal.Zero) > 0 {
						posMap[strings.ToLower(asset)] = append(posMap[strings.ToLower(asset)], ri)
					} else {
						negMap[strings.ToLower(asset)] = append(negMap[strings.ToLower(asset)], ri)
					}
				}
				// pair positives with negatives and emit transfer txs
				for asset, posList := range posMap {
					negList := negMap[asset]
					for _, p := range posList {
						// try find a matching negative row with similar absolute amount
						var matchedNeg *rowInfo
						for i, n := range negList {
							if n.amt.Abs().Cmp(p.amt.Abs()) == 0 {
								matchedNeg = &negList[i]
								break
							}
						}
						// If not exact match, just pick first negative if exists
						if matchedNeg == nil && len(negList) > 0 {
							matchedNeg = &negList[0]
						}
						// build transfer tx with dest = pos wallet, source in PairedComment
						timeStr := firstNonEmpty(p.rec, "time", "date", "datetime")
						t, _ := parseTimeGuess(timeStr)
						destWallet := firstNonEmpty(p.rec, "wallet", "account")
						if destWallet == "" {
							destWallet = lookupWallet(p.rec, defaultWallets, path)
						}
						ref := firstNonEmpty(p.rec, "refid", "txid")
						srcWallet := ""
						if matchedNeg != nil {
							srcWallet = firstNonEmpty(matchedNeg.rec, "wallet", "account")
							if srcWallet == "" {
								srcWallet = lookupWallet(matchedNeg.rec, defaultWallets, path)
							}
						}
						amt := p.amt.Abs()
						tx := Tx{
							Wallet:        destWallet,
							Time:          t,
							Type:          "transfer",
							Commodity:     p.rec["asset"],
							Currency:      firstNonEmpty(p.rec, "currency", "pair"),
							Amount:        amt,
							Cost:          decimal.Zero,
							PricePerUnit:  decimal.Zero,
							Fee:           decimal.Zero,
							Raw:           p.rec,
							SourceFile:    filepath.Base(path),
							ReferenceID:   ref,
							PairedComment: srcWallet,
						}
						txs = append(txs, tx)
					}
				}
				// done with this group
				continue
			}

			// if we have crypto rows, create Tx for each crypto row and allocate fiat amounts/fees proportionally
			if len(cryptoRows) > 0 {
				for _, rec := range cryptoRows {
					// when this is an income group, only keep the receiving (positive) side and treat as income
					if isIncomeGroup {
						amt := parseDecimal(firstNonEmpty(rec, "vol", "amount", "qty"))
						if amt.Cmp(decimal.Zero) <= 0 {
							// skip the negative source line (avoid generating a sell)
							continue
						}
					}
					tx, err := parseKrakenRecord(rec, path, defaultWallets)
					if err != nil {
						if verbose {
							log.Printf("skipping kraken row due to parse error: %v", err)
						}
						continue
					}
					if fiatAsset != "" && !cryptoTotalAbs.IsZero() {
						// allocate fiat cost and fee proportionally
						amtAbs := tx.Amount.Abs()
						proportion := decimal.Zero
						if !cryptoTotalAbs.IsZero() {
							proportion = amtAbs.Div(cryptoTotalAbs)
						}
						tx.Cost = totalFiat.Mul(proportion)
						tx.Currency = fiatAsset
						tx.Fee = fiatFee.Mul(proportion)
						if !tx.Amount.IsZero() {
							tx.PricePerUnit = tx.Cost.Abs().Div(tx.Amount.Abs())
						}
					}
					// force income type for earn/reward groups so handler treats as income
					if isIncomeGroup {
						tx.Type = "income"
					}
					txs = append(txs, tx)
				}
			} else {
				// group has no crypto (fiat-only): skip (we don't treat fiat as commodity)
				if verbose {
					// optional debug
				}
			}
		}
	} else {
		// generic: parse each row, but skip fiat-only rows (don't create tx for fiat assets)
		for _, rr := range rows {
			asset := firstNonEmpty(rr.rec, "asset", "symbol", "commodity", "pair")
			if isFiat(asset) {
				// skip fiat rows
				continue
			}
			if tx, err := parseGenericRecord(rr.rec, path, defaultWallets); err == nil {
				txs = append(txs, tx)
			} else {
				if verbose {
					log.Printf("skipping row due to parse error: %v", err)
				}
			}
		}
	}

	if verbose {
		log.Printf("parsed %d tx from %s (format=%s)", len(txs), path, format)
	}
	return txs, nil
}

func detectFormat(headerIdx map[string]int) string {
	// Kraken CSV typically has "txid","time","type","asset","amount","fee","cost","price",...
	// Use heuristic
	if _, ok := headerIdx["txid"]; ok {
		if _, ok2 := headerIdx["time"]; ok2 {
			if _, ok3 := headerIdx["type"]; ok3 {
				return "kraken"
			}
		}
	}
	// Falling back to generic
	return "generic"
}

// Kraken-specific mapping
func parseKrakenRecord(record map[string]string, srcFile string, defaultWallets []string) (Tx, error) {
	// required fields: time, type, asset/pair, vol/amount, fee, cost/price
	timeStr := firstNonEmpty(record, "time", "date", "datetime")
	if timeStr == "" {
		return Tx{}, fmt.Errorf("no time")
	}
	t, err := parseTimeGuess(timeStr)
	if err != nil {
		return Tx{}, err
	}
	typ := strings.ToLower(firstNonEmpty(record, "type", "tx_type"))
	asset := firstNonEmpty(record, "asset", "pair", "symbol")
	amount := parseDecimal(firstNonEmpty(record, "vol", "amount", "qty"))
	fee := parseDecimal(firstNonEmpty(record, "fee"))
	cost := parseDecimal(firstNonEmpty(record, "cost", "value", "price")) // cost may be total or unit price
	// If cost looks like unit price but we have amount, compute total cost
	pricePer := parseDecimal(firstNonEmpty(record, "price"))
	totalCost := cost
	if totalCost.IsZero() && !pricePer.IsZero() {
		totalCost = pricePer.Mul(amount.Abs())
	}
	// add fee to cost for buys; for sells, fee reduces proceeds; general approach include fees into cost for buys, subtract from proceeds for sells
	if typ == "buy" || typ == "deposit" || typ == "staking" || typ == "reward" || typ == "stakingreward" {
		totalCost = totalCost.Add(fee)
	} else if typ == "sell" {
		// we'll keep fee in Fee field and treat appropriately in processing pass
	}
	wallet := lookupWallet(record, defaultWallets, srcFile)
	tx := Tx{
		Wallet:       wallet,
		Time:         t,
		Type:         typ,
		Commodity:    asset,
		Currency:     firstNonEmpty(record, "currency", "pair"),
		Amount:       amount,
		Cost:         totalCost,
		PricePerUnit: decimal.Zero,
		Fee:          fee,
		Raw:          record,
		SourceFile:   filepath.Base(srcFile),
		ReferenceID:  firstNonEmpty(record, "txid", "refid", "orderno"),
	}
	if !tx.Amount.IsZero() {
		tx.PricePerUnit = tx.Cost.Abs().Div(tx.Amount.Abs())
	}
	return tx, nil
}

func parseGenericRecord(record map[string]string, srcFile string, defaultWallets []string) (Tx, error) {
	// Try common fields
	timeStr := firstNonEmpty(record, "time", "date", "datetime")
	if timeStr == "" {
		return Tx{}, fmt.Errorf("no time")
	}
	t, err := parseTimeGuess(timeStr)
	if err != nil {
		return Tx{}, err
	}
	typ := strings.ToLower(firstNonEmpty(record, "type", "tx_type", "category"))
	asset := firstNonEmpty(record, "asset", "symbol", "commodity", "pair")
	amount := parseDecimal(firstNonEmpty(record, "amount", "qty", "vol"))
	fee := parseDecimal(firstNonEmpty(record, "fee"))
	cost := parseDecimal(firstNonEmpty(record, "cost", "value", "price", "proceeds"))
	totalCost := cost
	pricePer := parseDecimal(firstNonEmpty(record, "price"))
	if totalCost.IsZero() && !pricePer.IsZero() {
		totalCost = pricePer.Mul(amount.Abs())
	}
	if typ == "buy" || strings.Contains(typ, "buy") {
		totalCost = totalCost.Add(fee)
	}
	wallet := lookupWallet(record, defaultWallets, srcFile)
	tx := Tx{
		Wallet:       wallet,
		Time:         t,
		Type:         typ,
		Commodity:    asset,
		Currency:     firstNonEmpty(record, "currency"),
		Amount:       amount,
		Cost:         totalCost,
		PricePerUnit: decimal.Zero,
		Fee:          fee,
		Raw:          record,
		SourceFile:   filepath.Base(srcFile),
		ReferenceID:  firstNonEmpty(record, "id", "txid", "refid"),
	}
	if !tx.Amount.IsZero() {
		tx.PricePerUnit = tx.Cost.Abs().Div(tx.Amount.Abs())
	}
	return tx, nil
}

func firstNonEmpty(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[strings.ToLower(k)]; ok {
			if strings.TrimSpace(v) != "" {
				return v
			}
		}
		// also try raw key as-is
		if v, ok := m[k]; ok {
			if strings.TrimSpace(v) != "" {
				return v
			}
		}
	}
	return ""
}

func lookupWallet(record map[string]string, defaults []string, srcFile string) string {
	// Prefer explicit wallet column; otherwise use default wallets or filename
	if w := firstNonEmpty(record, "wallet", "account"); w != "" {
		return w
	}
	if len(defaults) > 0 && defaults[0] != "" {
		// pick first if multiple provided; a better implementation could try mapping by currency or formatted name
		return defaults[0]
	}
	return filepath.Base(srcFile)
}

// Merge and sort transactions by time
func mergeAndSortTxs(all [][]Tx) []Tx {
	var merged []Tx
	for _, chunk := range all {
		merged = append(merged, chunk...)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Time.Equal(merged[j].Time) {
			// stable tie-breaker by source file and reference id
			if merged[i].SourceFile != merged[j].SourceFile {
				return merged[i].SourceFile < merged[j].SourceFile
			}
			return merged[i].ReferenceID < merged[j].ReferenceID
		}
		return merged[i].Time.Before(merged[j].Time)
	})
	return merged
}

// Processing pass
type txHandlerFunc func(s *State, tx Tx) error

func processTransactions(state *State, txs []Tx) error {
	handlers := getHandlers()
	for _, tx := range txs {
		if state.Verbose {
			// Only show verbose logs for transactions that match wallet and commodity filters (if filters provided)
			show := true
			if len(state.WalletFilter) > 0 {
				if !state.WalletFilter[tx.Wallet] {
					show = false
				}
			}
			if len(state.CommodityFilter) > 0 {
				if !state.CommodityFilter[strings.ToLower(strings.TrimSpace(tx.Commodity))] {
					show = false
				}
			}
			if show {
				log.Printf("processing tx: %s %s %s %s cost=%s fee=%s src=%s ref=%s",
					tx.Time.Format(time.RFC3339), tx.Type, tx.Amount.String(), tx.Commodity, tx.Cost.String(), tx.Fee.String(), tx.SourceFile, tx.ReferenceID)
			}
		}
		h := handlers[normalizeType(tx.Type)]
		if h == nil {
			// fallback by heuristics
			tt := strings.ToLower(tx.Type)
			switch {
			case strings.Contains(tt, "sell") || tx.Amount.Cmp(decimal.Zero) < 0:
				h = handlers["sell"]
			case strings.Contains(tt, "buy") || tx.Amount.Cmp(decimal.Zero) > 0:
				h = handlers["buy"]
			case strings.Contains(tt, "reward") || strings.Contains(tt, "staking") || strings.Contains(tt, "deposit") || strings.Contains(tt, "income"):
				h = handlers["income"]
			case strings.Contains(tt, "convert") || strings.Contains(tt, "trade"):
				h = handlers["convert"]
			case strings.Contains(tt, "transfer"):
				h = handlers["transfer"]
			default:
				// default: if positive amount -> buy, negative -> sell
				if tx.Amount.Cmp(decimal.Zero) > 0 {
					h = handlers["buy"]
				} else {
					h = handlers["sell"]
				}
			}
		}
		if err := h(state, tx); err != nil {
			return err
		}
	}
	return nil
}

func normalizeType(t string) string {
	return strings.ToLower(strings.TrimSpace(t))
}

func getHandlers() map[string]txHandlerFunc {
	return map[string]txHandlerFunc{
		"buy":      handleBuy,
		"sell":     handleSell,
		"income":   handleIncome,
		"reward":   handleIncome,
		"staking":  handleIncome,
		"deposit":  handleIncome,
		"convert":  handleConvert,
		"trade":    handleConvert,
		"transfer": handleTransfer,
	}
}

// Inventory helpers
func ensureInventoryBucket(state *State, wallet, commodity string) {
	if _, ok := state.Inventories[wallet]; !ok {
		state.Inventories[wallet] = make(map[string][]InventoryEntry)
	}
	if _, ok := state.Inventories[wallet][commodity]; !ok {
		state.Inventories[wallet][commodity] = []InventoryEntry{}
	}
}

func addInventory(state *State, wallet, commodity string, entry InventoryEntry) {
	ensureInventoryBucket(state, wallet, commodity)
	state.Inventories[wallet][commodity] = append(state.Inventories[wallet][commodity], entry)
	// keep sorted oldest first
	sort.Slice(state.Inventories[wallet][commodity], func(i, j int) bool {
		a := state.Inventories[wallet][commodity]
		return a[i].Time.Before(a[j].Time)
	})
}

// Get or create gains entry for year/wallet/commodity
func getGainsSlot(state *State, year int, wallet, commodity string) *Gains {
	if _, ok := state.TaxYears[year]; !ok {
		state.TaxYears[year] = make(map[string]map[string]*Gains)
	}
	if _, ok := state.TaxYears[year][wallet]; !ok {
		state.TaxYears[year][wallet] = make(map[string]*Gains)
	}
	if _, ok := state.TaxYears[year][wallet][commodity]; !ok {
		state.TaxYears[year][wallet][commodity] = &Gains{
			Short:  decimal.Zero,
			Long:   decimal.Zero,
			Income: decimal.Zero,
		}
	}
	return state.TaxYears[year][wallet][commodity]
}

// Handler implementations

func handleBuy(s *State, tx Tx) error {
	if tx.Amount.Cmp(decimal.Zero) <= 0 {
		// treat as buy of positive amount; if negative probably recorded as sell elsewhere
	}
	wallet := tx.Wallet
	commodity := tx.Commodity
	amount := tx.Amount.Abs()
	unitCost := decimal.Zero
	if !amount.IsZero() {
		unitCost = tx.Cost.Div(amount)
	}
	entry := InventoryEntry{
		Time:        tx.Time,
		Amount:      amount,
		UnitCost:    unitCost,
		TotalCost:   unitCost.Mul(amount),
		SourceFiles: []string{tx.SourceFile},
	}
	if s.Verbose {
		log.Printf("BUY: wallet=%s commodity=%s amt=%s unitCost=%s total=%s", wallet, commodity, amount.String(), unitCost.String(), entry.TotalCost.String())
	}
	addInventory(s, wallet, commodity, entry)
	return nil
}

func handleIncome(s *State, tx Tx) error {
	// Rewards/stakes: add to inventory and mark income (taxable in year)
	wallet := tx.Wallet
	commodity := tx.Commodity
	amount := tx.Amount
	if amount.IsZero() {
		return nil
	}
	amountAbs := amount.Abs()
	// Use provided cost if available; otherwise zero
	unitCost := decimal.Zero
	totalCost := decimal.Zero
	if !tx.Cost.IsZero() {
		totalCost = tx.Cost
		if !amountAbs.IsZero() {
			unitCost = totalCost.Div(amountAbs)
		}
	}
	// Add to inventory
	entry := InventoryEntry{
		Time:        tx.Time,
		Amount:      amountAbs,
		UnitCost:    unitCost,
		TotalCost:   totalCost,
		SourceFiles: []string{tx.SourceFile},
	}
	addInventory(s, wallet, commodity, entry)
	year := tx.Time.Year()
	slot := getGainsSlot(s, year, wallet, commodity)
	// Income should be recorded as the fair value at receipt; we approximate with tx.Cost if present else zero
	slot.Income = slot.Income.Add(totalCost)
	if s.Verbose {
		log.Printf("INCOME: wallet=%s commodity=%s amt=%s value=%s year=%d", wallet, commodity, amountAbs.String(), totalCost.String(), year)
	}
	return nil
}

func handleSell(s *State, tx Tx) error {
	wallet := tx.Wallet
	commodity := tx.Commodity
	amount := tx.Amount.Abs() // amount sold
	if amount.IsZero() {
		// no-op
		return nil
	}
	ensureInventoryBucket(s, wallet, commodity)
	inv := s.Inventories[wallet][commodity]
	remaining := amount
	proceedsTotal := tx.Cost
	// If cost field was not provided, attempt to compute proceeds from price*amount
	if proceedsTotal.IsZero() {
		if !tx.PricePerUnit.IsZero() {
			proceedsTotal = tx.PricePerUnit.Mul(amount)
		}
	}
	// Fees reduce proceeds for sells
	proceedsTotal = proceedsTotal.Sub(tx.Fee)
	if s.Verbose {
		log.Printf("SELL: wallet=%s commodity=%s amt=%s proceeds=%s fee=%s", wallet, commodity, amount.String(), proceedsTotal.String(), tx.Fee.String())
	}
	proceedsRemaining := proceedsTotal
	// iterate FIFO
	newInv := []InventoryEntry{}
	for i := 0; i < len(inv); i++ {
		entry := inv[i]
		if remaining.Cmp(decimal.Zero) <= 0 {
			newInv = append(newInv, entry)
			continue
		}
		if entry.Amount.Cmp(decimal.Zero) <= 0 {
			continue
		}
		use := minDecimal(entry.Amount, remaining)
		portionCostBasis := entry.UnitCost.Mul(use)
		// allocate matching portion of proceeds proportionally
		portionProceeds := decimal.Zero
		if !amount.IsZero() {
			portionProceeds = proceedsTotal.Mul(use).Div(amount)
		}
		// determine holding period
		holdingDays := tx.Time.Sub(entry.Time).Hours() / 24.0
		year := tx.Time.Year()
		gainsSlot := getGainsSlot(s, year, wallet, commodity)
		gain := portionProceeds.Sub(portionCostBasis)
		if holdingDays >= 365.0 {
			gainsSlot.Long = gainsSlot.Long.Add(gain)
		} else {
			gainsSlot.Short = gainsSlot.Short.Add(gain)
		}
		if s.Verbose {
			holdingStr := "SHORT"
			if holdingDays >= 365.0 {
				holdingStr = "LONG"
			}
			log.Printf("  Consumed FIFO entry: time=%s use=%s unitCost=%s cost=%s proceeds=%s gain=%s holdingDays=%.1f -> %s",
				entry.Time.Format("2006-01-02"), use.String(), entry.UnitCost.String(), portionCostBasis.String(), portionProceeds.String(), gain.String(), holdingDays, holdingStr)
		}
		// decrease the entry amount
		entry.Amount = entry.Amount.Sub(use)
		entry.TotalCost = entry.UnitCost.Mul(entry.Amount)
		remaining = remaining.Sub(use)
		proceedsRemaining = proceedsRemaining.Sub(portionProceeds)
		if entry.Amount.Cmp(decimal.NewFromFloat(1e-12)) > 0 {
			newInv = append(newInv, entry)
		}
	}
	eps := decimal.NewFromFloat(1e-9)
	if remaining.Cmp(eps) > 0 {
		// sold more than inventory: treat as negative inventory (short) or ignore with warning
		if s.Verbose {
			log.Printf("WARNING: selling more (%s) than available in inventory for %s/%s; remaining=%s", amount.String(), wallet, commodity, remaining.String())
		}
	}
	s.Inventories[wallet][commodity] = newInv
	return nil
}

func handleConvert(s *State, tx Tx) error {
	// Treat conversion as sell of one commodity and buy of another.
	// Heuristic: if amount > 0 then buy; if <0 then sell. If pair info is present try to infer counterpart.
	// Simpler approach: if amount < 0 => sell commodity; if >0 => buy commodity.
	if tx.Amount.Cmp(decimal.Zero) < 0 {
		// treat as sell
		return handleSell(s, tx)
	} else if tx.Amount.Cmp(decimal.Zero) > 0 {
		// treat as buy
		return handleBuy(s, tx)
	}
	return nil
}

func handleTransfer(s *State, tx Tx) error {
	// Move FIFO inventory from source wallet (PairedComment) to destination wallet (tx.Wallet) preserving original unit costs and timestamps.
	srcWallet := strings.TrimSpace(tx.PairedComment)
	destWallet := tx.Wallet
	commodity := tx.Commodity
	amountToMove := tx.Amount.Abs()
	if amountToMove.IsZero() {
		return nil
	}
	if srcWallet == "" {
		if s.Verbose {
			log.Printf("TRANSFER: missing source wallet in PairedComment for tx ref=%s", tx.ReferenceID)
		}
		return nil
	}
	ensureInventoryBucket(s, srcWallet, commodity)
	ensureInventoryBucket(s, destWallet, commodity)
	srcInv := s.Inventories[srcWallet][commodity]
	remaining := amountToMove
	newSrcInv := []InventoryEntry{}
	for i := 0; i < len(srcInv); i++ {
		entry := srcInv[i]
		if remaining.Cmp(decimal.Zero) <= 0 {
			newSrcInv = append(newSrcInv, entry)
			continue
		}
		if entry.Amount.Cmp(decimal.Zero) <= 0 {
			continue
		}
		use := minDecimal(entry.Amount, remaining)
		// create a moved entry for dest preserving time and unit cost
		moved := InventoryEntry{
			Time:        entry.Time,
			Amount:      use,
			UnitCost:    entry.UnitCost,
			TotalCost:   entry.UnitCost.Mul(use),
			SourceFiles: append([]string{}, entry.SourceFiles...),
		}
		addInventory(s, destWallet, commodity, moved)
		// decrease source entry
		entry.Amount = entry.Amount.Sub(use)
		entry.TotalCost = entry.Amount.Mul(entry.UnitCost)
		remaining = remaining.Sub(use)
		if entry.Amount.Cmp(decimal.NewFromFloat(1e-12)) > 0 {
			newSrcInv = append(newSrcInv, entry)
		}
	}
	if remaining.Cmp(decimal.NewFromFloat(1e-9)) > 0 {
		if s.Verbose {
			log.Printf("TRANSFER WARNING: moved less (%s) than requested (%s) for %s from %s to %s", amountToMove.Sub(remaining).String(), amountToMove.String(), commodity, srcWallet, destWallet)
		}
	}
	s.Inventories[srcWallet][commodity] = newSrcInv
	return nil
}

// Output helpers
func printSummary(state *State, yearFilter int, walletFilter []string, commodityFilter []string) {
	// Build set for wallet filter
	wset := map[string]bool{}
	for _, w := range walletFilter {
		wset[w] = true
	}
	// Build set for commodity filter (case-insensitive)
	cset := map[string]bool{}
	for _, c := range commodityFilter {
		c = strings.ToLower(strings.TrimSpace(c))
		if c != "" {
			cset[c] = true
		}
	}

	years := []int{}
	for y := range state.TaxYears {
		years = append(years, y)
	}
	sort.Ints(years)
	for _, y := range years {
		if yearFilter != 0 && y != yearFilter {
			continue
		}
		fmt.Printf("Year %d:\n", y)
		wallets := []string{}
		for w := range state.TaxYears[y] {
			if len(wset) > 0 {
				if !wset[w] {
					continue
				}
			}
			wallets = append(wallets, w)
		}
		sort.Strings(wallets)
		for _, w := range wallets {
			fmt.Printf("  Wallet: %s\n", w)
			commods := []string{}
			for c := range state.TaxYears[y][w] {
				// apply commodity filter if provided
				if len(cset) > 0 {
					if !cset[strings.ToLower(c)] {
						continue
					}
				}
				commods = append(commods, c)
			}
			sort.Strings(commods)
			for _, c := range commods {
				g := state.TaxYears[y][w][c]
				fmt.Printf("    %s: short=%s long=%s income=%s\n",
					c,
					g.Short.StringFixed(2),
					g.Long.StringFixed(2),
					g.Income.StringFixed(2),
				)
			}
		}
	}
}

func main() {
	year := flag.Int("year", 0, "tax year to report (e.g. 2023). 0 = all years")
	wallets := flag.String("wallet", "", "comma-separated wallet(s) to include (default: all). If not specified each file name becomes a wallet")
	commodities := flag.String("commodity", "", "comma-separated commodity symbols to include (default: all). Example: BTC,ETH")
	verbose := flag.Bool("v", false, "verbose logging")
	flag.Parse()
	files := flag.Args()
	if len(files) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: %s [-year YYYY] [-wallet W1,W2] [-commodity C1,C2] [-v] file1.csv [file2.csv ...]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}
	defaultWallets := []string{}
	if *wallets != "" {
		for _, w := range strings.Split(*wallets, ",") {
			w = strings.TrimSpace(w)
			if w != "" {
				defaultWallets = append(defaultWallets, w)
			}
		}
	}
	commodityFilterList := []string{}
	if *commodities != "" {
		for _, c := range strings.Split(*commodities, ",") {
			c = strings.TrimSpace(c)
			if c != "" {
				commodityFilterList = append(commodityFilterList, c)
			}
		}
	}

	allParsed := [][]Tx{}
	for _, f := range files {
		txs, err := parseCSVFile(f, defaultWallets, *verbose)
		if err != nil {
			log.Fatalf("error parsing %s: %v", f, err)
		}
		allParsed = append(allParsed, txs)
	}
	all := mergeAndSortTxs(allParsed)

	// If commodity filter provided, filter transactions before processing to avoid tracking unwanted commodities
	if len(commodityFilterList) > 0 {
		cset := map[string]bool{}
		for _, c := range commodityFilterList {
			cset[strings.ToLower(strings.TrimSpace(c))] = true
		}
		filtered := []Tx{}
		for _, tx := range all {
			if tx.Commodity == "" {
				continue
			}
			if cset[strings.ToLower(tx.Commodity)] {
				filtered = append(filtered, tx)
			}
		}
		all = filtered
	}

	// If wallet filter provided, filter transactions before processing to avoid tracking unwanted wallets
	if len(defaultWallets) > 0 {
		wset := map[string]bool{}
		for _, w := range defaultWallets {
			wset[strings.TrimSpace(w)] = true
		}
		filtered := []Tx{}
		for _, tx := range all {
			if wset[tx.Wallet] {
				filtered = append(filtered, tx)
			}
		}
		all = filtered
	}

	// Verbose listing: show transactions that match the command-line wallet and commodity filters
	if *verbose {
		fmt.Println("Transactions matching filters:")
		// build commodity set for quick lookup
		cset := map[string]bool{}
		for _, c := range commodityFilterList {
			c = strings.ToLower(strings.TrimSpace(c))
			if c != "" {
				cset[c] = true
			}
		}
		for _, tx := range all {
			// wallet filter check (if provided)
			if len(defaultWallets) > 0 {
				matchW := false
				for _, w := range defaultWallets {
					if strings.TrimSpace(w) == tx.Wallet {
						matchW = true
						break
					}
				}
				if !matchW {
					continue
				}
			}
			// commodity filter check (if provided)
			if len(cset) > 0 {
				if tx.Commodity == "" || !cset[strings.ToLower(strings.TrimSpace(tx.Commodity))] {
					continue
				}
			}
			fmt.Printf("  %s  wallet=%s  type=%s  amt=%s %s  cost=%s fee=%s src=%s ref=%s\n",
				tx.Time.Format(time.RFC3339), tx.Wallet, tx.Type, tx.Amount.String(), tx.Commodity, tx.Cost.String(), tx.Fee.String(), tx.SourceFile, tx.ReferenceID)
		}
	}

	// Create state with filters so verbose logging can respect them
	state := NewState(*verbose, defaultWallets, commodityFilterList)
	if err := processTransactions(state, all); err != nil {
		log.Fatalf("processing error: %v", err)
	}
	// print results
	wfilter := defaultWallets
	printSummary(state, *year, wfilter, commodityFilterList)
}
