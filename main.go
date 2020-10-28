package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/anxp/csvutils"
	"github.com/anxp/table2cli"
	clrscreen "github.com/inancgumus/screen"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// One day in seconds:
const operationPeriod int64 = 24 * 60 * 60

// One exmo trade\deal (building brick for other data structures):
type exmoOneTrade struct {
	Amount   float64 `json:"amount,string"`
	Date     int64   `json:"date"`
	Price    float64 `json:"price,string"`
	Quantity float64 `json:"quantity,string"`
	TradeId  int64   `json:"trade_id"`
	Type     string  `json:"type"`
}

// Full EXMO response (keyed with currency-pair ID):
type exmoTradesPerRequest map[string][]exmoOneTrade

// Locally stored trades (outer keyed with currency-pair ID, inner maps keyed by trade_id):
type exmoTradesLocalPool map[string]map[int64]exmoOneTrade

// Elementary building block of aggregated statistics data structure
type exmoAggregatedStatisticsElement struct {
	AvgSellPrice     float64
	AvgBuyPrice      float64
	SellDealsCount   int64
	BuyDealsCount    int64
	SoldAmountUSD    float64
	BoughtAmountUSD  float64
	TgAlphaSellABS   float64
	TgAlphaBuyABS    float64
	TgAlphaSellREL   float64
	TgAlphaBuyREL    float64
	ClosingTimestamp int64
	DataReliable     bool
}

// Aggregated statistics broken down by time slots. So values are averaged within corresponding time slots.
// Time-slot width is user chosen (see getStatisticsByInterval function).
// Smaller index - more recent value.
// Outer map keyed by currency pairs.
type exmoAggregatedStatisticsByTimeSlots map[string][]exmoAggregatedStatisticsElement

func main() {

	currencyPairs := "ETH_USD,LTC_USD,ETC_USD,BTC_USD"

	localPool := make(exmoTradesLocalPool)
	terminationCh := make(chan bool)

	go updatePoolInfiniteLoop(currencyPairs, &localPool)

	// We don't use this channel anywhere, it's needed just for endless work of main func.
	<-terminationCh
}

// Get most recent trades (they will be returned by EXMO API)
func performTradesRequest(currencyPairs string) (*exmoTradesPerRequest, int64, error) {

	exmoMappedResponse := make(exmoTradesPerRequest)
	autodetectedInterval := int64(0)
	requestUrl := "https://api.exmo.com/v1.1/trades"
	contentType := "application/x-www-form-urlencoded"

	resp, err := http.Post(requestUrl, contentType, bytes.NewBuffer([]byte("pair="+currencyPairs)))

	if err != nil {
		// Maybe temporary network error, so let's wait about 10 seconds:
		return nil, 10, err
	}

	defer resp.Body.Close()

	fmt.Printf("Response status: %s; ", resp.Status) //Don't break line here!

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	if err := json.Unmarshal(bodyBytes, &exmoMappedResponse); err != nil {
		// If there are problem with JSON - it means Exmo under maintenance, so it will not be fixed quickly:
		return nil, 60, err
	}

	autodetectedInterval = calcRequestInterval(&exmoMappedResponse)
	fmt.Printf("Autodetected interval: %dsec.\n", autodetectedInterval)

	return &exmoMappedResponse, autodetectedInterval, nil
}

func updateLocalPool(localPool *exmoTradesLocalPool, dataToAppend *exmoTradesPerRequest) *exmoTradesLocalPool {
	ttl := int64(24 * 60 * 60) // Time to live data. 24h in seconds
	currentTimestamp := time.Now().Unix()
	firstRun := len(*localPool) == 0 // If dataPool is empty - this is first whole program run

	for currencyPairKey, responseBody := range *dataToAppend {
		overlapCounter := 0

		if _, ok := (*localPool)[currencyPairKey]; !ok {
			(*localPool)[currencyPairKey] = make(map[int64]exmoOneTrade)
		}

		for _, oneTrade := range responseBody {
			if _, ok := (*localPool)[currencyPairKey][oneTrade.TradeId]; !ok {
				(*localPool)[currencyPairKey][oneTrade.TradeId] = oneTrade
			} else {
				overlapCounter++
			}
		}

		// Let's check overlap counter for CURRENT currencyPair:
		if overlapCounter == 0 && !firstRun {
			fmt.Println("Warning for", currencyPairKey, ": Request results does not overlap (while they should). Decrease request interval.")
		}
	}

	// Let's clean up local pool (delete too old trades):
	for currencyPairKey, currencyPairData := range *localPool {

		outdatedCount := 0

		for tradeId, oneTrade := range currencyPairData {
			if oneTrade.Date < currentTimestamp-ttl {
				// Unset map element
				delete(currencyPairData, tradeId)
				outdatedCount++
			}
		}

		if outdatedCount > 0 {
			fmt.Println("Deleted [", outdatedCount, "] outdated records for ", currencyPairKey, ". (We only store trade records for the last 24 hours).")
		}
	}

	return localPool
}

// Returns statistics aggregated by time slots specified in interval variable (in seconds).
// Use aggregationInterval = 1 * 60 * 60 to specify ONE hour, 2 * 60 * 60 for TWO hours and so on.
func getStatisticsByIntervals(localPool *exmoTradesLocalPool, aggregationInterval int64) exmoAggregatedStatisticsByTimeSlots {

	aggregatedStatisticsByTimeSlots := make(exmoAggregatedStatisticsByTimeSlots)
	currencyPairsList := getCurrencyPairsAsSortedSlice(localPool)
	nowTimestamp := time.Now().Unix()

	slotsCount, remainder := divmod(operationPeriod, aggregationInterval)

	if remainder != 0 {
		panic("Can't divide operation period (by default one day) to equal time slots. Wrong aggregation interval selected!")
	}

	if slotsCount == 0 {
		panic("Time slots count = 0. Too large aggregation interval selected?")
	}

	// Outer cycle iterates through currency pair list (currencyPairsList[i] = "BTC_USD", "ETH_USD", "LTC_USD" and so on):
	for i := 0; i < len(currencyPairsList); i++ {
		aggregatedStatisticsByTimeSlots[currencyPairsList[i]] = make([]exmoAggregatedStatisticsElement, slotsCount)

		// Iterate through time slots (sN stands for "Slot Number") FROM OLD TO RECENT:
		for sN := int64(slotsCount - 1); sN >= 0; sN-- {

			// Init aggregated values for Current Time Slot:
			sellDealsCount := int64(0)
			buyDealsCount := int64(0)
			avgSellPrice := 0.0
			avgBuyPrice := 0.0
			soldAmountUSD := 0.0
			boughtAmountUSD := 0.0
			tgAlphaSellABS := 0.0
			tgAlphaBuyABS := 0.0
			tgAlphaSellREL := 0.0
			tgAlphaBuyREL := 0.0
			dataReliableSell, dataReliableBuy, dataReliable := false, false, false

			// Iterate through WHOLE MAP for selected currency pair:
			for _, oneTrade := range (*localPool)[currencyPairsList[i]] {

				// Check if current trade deal falls within current time slot:
				if (nowTimestamp - (sN + 1) * aggregationInterval) < oneTrade.Date && oneTrade.Date <= (nowTimestamp - sN * aggregationInterval) {

					// Caught it! We found data which falls into current time slot!
					// The last thing - we should separately process SELL deals and BUY deals, so, last check:
					if oneTrade.Type == "sell" {
						sellDealsCount++
						avgSellPrice += oneTrade.Price * oneTrade.Amount // We calculate not just arithmetic average, but weighted average!
						soldAmountUSD += oneTrade.Amount

						dataReliableSell = true
					}

					if oneTrade.Type == "buy" {
						buyDealsCount++
						avgBuyPrice += oneTrade.Price * oneTrade.Amount // We calculate not just arithmetic average, but weighted average!
						boughtAmountUSD += oneTrade.Amount

						dataReliableBuy = true
					}

					// We consider data in current time slot reliable if it contains at least one SELL deal AND one BUY deal:
					dataReliable = dataReliableSell && dataReliableBuy
				}
			}

			// Further calculations makes sense only if current time slot IS NOT EMPTY:
			if dataReliable {
				avgSellPrice /= soldAmountUSD
				avgBuyPrice /= boughtAmountUSD

				// Now, when we have average price for current time slot, we can calculate tg(alpha).
				// But because this value represents the increase (or decrease), we should know average price for previous (sN+1) time slot.
				// Obviously, we can't calculate tg(alpha) for the very first time slot (as we don't have previous one).
				if (sN + 1) < slotsCount && aggregatedStatisticsByTimeSlots[currencyPairsList[i]][sN + 1].DataReliable {
					tgAlphaSellABS = (avgSellPrice - aggregatedStatisticsByTimeSlots[currencyPairsList[i]][sN + 1].AvgSellPrice) / 1 /*float64(aggregationInterval)*/
					tgAlphaBuyABS = (avgBuyPrice - aggregatedStatisticsByTimeSlots[currencyPairsList[i]][sN + 1].AvgBuyPrice) / 1 /*float64(aggregationInterval)*/
					tgAlphaSellREL = (tgAlphaSellABS / aggregatedStatisticsByTimeSlots[currencyPairsList[i]][sN + 1].AvgSellPrice) * 100
					tgAlphaBuyREL = (tgAlphaBuyABS / aggregatedStatisticsByTimeSlots[currencyPairsList[i]][sN + 1].AvgBuyPrice) * 100
				} else {
					tgAlphaSellABS = 0.0
					tgAlphaBuyABS = 0.0
					tgAlphaSellREL = 0.0
					tgAlphaBuyREL = 0.0
				}
			}

			// We did it! Current time slot calculation complete! Write result to data structure:
			aggregatedStatisticsByTimeSlots[currencyPairsList[i]][sN] = struct {
				AvgSellPrice     float64
				AvgBuyPrice      float64
				SellDealsCount   int64
				BuyDealsCount    int64
				SoldAmountUSD    float64
				BoughtAmountUSD  float64
				TgAlphaSellABS   float64
				TgAlphaBuyABS    float64
				TgAlphaSellREL   float64
				TgAlphaBuyREL    float64
				ClosingTimestamp int64
				DataReliable     bool
			} {
				AvgSellPrice: avgSellPrice,
				AvgBuyPrice: avgBuyPrice,
				SellDealsCount: sellDealsCount,
				BuyDealsCount: buyDealsCount,
				SoldAmountUSD: soldAmountUSD,
				BoughtAmountUSD: boughtAmountUSD,
				TgAlphaSellABS: tgAlphaSellABS,
				TgAlphaBuyABS: tgAlphaBuyABS,
				TgAlphaSellREL: tgAlphaSellREL,
				TgAlphaBuyREL: tgAlphaBuyREL,
				ClosingTimestamp: nowTimestamp - sN * aggregationInterval,
				DataReliable: dataReliable,
			}
		}
	}

	return aggregatedStatisticsByTimeSlots
}

func updatePoolInfiniteLoop(currencyPairs string, localPool *exmoTradesLocalPool) {
	var statisticsByIntervals exmoAggregatedStatisticsByTimeSlots
	var formattedForCSVDump [][]string
	aggregationInterval := int64(60 * 60)
	slotWidthHours := math.Round(float64(aggregationInterval) / (60 * 60))

	for {
		// At first, clear screen from previous output:
		clrscreen.Clear()

		requestResult, interval, err := performTradesRequest(currencyPairs)

		if err != nil {
			fmt.Println("ERROR:", err)
			for i := interval; i >= 0; i-- {
				fmt.Printf("\rRequest retry in %dsec.", i)
				time.Sleep(time.Second)

				if i == 0 {
					fmt.Print("\n")
				}
			}

			continue
		}

		localPool = updateLocalPool(localPool, requestResult)

		// TODO: Make a method of POOL structure which return total number of records in pool.
		totalNumberOfRecordsInPool := 0
		for _, currencyPairValues := range *localPool {
			totalNumberOfRecordsInPool += len(currencyPairValues)
		}

		formattedForCSVDump = make([][]string, totalNumberOfRecordsInPool)

		rowNumber := 0
		for currencyPairKey, currencyPairValues := range *localPool {
			for _, oneTrade := range currencyPairValues {
				formattedForCSVDump[rowNumber] = make([]string, 7)

				if rowNumber == 0 {
					// Prepare commented CSV header:
					//TODO: Refactor to reflection, so we'll be able dynamically detect names of fields of structure and use them as field names for CSV.
					formattedForCSVDump[rowNumber][0] = "#CURRENCY_PAIR" // 1st column
					formattedForCSVDump[rowNumber][1] = "TRADE_ID" // 2nd column
					formattedForCSVDump[rowNumber][2] = "DATE" // 3rd
					formattedForCSVDump[rowNumber][3] = "AMOUNT" // 4th
					formattedForCSVDump[rowNumber][4] = "PRICE" // 5th
					formattedForCSVDump[rowNumber][5] = "QUANTITY" // 6th
					formattedForCSVDump[rowNumber][6] = "DEAL_TYPE" // 7th

					rowNumber++
					continue // Header will be placed at the very top of slice once.
				}

				formattedForCSVDump[rowNumber][0] = currencyPairKey
				formattedForCSVDump[rowNumber][1] = strconv.FormatInt(oneTrade.TradeId, 10)
				formattedForCSVDump[rowNumber][2] = strconv.FormatInt(oneTrade.Date, 10)
				formattedForCSVDump[rowNumber][3] = strconv.FormatFloat(oneTrade.Amount, 'f', 8, 32)
				formattedForCSVDump[rowNumber][4] = strconv.FormatFloat(oneTrade.Price, 'f', 8, 32)
				formattedForCSVDump[rowNumber][5] = strconv.FormatFloat(oneTrade.Quantity, 'f', 8, 32)
				formattedForCSVDump[rowNumber][6] = oneTrade.Type

				rowNumber++
			}
		}

		csvutils.NewCSV().Init(formattedForCSVDump).Dump("LocalPoolDump.txt")

		statisticsByIntervals = getStatisticsByIntervals(localPool, aggregationInterval)

		//======= Print NEW table: =======
		th := []string {
			"Slot# / Width (h) / Time",
			"BUY/SELL Ratio (%)",
			"BUY/SELL Price",
			"BUY/SELL Trading Vol. ($)",
			"BUY/SELL tg(⍺)",
		}

		tc := make([][]string, len(statisticsByIntervals["ETH_USD"]))

		for i := 0; i < len(statisticsByIntervals["ETH_USD"]); i++ {
			tc[i] = make([]string, len(th)) // Allocate MEM for columns

			buyCount := statisticsByIntervals["ETH_USD"][i].BuyDealsCount
			sellCount := statisticsByIntervals["ETH_USD"][i].SellDealsCount

			dateTime := time.Unix(statisticsByIntervals["ETH_USD"][i].ClosingTimestamp, 0)

			tc[i][0] = fmt.Sprintf("%2d/%.1f/%s", i, slotWidthHours, dateTime.Format("2006-01-02 15:04PM"))

			if buyCount == 0 || sellCount == 0 {
				tc[i][1] = fmt.Sprintf("%s", "No Data")
				tc[i][2] = fmt.Sprintf("%s", "No Data")
				tc[i][3] = fmt.Sprintf("%s", "No Data")
				tc[i][4] = fmt.Sprintf("%s", "No Data")
			} else {
				tc[i][1] = fmt.Sprintf("%2d%%/%2d%%", (buyCount * 100) / (buyCount + sellCount), (sellCount * 100) / (buyCount + sellCount))
				tc[i][2] = fmt.Sprintf("%8.2f/%8.2f", statisticsByIntervals["ETH_USD"][i].AvgBuyPrice, statisticsByIntervals["ETH_USD"][i].AvgSellPrice)
				tc[i][3] = fmt.Sprintf("%.0f/%.0f", statisticsByIntervals["ETH_USD"][i].BoughtAmountUSD, statisticsByIntervals["ETH_USD"][i].SoldAmountUSD)
				tc[i][4] = fmt.Sprintf("%2.0f%%/%2.0f%%", statisticsByIntervals["ETH_USD"][i].TgAlphaBuyREL, statisticsByIntervals["ETH_USD"][i].TgAlphaSellREL)
			}
		}

		table2cli.PrintTable(th, tc, 27)
		//================================

		// Print countdown to next request:
		for i := interval; i >= 0; i-- {
			fmt.Printf("\rRefresh in %dsec.", i)
			time.Sleep(time.Second)

			if i == 0 {
				fmt.Print("\n")
			}
		}
	}
}

func calcRequestInterval(oneRequestTrades *exmoTradesPerRequest) int64 {
	var intervals []int64

	for _, currencyPairData := range *oneRequestTrades {

		minTS := int64(0)
		maxTS := int64(0)

		for i := 0; i < len(currencyPairData); i++ {
			if i == 0 {
				// Take the first element in set as reference:
				minTS = currencyPairData[i].Date
				maxTS = currencyPairData[i].Date
				continue
			}

			if currencyPairData[i].Date < minTS {
				minTS = currencyPairData[i].Date
			}

			if currencyPairData[i].Date > maxTS {
				maxTS = currencyPairData[i].Date
			}
		}

		intervals = append(intervals, maxTS-minTS)
	}

	sort.Slice(intervals, func(i, j int) bool { return intervals[i] < intervals[j] })

	return int64(float64(intervals[0]) * 0.7) // we return the most LESS interval
}

func getCurrencyPairsAsSortedSlice(localPool *exmoTradesLocalPool) []string {
	// Gather map keys (currency pair IDs) into slice:
	var currencyPairsList []string
	for currencyPairKey, _ := range *localPool {
		currencyPairsList = append(currencyPairsList, currencyPairKey)
	}

	// Sort currency pair IDs alphabetically:
	sort.Strings(currencyPairsList)

	return currencyPairsList
}

func divmod(numerator, denominator int64) (quotient, remainder int64) {
	quotient = numerator / denominator // integer division, decimals are truncated
	remainder = numerator % denominator
	return
}