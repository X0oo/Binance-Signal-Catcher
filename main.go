package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ============== 1. 설정 및 구조체 정의 ==============

// Timeframes는 분석할 캔들 시간 단위 목록입니다. (요구사항에 따라 하드코딩)
var Timeframes = []string{"3m", "15m", "1h", "4h", "1d", "1W", "1M"}

// CandleStore는 메모리 내에 캔들 데이터를 저장하고 동시성 접근을 관리합니다.
// HFT 시스템의 핵심으로, 디스크 I/O 없이 실시간 분석이 가능하도록 합니다.
type CandleStore struct {
	sync.RWMutex
	data map[string][]*Kline // Key: "SYMBOL-TIMEFRAME", Value: 캔들 슬라이스 (최신 1000개 유지)
}

// Job은 워커풀에서 처리할 개별 작업을 정의합니다. (초기 데이터 로딩용)
type Job struct {
	Symbol    string
	Timeframe string
}

// Config는 config.json 파일의 설정을 담는 구조체입니다.
type Config struct {
	TargetMarkets []string `json:"target_markets"` // ["SPOT", "FUTURE"]
	QuoteAssets   []string `json:"quote_assets"`   // ["USDT", "USDC", "BTC", "ETH"]
	WorkerLimit   int      `json:"worker_limit"`   // 동시 요청 수 제한
	ClickhouseDSN string   `json:"clickhouse_dsn"` // ClickHouse 연결 정보
}

// BinanceSymbol은 바이낸스 ExchangeInfo API 응답 중 심볼 정보를 파싱하기 위한 구조체입니다.
type BinanceSymbol struct {
	Symbol     string `json:"symbol"`
	Status     string `json:"status"`
	QuoteAsset string `json:"quoteAsset"`
}

// ExchangeInfoResponse는 ExchangeInfo API의 전체 응답을 감싸는 구조체입니다.
type ExchangeInfoResponse struct {
	Symbols []BinanceSymbol `json:"symbols"`
}

// Kline은 바이낸스 캔들(Kline) 데이터를 담는 구조체입니다.
// API 응답은 배열 형태이므로, 이 구조체에 맞게 파싱하여 사용합니다.
type Kline struct {
	OpenTime         int64   // 캔들 시작 시간 (ms)
	Open             float64 // 시가
	High             float64 // 고가
	Low              float64 // 저가
	Close            float64 // 종가
	Volume           float64 // 거래량
	CloseTime        int64   // 캔들 마감 시간 (ms)
	QuoteAssetVolume float64 // 거래대금
	NumberOfTrades   int     // 거래 횟수
}

// Alert는 분석 엔진이 생성하는 신호 정보를 담는 구조체입니다.
// 이 정보가 터미널 UI에 표시됩니다.
type Alert struct {
	Symbol    string
	Timeframe string
	Type      string    // 예: "MA 돌파", "EMA 이탈"
	Period    int       // 이동평균 기간 (7, 25, 100, 200)
	Price     float64   // 신호 발생 시점의 가격
	Volume    float64   // 정렬 기준이 되는 24시간 거래대금
	Timestamp time.Time // 신호 발생 시간
}

// ============== 프로그램 시작점 ==============

func main() {
	// --- 초기 설정 ---
	fmt.Println("바이낸스 HFT 스크리너를 시작합니다...")
	// 모든 고루틴의 생명주기를 관리하고, 우아한 종료를 가능하게 하는 context 생성
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // main 함수가 끝나기 직전 모든 고루틴에 종료 신호를 보냄

	// 설정 파일(config.json) 로드
	config, err := loadConfig("config.json")
	if err != nil {
		fmt.Printf("치명적 오류: 설정 파일 로드 실패: %v\n", err)
		return
	}

	// --- 1단계: 동적 심볼 탐색 ---
	// 바이낸스 API(목업)를 통해 설정에 맞는 심볼 목록을 필터링하여 가져옴
	filteredSymbols, err := fetchAndFilterSymbols(config)
	if err != nil {
		fmt.Printf("치명적 오류: 심볼 탐색 실패: %v\n", err)
		return
	}

	// --- 2단계: 데이터베이스 연동 (목업) ---
	// ClickHouse DB 연결을 설정하고, Batch Insert를 위한 버퍼 관리 시작
	db, err := NewDatabase(config.ClickhouseDSN)
	if err != nil {
		fmt.Printf("치명적 오류: 데이터베이스 초기화 실패: %v\n", err)
		return
	}
	defer db.Close() // 프로그램 종료 시 남은 버퍼 flush 및 연결 종료

	// --- 3단계 (파트1): 초기 데이터 로딩 ---
	// 워커풀을 사용하여 대량의 과거 캔들 데이터를 병렬로 가져와 메모리에 적재
	candleStore := loadInitialData(config, filteredSymbols, db)

	// --- 4단계: 분석 엔진 및 채널 설정 ---
	alertsChan := make(chan Alert, 100) // 분석 신호를 UI로 전달하는 중앙 통로
	engine := NewAnalysisEngine()

	// --- 3단계 (파트2): 실시간 데이터 스트리밍 ---
	// 목업 WebSocket을 실행하여 실시간 가격 변동을 시뮬레이션하고,
	// 가격이 변할 때마다 분석 엔진을 즉시 호출
	startMockWebSocketStream(candleStore, filteredSymbols, db, engine, alertsChan)

	// --- 5단계: 터미널 UI 실행 ---
	// 별도의 고루틴에서 UI를 실행하여 1초마다 화면을 갱신
	ui := NewTerminalUI()
	go ui.Run(ctx, alertsChan)

	// --- 6단계: 우아한 종료 대기 ---
	// OS의 종료 시그널(Ctrl+C 등)을 감지할 때까지 프로그램을 계속 실행
	waitForShutdownSignal(cancel)
	fmt.Println("\n프로그램이 성공적으로 종료되었습니다.")
}

// ============== 핵심 기능별 함수 구현 ==============

// --- 시스템 및 설정 관련 함수 ---

// loadInitialData는 워커풀을 생성하고 작업을 분배하여 초기 캔들 데이터 로딩을 총괄합니다.
func loadInitialData(config *Config, symbols []string, db *Database) *CandleStore {
	jobs := make(chan Job, len(symbols)*len(Timeframes))
	candleStore := &CandleStore{data: make(map[string][]*Kline)}
	var wg sync.WaitGroup

	fmt.Printf("%d개의 워커를 생성하여 초기 데이터 로딩을 시작합니다...\n", config.WorkerLimit)
	for i := 0; i < config.WorkerLimit; i++ {
		wg.Add(1)
		go worker(&wg, jobs, candleStore, db)
	}

	// 모든 심볼과 타임프레임 조합에 대한 작업을 채널에 추가
	for _, symbol := range symbols {
		for _, tf := range Timeframes {
			jobs <- Job{Symbol: symbol, Timeframe: tf}
		}
	}
	close(jobs) // 작업 추가가 끝났음을 워커들에게 알림

	wg.Wait() // 모든 워커가 작업을 마칠 때까지 대기
	fmt.Println("모든 초기 캔들 데이터 로딩이 완료되었습니다.")
	fmt.Printf("메모리에 %d개의 '심볼-타임프레임' 조합이 저장되었습니다.\n", len(candleStore.data))
	return candleStore
}

// waitForShutdownSignal은 OS 종료 시그널(Ctrl+C)을 감지하여 프로그램을 우아하게 종료시킵니다.
func waitForShutdownSignal(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	// SIGINT(Ctrl+C)와 SIGTERM(프로세스 종료) 시그널을 감지
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan // 시그널이 들어올 때까지 여기서 대기
	fmt.Println("\n종료 시그널을 받았습니다. 리소스를 정리합니다...")
	cancel()               // Context를 취소하여 모든 고루틴에 종료를 알림
	time.Sleep(1 * time.Second) // DB 버퍼 flush 등 정리 작업을 위한 시간 확보
}

// loadConfig는 지정된 경로의 JSON 설정 파일을 읽어 Config 구조체로 파싱합니다.
func loadConfig(path string) (*Config, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("'%s' 파일을 열 수 없습니다: %w", path, err)
	}

	var config Config
	if err := json.Unmarshal(file, &config); err != nil {
		return nil, fmt.Errorf("'%s' 파일 파싱 실패: %w", path, err)
	}

	return &config, nil
}

// --- 1단계: 동적 심볼 탐색 ---

// fetchAndFilterSymbols는 바이낸스 API를 통해 심볼 목록을 가져와 설정에 따라 필터링합니다.
func fetchAndFilterSymbols(config *Config) ([]string, error) {
	fmt.Println("바이낸스에서 거래 가능한 심볼 목록을 가져오는 중...")
	var allSymbols []BinanceSymbol

	// 설정된 각 마켓(SPOT, FUTURE)에 대해 API 호출
	for _, market := range config.TargetMarkets {
		var symbols []BinanceSymbol
		var err error
		switch strings.ToUpper(market) {
		case "SPOT":
			symbols, err = fetchExchangeInfo("https://api3.binance.com/api/v3/exchangeInfo")
		case "FUTURE":
			symbols, err = fetchExchangeInfo("https://fapi3.binance.com/fapi/v1/exchangeInfo")
		default:
			fmt.Printf("경고: 알 수 없는 마켓 타입 '%s'는 건너뜁니다.\n", market)
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("%s 마켓 정보 조회 실패: %w", market, err)
		}
		allSymbols = append(allSymbols, symbols...)
	}

	fmt.Printf("총 %d개의 심볼을 가져왔습니다. 필터링을 시작합니다...\n", len(allSymbols))

	// 필터링 로직: 거래 중(TRADING)이며, QuoteAsset이 설정과 일치하는 심볼만 선택
	var filteredSymbols []string
	quoteAssetSet := make(map[string]struct{}, len(config.QuoteAssets))
	for _, asset := range config.QuoteAssets {
		quoteAssetSet[asset] = struct{}{}
	}

	for _, s := range allSymbols {
		if s.Status != "TRADING" {
			continue
		}
		if _, ok := quoteAssetSet[s.QuoteAsset]; ok {
			filteredSymbols = append(filteredSymbols, s.Symbol)
		}
	}

	fmt.Printf("필터링 완료. %d개의 심볼이 최종 선택되었습니다.\n", len(filteredSymbols))
	return filteredSymbols, nil
}

// fetchExchangeInfo는 거래소 정보를 가져옵니다. (현재 목업 데이터 반환)
func fetchExchangeInfo(apiURL string) ([]BinanceSymbol, error) {
	fmt.Printf("주의: 목업 데이터를 사용합니다. (%s)\n", apiURL)
	// (API 차단 문제로 목업 데이터 사용, 실제 환경에서는 http.Get 로직으로 복원 필요)
	if strings.Contains(apiURL, "fapi") {
		return []BinanceSymbol{
			{Symbol: "BTCUSDT", Status: "TRADING", QuoteAsset: "USDT"},
			{Symbol: "ETHUSDT", Status: "TRADING", QuoteAsset: "USDT"},
			{Symbol: "ADAUSDT_PERP", Status: "TRADING", QuoteAsset: "USDT"},
		}, nil
	}
	return []BinanceSymbol{
		{Symbol: "BTCUSDT", Status: "TRADING", QuoteAsset: "USDT"},
		{Symbol: "ETHUSDT", Status: "TRADING", QuoteAsset: "USDT"},
		{Symbol: "ADABTC", Status: "TRADING", QuoteAsset: "BTC"},
		{Symbol: "LTCUSDC", Status: "TRADING", QuoteAsset: "USDC"},
		{Symbol: "BNBEUR", Status: "TRADING", QuoteAsset: "EUR"},
		{Symbol: "LUNAUSDT", Status: "BREAK", QuoteAsset: "USDT"},
	}, nil
}

// --- 3단계: 데이터 수집 파이프라인 ---

// startMockWebSocketStream은 실시간 가격 변동과 신규 캔들 생성을 시뮬레이션합니다.
func startMockWebSocketStream(store *CandleStore, symbols []string, db *Database, engine *AnalysisEngine, alertsChan chan<- Alert) {
	fmt.Println("목업 WebSocket 스트림을 시작합니다...")
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond) // 0.5초마다 가격 변동
		defer ticker.Stop()
		candleCreationCounter := 0

		for range ticker.C {
			candleCreationCounter++
			var (
				randSymbol, randTF string
				lastCandleCopy     Kline
				analysisNeeded     bool
			)

			store.Lock()
			if len(symbols) > 0 {
				randIndex := rand.Intn(len(symbols))
				randSymbol = symbols[randIndex]
				randTF = Timeframes[rand.Intn(len(Timeframes))]
				key := fmt.Sprintf("%s-%s", randSymbol, randTF)

				if candles, ok := store.data[key]; ok && len(candles) > 0 {
					lastCandle := candles[len(candles)-1]
					lastCandle.Close += (rand.Float64() - 0.5) * 5.0 // 가격 크게 변동시켜 테스트
					if lastCandle.Close > lastCandle.High { lastCandle.High = lastCandle.Close }
					if lastCandle.Close < lastCandle.Low { lastCandle.Low = lastCandle.Close }
					lastCandleCopy = *lastCandle
					analysisNeeded = true

					// 약 5초마다 새 캔들 생성 시뮬레이션
					if candleCreationCounter%10 == 0 {
						newCandle := &Kline{ OpenTime: lastCandle.OpenTime + 60000, Open: lastCandle.Close, High: lastCandle.Close, Low: lastCandle.Close, Close: lastCandle.Close, Volume: rand.Float64() * 100 }
						store.data[key] = append(candles[1:], newCandle) // 1000개 유지
					}
				}
			}
			store.Unlock()

			// Lock 해제 후 DB 저장 및 분석 실행 (교착 상태 방지)
			if analysisNeeded {
				db.AddToBatch(&lastCandleCopy)
				engine.Run(store, randSymbol, randTF, &lastCandleCopy, alertsChan)
			}
		}
	}()
}

// worker는 워커풀의 일꾼으로, 할당된 Job(초기 캔들 데이터 로딩)을 수행합니다.
func worker(wg *sync.WaitGroup, jobs <-chan Job, store *CandleStore, db *Database) {
	defer wg.Done()
	for job := range jobs {
		candles, err := fetchInitialCandles(job.Symbol, job.Timeframe)
		if err != nil {
			fmt.Printf("경고: %s-%s 캔들 데이터 로딩 실패: %v\n", job.Symbol, job.Timeframe, err)
			continue
		}
		for _, kline := range candles {
			db.AddToBatch(kline)
		}
		key := fmt.Sprintf("%s-%s", job.Symbol, job.Timeframe)
		store.Lock()
		store.data[key] = candles
		store.Unlock()
	}
}

// fetchInitialCandles는 목업용 과거 캔들 데이터 1000개를 생성합니다.
func fetchInitialCandles(symbol, timeframe string) ([]*Kline, error) {
	candles := make([]*Kline, 1000)
	lastClose := 100.0 + (rand.Float64() * 50)
	now := time.Now().UnixMilli()
	for i := 0; i < 1000; i++ {
		change := (rand.Float64() - 0.5) * 2
		open := lastClose
		close := open + change
		candles[i] = &Kline{ OpenTime: now - int64(1000-i)*60000, Open: open, High: open, Low: close, Close: close, Volume: rand.Float64() * 1000 }
		if close > open { candles[i].High = close; candles[i].Low = open }
		lastClose = close
	}
	return candles, nil
}

// --- 4단계: 분석 엔진 ---

// IndicatorPeriods는 분석에 사용할 이동평균 기간 목록입니다.
var IndicatorPeriods = []int{7, 25, 100, 200}

// AnalysisEngine은 실시간 분석 로직과 중복 알림 방지 상태를 관리합니다.
type AnalysisEngine struct {
	sync.Mutex
	lastAlertTimes map[string]int64 // Key: "SYMBOL-TIMEFRAME-TYPE-PERIOD"
}

// NewAnalysisEngine은 새 분석 엔진 인스턴스를 생성합니다.
func NewAnalysisEngine() *AnalysisEngine {
	return &AnalysisEngine{lastAlertTimes: make(map[string]int64)}
}

// Run은 분석 엔진의 메인 로직으로, 가격 변동 시마다 호출됩니다.
func (e *AnalysisEngine) Run(store *CandleStore, symbol, timeframe string, lastCandle *Kline, alertsChan chan<- Alert) {
	key := fmt.Sprintf("%s-%s", symbol, timeframe)
	store.RLock()
	candles, ok := store.data[key]
	store.RUnlock()
	if !ok || len(candles) < 2 {
		return
	}

	prevCandle := candles[len(candles)-2]
	currentPrice := lastCandle.Close

	for _, period := range IndicatorPeriods {
		// MA, EMA 계산 및 조건 확인
		if len(candles) >= period {
			ma := calculateMA(candles, period)
			e.checkCondition(symbol, timeframe, "MA", period, ma, prevCandle, currentPrice, lastCandle.OpenTime, alertsChan)
		}
		if len(candles) >= period*2 { // EMA는 더 많은 데이터 필요
			ema := calculateEMA(candles, period)
			e.checkCondition(symbol, timeframe, "EMA", period, ema, prevCandle, currentPrice, lastCandle.OpenTime, alertsChan)
		}
	}
}

// checkCondition은 돌파/이탈 조건을 확인하고, 중복이 아닐 경우 알림을 채널로 보냅니다.
func (e *AnalysisEngine) checkCondition(symbol, timeframe, indicatorType string, period int, indicatorValue float64, prevCandle *Kline, currentPrice float64, currentCandleOpenTime int64, alertsChan chan<- Alert) {
	alertKey := fmt.Sprintf("%s-%s-%s-%d", symbol, timeframe, indicatorType, period)

	// 돌파 조건: 직전 캔들 종가 < 지표값 AND 현재가 > 지표값
	isBreakout := prevCandle.Close < indicatorValue && currentPrice > indicatorValue
	// 이탈 조건: 직전 캔들 종가 > 지표값 AND 현재가 < 지표값
	isBreakdown := prevCandle.Close > indicatorValue && currentPrice < indicatorValue

	if isBreakout || isBreakdown {
		e.Lock()
		lastAlertTime, ok := e.lastAlertTimes[alertKey]
		// 동일 캔들 내에서 중복 알림이 발생하지 않도록 체크
		if !ok || lastAlertTime != currentCandleOpenTime {
			e.lastAlertTimes[alertKey] = currentCandleOpenTime
			alertType := fmt.Sprintf("%s 이탈", indicatorType)
			if isBreakout {
				alertType = fmt.Sprintf("%s 돌파", indicatorType)
			}
			alertsChan <- Alert{
				Symbol: symbol, Timeframe: timeframe, Type: alertType, Period: period,
				Price: currentPrice, Volume: prevCandle.QuoteAssetVolume, Timestamp: time.Now(),
			}
		}
		e.Unlock()
	}
}

// calculateMA는 단순이동평균(SMA)을 계산합니다.
func calculateMA(candles []*Kline, period int) float64 {
	sum := 0.0
	for i := len(candles) - period; i < len(candles); i++ {
		sum += candles[i].Close
	}
	return sum / float64(period)
}

// calculateEMA는 지수이동평균(EMA)을 표준 방식에 따라 계산합니다.
func calculateEMA(candles []*Kline, period int) float64 {
	relevantCandles := candles[len(candles)-(period*2):]
	multiplier := 2.0 / float64(period+1)

	// 첫 EMA 값은 SMA로 초기화
	sum := 0.0
	for i := 0; i < period; i++ { sum += relevantCandles[i].Close }
	ema := sum / float64(period)

	// 나머지 캔들에 대해 점화식을 순차적으로 적용
	for i := period; i < len(relevantCandles); i++ {
		ema = (relevantCandles[i].Close-ema)*multiplier + ema
	}
	return ema
}

// --- 5단계: 터미널 UI ---

const (
	ColorGreen = "\033[32m"; ColorRed = "\033[31m"; ColorBold = "\033[1m"; ColorReset = "\033[0m"; Italic = "\033[3m"; Clear = "\033[H\033[2J"
)

// TerminalUI는 터미널 출력을 관리합니다.
type TerminalUI struct {
	alerts     []Alert
	alertsMux  sync.Mutex
	lastSymbol string
}

// NewTerminalUI는 새 UI 인스턴스를 생성합니다.
func NewTerminalUI() *TerminalUI {
	return &TerminalUI{alerts: make([]Alert, 0, 100)}
}

// Run은 1초마다 화면을 갱신하는 UI 메인 루프입니다.
func (ui *TerminalUI) Run(ctx context.Context, alertsChan <-chan Alert) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case alert := <-alertsChan:
			ui.addAlert(alert)
		case <-ticker.C:
			ui.display()
		case <-ctx.Done(): // 프로그램 종료 신호 감지
			return
		}
	}
}

// addAlert는 수신된 알림을 버퍼에 안전하게 추가합니다.
func (ui *TerminalUI) addAlert(alert Alert) {
	ui.alertsMux.Lock()
	defer ui.alertsMux.Unlock()
	ui.alerts = append(ui.alerts, alert)
}

// display는 수집된 알림을 정렬하여 터미널에 출력합니다.
func (ui *TerminalUI) display() {
	ui.alertsMux.Lock()
	defer ui.alertsMux.Unlock()

	// 정렬 기준: 1.거래대금(내림차순) -> 2.타임프레임 -> 3.지표타입 -> 4.기간
	sort.Slice(ui.alerts, func(i, j int) bool {
		if ui.alerts[i].Volume != ui.alerts[j].Volume { return ui.alerts[i].Volume > ui.alerts[j].Volume }
		if ui.alerts[i].Timeframe != ui.alerts[j].Timeframe { return ui.alerts[i].Timeframe < ui.alerts[j].Timeframe }
		if ui.alerts[i].Type != ui.alerts[j].Type { return ui.alerts[i].Type < ui.alerts[j].Type }
		return ui.alerts[i].Period < ui.alerts[j].Period
	})

	fmt.Print(Clear)
	fmt.Printf("%s[바이낸스 HFT 스크리너] | 현재 시간: %s%s\n\n", ColorBold, time.Now().Format("2006-01-02 15:04:05"), ColorReset)

	limit := 20
	if len(ui.alerts) < limit { limit = len(ui.alerts) }

	ui.lastSymbol = ""
	for i := 0; i < limit; i++ {
		alert := ui.alerts[i]
		color := ColorRed
		if strings.Contains(alert.Type, "돌파") { color = ColorGreen }
		symbolStyle := ""
		if alert.Symbol == ui.lastSymbol { symbolStyle = Italic }

		fmt.Printf("%s%s[%-8s]%s %-10s | %-12s (%3d) | Price: %12.4f | Volume: %14.2f\n",
			color, symbolStyle, alert.Timeframe, ColorReset, alert.Symbol,
			alert.Type, alert.Period, alert.Price, alert.Volume)
		ui.lastSymbol = alert.Symbol
	}

	ui.alerts = ui.alerts[:0] // 화면에 표시된 알림은 비워서 다음 주기에 새로운 알림만 표시
}

// --- 6단계: 데이터베이스 연동 (목업) ---

// Database는 ClickHouse 연결 및 데이터 저장을 관리합니다.
type Database struct {
	buffer    []*Kline
	bufferMux sync.Mutex
	ticker    *time.Ticker
	batchSize int
}

// NewDatabase는 DB 인스턴스를 생성하고 주기적인 버퍼 flush를 시작합니다.
func NewDatabase(dsn string) (*Database, error) {
	fmt.Println("데이터베이스 연결을 설정합니다 (목업)...")
	db := &Database{
		buffer:    make([]*Kline, 0, 1000),
		batchSize: 1000, // 버퍼가 1000개 차면 flush
		ticker:    time.NewTicker(5 * time.Second), // 또는 5초마다 flush
	}
	go db.flushBufferPeriodically()
	fmt.Println("데이터베이스 (목업)가 준비되었습니다.")
	return db, nil
}

// AddToBatch는 캔들 데이터를 버퍼에 추가하고, 버퍼가 가득 차면 flush를 호출합니다.
func (db *Database) AddToBatch(kline *Kline) {
	db.bufferMux.Lock()
	defer db.bufferMux.Unlock()
	db.buffer = append(db.buffer, kline)
	if len(db.buffer) >= db.batchSize {
		db.flushBuffer()
	}
}

// flushBufferPeriodically는 ticker 주기에 맞춰 버퍼를 flush합니다.
func (db *Database) flushBufferPeriodically() {
	for range db.ticker.C {
		db.bufferMux.Lock()
		db.flushBuffer()
		db.bufferMux.Unlock()
	}
}

// flushBuffer는 버퍼의 데이터를 DB에 저장하는 로직을 수행합니다. (현재는 로그만 출력)
func (db *Database) flushBuffer() {
	if len(db.buffer) == 0 { return }
	// fmt.Printf("[DB 목업] %d개의 캔들 데이터를 ClickHouse에 Batch Insert합니다.\n", len(db.buffer))
	db.buffer = db.buffer[:0] // 버퍼 비우기
}

// Close는 프로그램을 종료하기 직전, 남은 데이터를 모두 flush합니다.
func (db *Database) Close() {
	fmt.Println("데이터베이스 연결을 닫고 남은 데이터를 모두 저장합니다...")
	db.ticker.Stop()
	db.bufferMux.Lock()
	db.flushBuffer()
	db.bufferMux.Unlock()
	fmt.Println("DB 버퍼 처리가 완료되었습니다.")
}
