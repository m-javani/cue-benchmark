// Copyright 2026 M. Javani
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/m-javani/cue-proxy/pkg"
	"go.uber.org/zap"
)

// ─── Configuration ───────────────────────────────────────────────────────────
type Config struct {
	ProxyURL      string
	ProxyToken    string
	ConsumerToken string
	NumTopics     int
	NumJobs       int
	NumConsumers  int
	PayloadSize   int
	BatchSize     int
	Timeout       time.Duration
	Insecure      bool
	LogLevel      string
	WorkerPool    int
}

// ─── Benchmark Metrics ───────────────────────────────────────────────────────
type Metrics struct {
	mu              sync.RWMutex
	ingestionStart  time.Time
	ingestionEnd    time.Time
	dispatchStart   time.Time
	dispatchEnd     time.Time
	jobsAccepted    int64
	jobsReceived    atomic.Int64
	errors          atomic.Int64
	httpErrors      atomic.Int64
	websocketErrors atomic.Int64
	consumerJobs    map[int]int64
	consumerMu      sync.RWMutex
}

func NewMetrics() *Metrics {
	return &Metrics{
		consumerJobs: make(map[int]int64),
	}
}

func (m *Metrics) RecordSend() { atomic.AddInt64(&m.jobsAccepted, 1) }

func (m *Metrics) RecordReceive(consumerID int) {
	m.jobsReceived.Add(1)
	m.consumerMu.Lock()
	m.consumerJobs[consumerID]++
	m.consumerMu.Unlock()
}

func (m *Metrics) RecordError()          { m.errors.Add(1) }
func (m *Metrics) RecordHTTPError()      { m.httpErrors.Add(1) }
func (m *Metrics) RecordWebSocketError() { m.websocketErrors.Add(1) }

func (m *Metrics) SetIngestionStart(t time.Time) {
	m.mu.Lock()
	m.ingestionStart = t
	m.mu.Unlock()
}

func (m *Metrics) SetIngestionEnd(t time.Time) {
	m.mu.Lock()
	m.ingestionEnd = t
	m.mu.Unlock()
}

func (m *Metrics) SetDispatchStart(t time.Time) {
	m.mu.Lock()
	m.dispatchStart = t
	m.mu.Unlock()
}

func (m *Metrics) SetDispatchEnd(t time.Time) {
	m.mu.Lock()
	m.dispatchEnd = t
	m.mu.Unlock()
}

// ─── Producer ────────────────────────────────────────────────────────────────
type Producer struct {
	client  *http.Client
	baseURL string
	token   string
	metrics *Metrics
	logger  *zap.Logger
}

func NewProducer(baseURL, token string, metrics *Metrics, insecure bool, logger *zap.Logger) *Producer {
	transport := &http.Transport{MaxIdleConns: 100, IdleConnTimeout: 90 * time.Second}
	if insecure {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &Producer{
		client:  &http.Client{Timeout: 10 * time.Second, Transport: transport},
		baseURL: baseURL,
		token:   token,
		metrics: metrics,
		logger:  logger,
	}
}

type JobRequest struct {
	Job struct {
		ID    string `json:"id"`
		Topic string `json:"topic"`
		Data  string `json:"data"`
	} `json:"job"`
}

type BatchJob struct {
	ID    string `json:"id"`
	Topic string `json:"topic"`
	Data  string `json:"data"`
}

type BatchJobRequest struct {
	Topic string     `json:"topic"`
	Jobs  []BatchJob `json:"jobs"`
}

func (p *Producer) SendJob(jobID, topic string, data []byte) error {
	reqBody := JobRequest{}
	reqBody.Job.ID = jobID
	reqBody.Job.Topic = topic
	reqBody.Job.Data = base64.StdEncoding.EncodeToString(data)
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", p.baseURL+"/producer/jobs", bytes.NewReader(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		p.metrics.RecordHTTPError()
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.metrics.RecordHTTPError()
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	p.metrics.RecordSend()
	return nil
}

func (p *Producer) SendBatchJobs(topic string, jobs []BatchJob) error {
	reqBody := BatchJobRequest{
		Topic: topic,
		Jobs:  jobs,
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", p.baseURL+"/producer/jobs", bytes.NewReader(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		p.metrics.RecordHTTPError()
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.metrics.RecordHTTPError()
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	for range jobs {
		p.metrics.RecordSend()
	}
	return nil
}

func (p *Producer) AddTopic(topic string) error {
	body, err := json.Marshal(map[string]string{"topic": topic})
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", p.baseURL+"/producer/topic", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		// return fmt.Errorf("add topic failed: %s", resp.Status)
	}
	return nil
}

// ─── Consumer ────────────────────────────────────────────────────────────────
type Consumer struct {
	id       int
	uuid     string
	topic    string
	conn     *websocket.Conn
	connMu   sync.Mutex
	metrics  *Metrics
	logger   *zap.Logger
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	wsURL    string
	insecure bool
	ready    chan struct{}
}

func NewConsumer(id int, topic, wsURL string, metrics *Metrics, insecure bool, logger *zap.Logger) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Consumer{
		id:       id,
		uuid:     fmt.Sprintf("bench-c%d-%d", id, time.Now().UnixNano()%1000000),
		topic:    topic,
		metrics:  metrics,
		logger:   logger,
		ctx:      ctx,
		cancel:   cancel,
		wsURL:    wsURL,
		insecure: insecure,
		ready:    make(chan struct{}, 1),
	}
}

func (c *Consumer) Start() {
	c.wg.Add(1)
	go c.run()
}

func (c *Consumer) Stop() {
	c.cancel()
	c.wg.Wait()
	c.connMu.Lock()
	if c.conn != nil {
		_ = c.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		c.conn.Close()
		c.conn = nil
	}
	c.connMu.Unlock()
}

func (c *Consumer) run() {
	defer c.wg.Done()
	var err error
	for retries := range 5 {
		c.conn, _, err = c.connect()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(retries+1) * 150 * time.Millisecond)
	}
	if err != nil {
		c.logger.Error("consumer failed to connect", zap.Int("id", c.id), zap.Error(err))
		c.metrics.RecordWebSocketError()
		return
	}

	initMsg := pkg.WebSocketMessage{Action: "init", UUID: c.uuid, Topic: c.topic}
	if err := c.conn.WriteJSON(initMsg); err != nil {
		c.logger.Error("init failed", zap.Int("id", c.id), zap.Error(err))
		c.metrics.RecordWebSocketError()
		return
	}
	close(c.ready) // Signal that init was sent

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		var delivery pkg.ToConsumerDelivery
		if err := c.conn.ReadJSON(&delivery); err != nil {
			return
		}
		if delivery.Action != "job" {
			continue
		}
		c.metrics.RecordReceive(c.id)

		ackMsg := pkg.WebSocketMessage{
			Action: "ack",
			LastID: delivery.SeqID,
			JobID:  delivery.JobID,
			Topic:  delivery.Topic,
		}
		if err := c.conn.WriteJSON(ackMsg); err != nil {
			c.metrics.RecordWebSocketError()
		}
	}
}

func (c *Consumer) connect() (*websocket.Conn, *http.Response, error) {
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	if c.insecure {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return dialer.Dial(c.wsURL, nil)
}

// ─── Benchmark ───────────────────────────────────────────────────────────────
type Benchmark struct {
	config    Config
	metrics   *Metrics
	producer  *Producer
	consumers []*Consumer
	logger    *zap.Logger
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewBenchmark(config Config) (*Benchmark, error) {
	var logger *zap.Logger
	if config.LogLevel == "debug" {
		logger, _ = zap.NewDevelopment()
	} else {
		logger, _ = zap.NewProduction()
	}

	metrics := NewMetrics()
	ctx, cancel := context.WithCancel(context.Background())

	b := &Benchmark{
		config:  config,
		metrics: metrics,
		logger:  logger,
		ctx:     ctx,
		cancel:  cancel,
	}
	b.producer = NewProducer(config.ProxyURL, config.ProxyToken, metrics, config.Insecure, logger)

	wsURL := "ws://" + strings.TrimPrefix(strings.TrimPrefix(config.ProxyURL, "http://"), "https://") + "/ws?token=" + config.ConsumerToken
	for i := 0; i < config.NumConsumers; i++ {
		topicIndex := i % config.NumTopics
		topic := b.getTopicName(topicIndex)
		c := NewConsumer(i, topic, wsURL, metrics, config.Insecure, logger)
		b.consumers = append(b.consumers, c)
	}
	return b, nil
}

// jobWork represents a single job to be processed
type jobWork struct {
	jobID      int
	topicIndex int
}

func (b *Benchmark) Run() error {
	// Create all topics
	for i := 0; i < b.config.NumTopics; i++ {
		topic := b.getTopicName(i)
		_ = b.producer.AddTopic(topic)
	}

	b.metrics.SetIngestionStart(time.Now())

	padding := strings.Repeat("x", b.config.PayloadSize)

	// If batching is disabled (batch=1), use the old fast path
	if b.config.BatchSize <= 1 {
		return b.runSingleJobMode(padding)
	}

	return b.runBatchMode(padding)
}

// runSingleJobMode: original behavior, one job per HTTP request
func (b *Benchmark) runSingleJobMode(padding string) error {
	var sendWg sync.WaitGroup
	jobs := make(chan jobWork, b.config.WorkerPool*2)

	for w := 0; w < b.config.WorkerPool; w++ {
		sendWg.Add(1)
		go func() {
			defer sendWg.Done()
			buf := make([]byte, 0, b.config.PayloadSize+128)
			for job := range jobs {
				buf = buf[:0]
				topic := b.getTopicName(job.topicIndex)
				id := fmt.Sprintf("bench-t%d-j%d-%d", job.topicIndex, job.jobID, time.Now().UnixNano())

				buf = append(buf, `{"pad":"`...)
				paddingNeeded := b.config.PayloadSize - len(buf) - 2
				if paddingNeeded > 0 {
					if paddingNeeded > len(padding) {
						paddingNeeded = len(padding)
					}
					buf = append(buf, padding[:paddingNeeded]...)
				}
				buf = append(buf, `"}`...)

				if err := b.producer.SendJob(id, topic, buf); err != nil {
					b.logger.Warn("send failed", zap.Error(err), zap.String("topic", topic))
					b.metrics.RecordError()
				}
			}
		}()
	}

	for i := 0; i < b.config.NumJobs; i++ {
		topicIndex := i % b.config.NumTopics
		jobs <- jobWork{jobID: i, topicIndex: topicIndex}
	}
	close(jobs)
	sendWg.Wait()

	return b.finishIngestion()
}

// runBatchMode: coordinated batching per topic
func (b *Benchmark) runBatchMode(padding string) error {
	// One batch builder per topic
	type topicBatch struct {
		topicIndex int
		jobs       []BatchJob
		mu         sync.Mutex
	}

	batches := make([]*topicBatch, b.config.NumTopics)
	for i := range batches {
		batches[i] = &topicBatch{
			topicIndex: i,
			jobs:       make([]BatchJob, 0, b.config.BatchSize),
		}
	}

	// Channel for workers to submit jobs to batchers
	// We use a single channel and workers hash by topic
	jobCh := make(chan jobWork, b.config.WorkerPool*4)

	// Workers: build jobs and send to batchers
	var workerWg sync.WaitGroup
	for w := 0; w < b.config.WorkerPool; w++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			buf := make([]byte, 0, b.config.PayloadSize+128)

			for job := range jobCh {
				buf = buf[:0]
				topic := b.getTopicName(job.topicIndex)
				id := fmt.Sprintf("bench-t%d-j%d-%d", job.topicIndex, job.jobID, time.Now().UnixNano())

				buf = append(buf, `{"pad":"`...)
				paddingNeeded := b.config.PayloadSize - len(buf) - 2
				if paddingNeeded > 0 {
					if paddingNeeded > len(padding) {
						paddingNeeded = len(padding)
					}
					buf = append(buf, padding[:paddingNeeded]...)
				}
				buf = append(buf, `"}`...)

				batchJob := BatchJob{
					ID:    id,
					Topic: topic,
					Data:  base64.StdEncoding.EncodeToString(buf),
				}

				// Add to topic batch
				tb := batches[job.topicIndex]
				tb.mu.Lock()
				tb.jobs = append(tb.jobs, batchJob)

				// Flush if full
				if len(tb.jobs) >= b.config.BatchSize {
					jobsCopy := make([]BatchJob, len(tb.jobs))
					copy(jobsCopy, tb.jobs)
					tb.jobs = tb.jobs[:0]
					tb.mu.Unlock()

					if err := b.producer.SendBatchJobs(topic, jobsCopy); err != nil {
						b.logger.Warn("batch send failed", zap.Error(err), zap.String("topic", topic), zap.Int("batchSize", len(jobsCopy)))
						b.metrics.RecordError()
					}
				} else {
					tb.mu.Unlock()
				}
			}
		}()
	}

	// Feed jobs
	for i := 0; i < b.config.NumJobs; i++ {
		topicIndex := i % b.config.NumTopics
		jobCh <- jobWork{jobID: i, topicIndex: topicIndex}
	}
	close(jobCh)
	workerWg.Wait()

	// Flush remaining jobs from all topics
	var flushWg sync.WaitGroup
	for i, tb := range batches {
		flushWg.Add(1)
		go func(tb *topicBatch, topicIndex int) {
			defer flushWg.Done()
			tb.mu.Lock()
			if len(tb.jobs) > 0 {
				jobsCopy := make([]BatchJob, len(tb.jobs))
				copy(jobsCopy, tb.jobs)
				tb.jobs = tb.jobs[:0]
				tb.mu.Unlock()

				topic := b.getTopicName(topicIndex)
				if err := b.producer.SendBatchJobs(topic, jobsCopy); err != nil {
					b.logger.Warn("final batch send failed", zap.Error(err), zap.String("topic", topic), zap.Int("batchSize", len(jobsCopy)))
					b.metrics.RecordError()
				}
			} else {
				tb.mu.Unlock()
			}
		}(tb, i)
	}
	flushWg.Wait()

	return b.finishIngestion()
}

func (b *Benchmark) finishIngestion() error {
	ingestionEnd := time.Now()
	b.metrics.SetIngestionEnd(ingestionEnd)

	sent := atomic.LoadInt64(&b.metrics.jobsAccepted)
	fmt.Printf("\nPhase 1 Complete\n")
	fmt.Printf("Jobs Accepted: %d\n", sent)
	fmt.Printf("Ingestion Time: %v\n", ingestionEnd.Sub(b.metrics.ingestionStart))
	if sent > 0 {
		fmt.Printf("Ingestion Throughput: %.2f jobs/sec\n", float64(sent)/ingestionEnd.Sub(b.metrics.ingestionStart).Seconds())
	}
	fmt.Println()

	// Phase 2: Dispatch
	for _, c := range b.consumers {
		c.Start()
	}

	// Wait for ALL consumers to be fully ready
	readyTimeout := time.After(10 * time.Second)
	readyCount := 0
	for readyCount < len(b.consumers) {
		select {
		case <-readyTimeout:
			b.logger.Warn("not all consumers ready in time", zap.Int("ready", readyCount))
			goto waitForJobs
		case <-b.consumers[readyCount].ready:
			readyCount++
		}
	}

waitForJobs:
	// Pure dispatch measurement starts here (after connection + init overhead)
	b.metrics.SetDispatchStart(time.Now())

	// Wait for all jobs to be delivered
	deadline := time.Now().Add(b.config.Timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			received := b.metrics.jobsReceived.Load()
			sent := atomic.LoadInt64(&b.metrics.jobsAccepted)
			if received >= sent {
				b.metrics.SetDispatchEnd(time.Now())
				b.printReport()
				return nil
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout: %d/%d jobs received", received, sent)
			}
		case <-b.ctx.Done():
			return b.ctx.Err()
		}
	}
}

func (b *Benchmark) printReport() {
	m := b.metrics
	m.mu.RLock()
	ingestionDuration := m.ingestionEnd.Sub(m.ingestionStart)
	dispatchDuration := m.dispatchEnd.Sub(m.dispatchStart)
	sent := atomic.LoadInt64(&m.jobsAccepted)
	received := m.jobsReceived.Load()
	m.mu.RUnlock()

	fmt.Printf("\n%s\n", strings.Repeat("=", 50))
	fmt.Printf("PHASE 1 — INGESTION\n")
	fmt.Printf("Jobs Accepted : %d\n", sent)
	fmt.Printf("Duration      : %v\n", ingestionDuration)
	if ingestionDuration > 0 {
		fmt.Printf("Throughput    : %.2f jobs/sec\n", float64(sent)/ingestionDuration.Seconds())
	}
	fmt.Printf("%s\n", strings.Repeat("-", 50))
	fmt.Printf("PHASE 2 — DISPATCH\n")
	fmt.Printf("Jobs Delivered: %d\n", received)
	fmt.Printf("Duration      : %v\n", dispatchDuration)
	if dispatchDuration > 0 {
		fmt.Printf("Throughput    : %.2f jobs/sec\n", float64(received)/dispatchDuration.Seconds())
	}
	fmt.Printf("%s\n", strings.Repeat("-", 50))
	fmt.Printf("Errors          : %d\n", m.errors.Load())
	fmt.Printf("HTTP Errors     : %d\n", m.httpErrors.Load())
	fmt.Printf("WebSocket Errors: %d\n", m.websocketErrors.Load())
	fmt.Printf("%s\n", strings.Repeat("=", 50))
}

func (b *Benchmark) Stop() {
	b.cancel()
	for _, c := range b.consumers {
		c.Stop()
	}
	b.producer.client.CloseIdleConnections()
}

func (b *Benchmark) getTopicName(index int) string {
	if b.config.NumTopics <= 1 {
		return "bench-topic"
	}
	return fmt.Sprintf("bench-topic-%d", index)
}

// ─── Main ────────────────────────────────────────────────────────────────────
func main() {
	var config Config
	flag.StringVar(&config.ProxyURL, "proxy", "http://localhost:8080", "Proxy URL")
	flag.StringVar(&config.ProxyToken, "proxy-token", "admin_2024", "Proxy token")
	flag.StringVar(&config.ConsumerToken, "consumer-token", "admin_2024", "Consumer token")
	flag.IntVar(&config.NumTopics, "topics", 1, "Number of topics to create")
	flag.IntVar(&config.NumJobs, "jobs", 1000, "Number of jobs")
	flag.IntVar(&config.NumConsumers, "consumers", 1, "Number of consumers")
	flag.IntVar(&config.PayloadSize, "payload", 1024, "Payload size in bytes")
	flag.IntVar(&config.BatchSize, "batch", 1, "Number of jobs per batch request (1 = single job mode)")
	flag.DurationVar(&config.Timeout, "timeout", 60*time.Second, "Timeout")
	flag.BoolVar(&config.Insecure, "insecure", false, "Insecure TLS")
	flag.StringVar(&config.LogLevel, "log-level", "info", "Log level")
	flag.IntVar(&config.WorkerPool, "workers", 64, "Worker pool size")
	flag.Parse()

	if config.NumJobs <= 0 || config.NumConsumers <= 0 || config.PayloadSize <= 0 || config.BatchSize <= 0 {
		fmt.Println("Error: jobs, consumers, payload and batch must be > 0")
		os.Exit(1)
	}

	fmt.Printf("🚀 Starting Two-Phase Benchmark\n")
	fmt.Printf(" Proxy : %s\n", config.ProxyURL)
	fmt.Printf(" Topics : %d\n", config.NumTopics)
	fmt.Printf(" Jobs : %d\n", config.NumJobs)
	fmt.Printf(" Consumers : %d\n", config.NumConsumers)
	fmt.Printf(" Payload : %d bytes\n", config.PayloadSize)
	fmt.Printf(" Batch Size : %d jobs/request\n", config.BatchSize)
	fmt.Printf(" Workers : %d\n", config.WorkerPool)
	fmt.Println()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	bench, err := NewBenchmark(config)
	if err != nil {
		fmt.Printf("❌ %v\n", err)
		os.Exit(1)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- bench.Run() }()

	select {
	case err := <-errCh:
		if err != nil {
			fmt.Printf("❌ %v\n", err)
		}
	case <-sigCh:
		fmt.Println("\n⚠️ Interrupted")
	}

	bench.Stop()
}
