//+build integration

package main

import (
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"

	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
	"github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl/clientlibrary/metrics/prometheus"
	"github.com/vmware/vmware-go-kcl/clientlibrary/worker"
	"github.com/vmware/vmware-go-kcl/logger"
)

type IntegrationRecordConsumer struct {
	shardID          string
	processedRecords map[string]int
}

func (p *IntegrationRecordConsumer) Initialize(in *interfaces.InitializationInput) {
	p.shardID = in.ShardId
}

func (p *IntegrationRecordConsumer) ProcessRecords(in *interfaces.ProcessRecordsInput) {
	if len(in.Records) > 0 {
		for _, record := range in.Records {
			p.processedRecords[*record.SequenceNumber]++
		}

		// move checkpoint
		lastRecordSequenceNubmer := in.Records[len(in.Records)-1].SequenceNumber
		in.Checkpointer.Checkpoint(lastRecordSequenceNubmer)
	}
}

func (p *IntegrationRecordConsumer) Shutdown(in *interfaces.ShutdownInput) {}

type integrationRecordProcessorFactory struct {
	rc *IntegrationRecordConsumer
}

func (f integrationRecordProcessorFactory) CreateProcessor() interfaces.IRecordProcessor {
	return f.rc
}

// returns a record processor factory that returns rc.
func newIntegrationRecordProcessorFactory(rc *IntegrationRecordConsumer) interfaces.IRecordProcessorFactory {
	return integrationRecordProcessorFactory{rc: rc}
}

func TestCheckpointRecovery(t *testing.T) {
	log.Println("Starting integration test", t.Name())

	rc := &IntegrationRecordConsumer{
		processedRecords: make(map[string]int),
	}

	cfg := config.NewKinesisClientLibConfig("integration_checkpoint_recovery",
		"checkpoint_recovery", "ap-nil-1", "").
		WithInitialPositionInStream(config.TRIM_HORIZON).
		WithTableName("checkpoint_recovery").
		WithDynamoDBEndpoint("http://dynamodb:8000").
		WithKinesisEndpoint("http://kinesis:4567").
		WithFailoverTimeMillis(1).             // gokini: LeaseDuration
		WithShardSyncIntervalMillis(1).        // gokini: eventLoopSleepMs
		WithIdleTimeBetweenReadsInMillis(2000) // gokini: EmptyRecordBackoffMs

	pushRecordToKinesis("checkpoint_recovery", []byte("abcd"), true)
	defer deleteStream("checkpoint_recovery")
	defer deleteTable("checkpoint_recovery")

	w := worker.NewWorker(newIntegrationRecordProcessorFactory(rc), cfg)
	if err := w.Start(); err != nil {
		t.Fatal("can't start worker:", err)
	}

	time.Sleep(200 * time.Millisecond)
	w.Shutdown()

	cfg = config.NewKinesisClientLibConfig("integration_checkpoint_recovery",
		"checkpoint_recovery", "ap-nil-1", "").
		WithInitialPositionInStream(config.TRIM_HORIZON).
		WithTableName("checkpoint_recovery").
		WithDynamoDBEndpoint("http://dynamodb:8000").
		WithKinesisEndpoint("http://kinesis:4567").
		WithFailoverTimeMillis(1).            // gokini: LeaseDuration
		WithShardSyncIntervalMillis(1000).    // gokini: eventLoopSleepMs
		WithIdleTimeBetweenReadsInMillis(500) // gokini: EmptyRecordBackoffMs

	w2 := worker.NewWorker(newIntegrationRecordProcessorFactory(rc), cfg)
	if err := w2.Start(); err != nil {
		t.Fatal("can't start worker:", err)
	}

	time.Sleep(200 * time.Millisecond)
	for sequenceID, timesSequenceProcessed := range rc.processedRecords {
		fmt.Printf("sequenceID: %s, processed %d time(s)\n", sequenceID, timesSequenceProcessed)
		if timesSequenceProcessed > 1 {
			t.Errorf("got sequence number %s processed %d times, want 1", sequenceID, timesSequenceProcessed)
		}
	}
	w2.Shutdown()
}

func TestCheckpointGainLock(t *testing.T) {
	log.Println("Starting integration test", t.Name())

	rc := &IntegrationRecordConsumer{
		processedRecords: make(map[string]int),
	}

	cfg := config.NewKinesisClientLibConfig("integration_checkpoint_gain_lock",
		"checkpoint_gain_lock", "ap-nil-1", "").
		WithInitialPositionInStream(config.TRIM_HORIZON).
		WithTableName("checkpoint_gain_lock").
		WithDynamoDBEndpoint("http://dynamodb:8000").
		WithKinesisEndpoint("http://kinesis:4567").
		WithFailoverTimeMillis(100).           // gokini: LeaseDuration
		WithIdleTimeBetweenReadsInMillis(2000) // gokini: EmptyRecordBackoffMs

	pushRecordToKinesis("checkpoint_gain_lock", []byte("abcd"), true)
	defer deleteStream("checkpoint_gain_lock")
	defer deleteTable("checkpoint_gain_lock")

	w := worker.NewWorker(newIntegrationRecordProcessorFactory(rc), cfg)
	if err := w.Start(); err != nil {
		t.Fatal("can't start worker:", err)
	}

	time.Sleep(200 * time.Millisecond)
	w.Shutdown()

	cfg2 := config.NewKinesisClientLibConfig("integration_checkpoint_gain_lock",
		"checkpoint_gain_lock", "ap-nil-1", "").
		WithInitialPositionInStream(config.TRIM_HORIZON).
		WithTableName("checkpoint_gain_lock").
		WithDynamoDBEndpoint("http://dynamodb:8000").
		WithKinesisEndpoint("http://kinesis:4567").
		WithFailoverTimeMillis(100) // gokini: LeaseDuration

	w2 := worker.NewWorker(newIntegrationRecordProcessorFactory(rc), cfg2)
	if err := w2.Start(); err != nil {
		t.Fatal("can't start worker:", err)
	}

	pushRecordToKinesis("checkpoint_gain_lock", []byte("abcd"), false)
	time.Sleep(200 * time.Millisecond)
	if len(rc.processedRecords) != 2 {
		t.Errorf("got %d processed records, want 2", len(rc.processedRecords))
		for sequenceID, timesProcessed := range rc.processedRecords {
			fmt.Println("Processed", sequenceID, timesProcessed, "time(s)")
		}
	}
	w2.Shutdown()
}

func TestPrometheusMonitoring(t *testing.T) {
	log.Println("Starting integration test", t.Name())

	rc := &IntegrationRecordConsumer{
		processedRecords: make(map[string]int),
	}

	prom := prometheus.NewMonitoringService(":8080", "ap-nil-1", logger.GetDefaultLogger())
	cfg := config.NewKinesisClientLibConfig("integration_prometheus_monitoring",
		"prometheus_monitoring", "ap-nil-1", "").
		WithInitialPositionInStream(config.TRIM_HORIZON).
		WithTableName("prometheus_monitoring").
		WithDynamoDBEndpoint("http://dynamodb:8000").
		WithKinesisEndpoint("http://kinesis:4567").
		WithFailoverTimeMillis(1).              // gokini: LeaseDuration
		WithIdleTimeBetweenReadsInMillis(2000). // gokini: EmptyRecordBackoffMs
		WithMonitoringService(prom)

	pushRecordToKinesis("prometheus_monitoring", []byte("abcd"), true)
	defer deleteStream("prometheus_monitoring")
	defer deleteTable("prometheus_monitoring")

	w := worker.NewWorker(newIntegrationRecordProcessorFactory(rc), cfg)
	if err := w.Start(); err != nil {
		t.Fatal("can't start worker:", err)
	}

	time.Sleep(600 * time.Millisecond)

	res, err := http.Get("http://localhost:8080/metrics")
	if err != nil {
		t.Fatal("can't scrape prometheus endpoint:", err)
	}
	w.Shutdown()

	var parser expfmt.TextParser
	parsed, err := parser.TextToMetricFamilies(res.Body)
	res.Body.Close()
	if err != nil {
		t.Error("can't read prometheus response:", err)
	}

	if got := *parsed["integration_prometheus_monitoring_processed_bytes"].Metric[0].Counter.Value; got != 4 {
		t.Errorf("got 'processed_bytes' = %v, want 4", got)
	}

	if got := *parsed["integration_prometheus_monitoring_processed_records"].Metric[0].Counter.Value; got != 1 {
		t.Errorf("got 'processed_records' = %v, want 1", got)
	}

	if got := *parsed["integration_prometheus_monitoring_leases_held"].Metric[0].Gauge.Value; got != 1 {
		t.Errorf("got 'leases_held' = %v, want 1", got)
	}
}

/*
func setupConsumer(name string, t *testing.T) *KinesisConsumer {
	rc := &IntegrationRecordConsumer{
		processedRecords: make(map[string]int),
	}
	kc := &KinesisConsumer{
		StreamName:           name,
		ShardIteratorType:    "TRIM_HORIZON",
		RecordConsumer:       rc,
		TableName:            name,
		EmptyRecordBackoffMs: 50,
		LeaseDuration:        400,
		eventLoopSleepMs:     100,
	}
	err := kc.StartConsumer()
	if err != nil {
		t.Fatalf("Error starting consumer %s", err)
	}
	return kc
}

func TestRebalance(t *testing.T) {
	uuid, _ := uuid.NewUUID()
	name := uuid.String()
	err := createStream(name, 2)
	if err != nil {
		t.Fatalf("Error creating stream %s", err)
	}
	kc := setupConsumer(name, t)
	time.Sleep(600 * time.Millisecond)
	secondKc := setupConsumer(name, t)
	defer deleteStream(name)
	defer deleteTable(name)
	time.Sleep(2000 * time.Millisecond)
	workers, err := kc.checkpointer.ListActiveWorkers()
	if err != nil {
		t.Fatalf("Error getting workers %s", err)
	}
	if len(workers[kc.consumerID]) != 1 {
		t.Errorf("Expected consumer to have 1 shard, it has %d", len(workers[kc.consumerID]))
	}
	if len(workers[secondKc.consumerID]) != 1 {
		t.Errorf("Expected consumer to have 1 shard, it has %d", len(workers[secondKc.consumerID]))
	}
	kc.Shutdown()
	secondKc.Shutdown()
}
*/
