package cluster

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hanzoai/kafka/logging"
	broker "github.com/hanzoai/kafka/protocol"
	"github.com/hanzoai/kafka/types"
)

// Two Hanzo Kafka instances pointing at the same PubSub server
var TestConfig = types.Configuration{
	PubSubUrl:        "nats://localhost:4222",
	BrokerHost:     "localhost",
	BrokerPort:     19091,
	NodeID:         1,
	StreamReplicas: 1,
	StorageType:    "file",
}

var TestConfig2 = types.Configuration{
	PubSubUrl:        "nats://localhost:4222",
	BrokerHost:     "localhost",
	BrokerPort:     19092,
	NodeID:         2,
	StreamReplicas: 1,
	StorageType:    "file",
}

var BootstrapServers = fmt.Sprintf("%s:%d", TestConfig.BrokerHost, TestConfig.BrokerPort)
var HomeDir, _ = os.UserHomeDir()
var KafkaBinDir = HomeDir + "/kafka_2.13-3.9.0/bin/"

var topicName = "cluster-test-topic"

func TestMain(m *testing.M) {
	logging.SetLogLevel(logging.INFO)
	log.Println("Setup: Initializing resources")
	v, exists := os.LookupEnv("KAFKA_BIN_DIR")
	if exists {
		KafkaBinDir = v
	}
	if _, err := os.Stat(KafkaBinDir); err != nil {
		log.Printf("Ensure Kafka bin dir exists. %v", err)
		os.Exit(1)
	}
	broker1 := broker.NewBroker(&TestConfig)
	go broker1.Startup()
	time.Sleep(2 * time.Second)
	broker2 := broker.NewBroker(&TestConfig2)
	go broker2.Startup()
	time.Sleep(1 * time.Second)

	exitCode := m.Run()

	log.Println("Teardown: Cleaning up resources")
	broker1.Shutdown()
	broker2.Shutdown()

	os.Exit(exitCode)
}

func TestTopicCreation(t *testing.T) {
	cmd := exec.Command(
		filepath.Join(KafkaBinDir, "kafka-topics.sh"),
		"--bootstrap-server", BootstrapServers,
		"--create",
		"--topic", topicName,
		"--partitions", "10",
	)
	output, err := cmd.CombinedOutput()

	if err != nil {
		t.Error(err.Error())
	}
	if !strings.Contains(string(output), fmt.Sprintf("Created topic %s.", topicName)) {
		t.Errorf("Expected output to contain 'Created topic %s.'. Output: %v", topicName, string(output))
	}
}

func TestProducerAndConsumer(t *testing.T) {
	nbRecords := "1000"
	cmd := exec.Command(
		filepath.Join(KafkaBinDir, "kafka-producer-perf-test.sh"),
		"--topic", topicName,
		"--num-records", nbRecords,
		"--record-size", "500",
		"--throughput", "100000",
		"--producer-props", "acks=1", "batch.size=16384", "linger.ms=5", fmt.Sprintf("bootstrap.servers=%s", BootstrapServers),
	)
	_, err := cmd.Output()

	if err != nil {
		t.Error(err.Error())
	}

	// Consume via the second broker to verify shared PubSub state
	bootstrapServers2 := fmt.Sprintf("%s:%d", TestConfig2.BrokerHost, TestConfig2.BrokerPort)
	consumerCmd := exec.Command(
		filepath.Join(KafkaBinDir, "/kafka-console-consumer.sh"),
		"--bootstrap-server", bootstrapServers2,
		"--topic", topicName,
		"--max-messages", nbRecords,
		"--timeout-ms", "30000",
		"--from-beginning",
	)
	output, err := consumerCmd.CombinedOutput()

	if err != nil {
		t.Error(err.Error())
	}
	if !strings.Contains(string(output), fmt.Sprintf("Processed a total of %s messages", nbRecords)) {
		t.Errorf("Expected consumer output to contain 'Processed a total of %s messages'. Output: %v", nbRecords, string(output))
	}
}
