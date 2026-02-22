package test

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

var TestConfig = types.Configuration{
	PubSubUrl:        "nats://localhost:4222",
	BrokerHost:     "localhost",
	BrokerPort:     19090,
	NodeID:         1,
	StreamReplicas: 1,
	StorageType:    "file",
}

var BootstrapServers = fmt.Sprintf("%s:%d", TestConfig.BrokerHost, TestConfig.BrokerPort)
var HomeDir, _ = os.UserHomeDir()
var KafkaBinDir = HomeDir + "/kafka_2.13-3.9.0/bin/" // assumes kafka is in HomeDir

func TestMain(m *testing.M) {
	logging.SetLogLevel(logging.DEBUG)
	log.Println("Setup: Initializing resources")
	v, exists := os.LookupEnv("KAFKA_BIN_DIR")
	if exists {
		KafkaBinDir = v
	}
	if _, err := os.Stat(KafkaBinDir); err != nil {
		log.Printf("Ensure Kafka bin dir exists. %v", err)
		os.Exit(1)
	}
	b := broker.NewBroker(&TestConfig)

	go b.Startup()
	time.Sleep(2 * time.Second)

	// Run the tests
	exitCode := m.Run()

	// Teardown
	log.Println("Teardown: Cleaning up resources")
	b.Shutdown()
	os.Exit(exitCode)
}

func TestTopicCreation(t *testing.T) {
	topicName := "titi"
	cmd := exec.Command(
		filepath.Join(KafkaBinDir, "kafka-topics.sh"),
		"--bootstrap-server", BootstrapServers,
		"--create",
		"--topic", topicName,
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
	topicName := "test-topic"
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

	consumerCmd := exec.Command(
		filepath.Join(KafkaBinDir, "/kafka-console-consumer.sh"),
		"--bootstrap-server", BootstrapServers,
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

func TestGroupOffsetResume(t *testing.T) {
	nbRecords := "100"

	topicName := "test-group-offset-resume"
	cmd := exec.Command(
		filepath.Join(KafkaBinDir, "kafka-producer-perf-test.sh"),
		"--topic", topicName,
		"--num-records", nbRecords,
		"--payload-monotonic", // payload will be 1,2 ...nbRecords
		"--throughput", "100000",
		"--producer-props", "acks=1", "batch.size=1", fmt.Sprintf("bootstrap.servers=%s", BootstrapServers), // batch.size=1 to have fine-grained consumption
	)
	_, err := cmd.Output()

	if err != nil {
		t.Error(err.Error())
	}

	// we consume nbConsumedRecords from beginning and commit offset
	// next consumed message for the same group id should be "nbConsumedRecords" given we used --payload-monotonic
	nbConsumedRecords := "33"
	consumerCmd := exec.Command(
		filepath.Join(KafkaBinDir, "kafka-console-consumer.sh"),
		"--bootstrap-server", BootstrapServers,
		"--topic", topicName,
		"--max-messages", nbConsumedRecords,
		"--timeout-ms", "30000",
		"--consumer-property", "enable.auto.commit=true",
		"--consumer-property", "group.id=test1",
		"--from-beginning",
	)
	err = consumerCmd.Run()
	if err != nil {
		t.Error(err.Error())
	}

	// consumer resumes from last offset at `nbConsumedRecords`
	consumerCmd = exec.Command(
		filepath.Join(KafkaBinDir, "kafka-console-consumer.sh"),
		"--bootstrap-server", BootstrapServers,
		"--topic", topicName,
		"--max-messages", "1",
		"--timeout-ms", "30000",
		"--consumer-property", "group.id=test1",
	)
	output, err := consumerCmd.CombinedOutput()
	if err != nil {
		t.Error(err.Error())
		return
	}
	if !strings.Contains(string(output), nbConsumedRecords) {
		t.Errorf("Expected kafka-console-consumer output to contain value '%v'. Output: %v", nbConsumedRecords, string(output))
	}
}

func TestConsumeFromUnknownTopic(t *testing.T) {
	topicName := "fugazi-topic"
	consumerCmd := exec.Command(
		filepath.Join(KafkaBinDir, "/kafka-console-consumer.sh"),
		"--bootstrap-server", BootstrapServers,
		"--topic", topicName,
		"--timeout-ms", "1000",
		"--consumer-property", "allow.auto.create.topics=false",
	)
	output, err := consumerCmd.CombinedOutput()

	if !strings.Contains(string(output), fmt.Sprintf("{%s=UNKNOWN_TOPIC_OR_PARTITION}", topicName)) {
		t.Errorf("Expected output to contain '{%s=UNKNOWN_TOPIC_OR_PARTITION}'. Output: %v. Err: %v", topicName, string(output), err)
	}
}
