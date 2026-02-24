package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/hanzoai/stream/logging"
	"github.com/hanzoai/stream/protocol"
	"github.com/hanzoai/stream/types"
	"github.com/spf13/cobra"
)

var config = types.Configuration{
	PubSubUrl:      "nats://localhost:4222",
	BrokerHost:     "localhost",
	BrokerPort:     9092,
	AdminPort:      9093,
	NodeID:         1,
	StreamReplicas: 1,
	StorageType:    "file",
}

func main() {
	var rootCmd = &cobra.Command{
		Use:   "hanzo-stream",
		Short: "Hanzo Stream — Kafka wire protocol gateway for Hanzo PubSub",
		Run: func(cmd *cobra.Command, args []string) {
			broker := protocol.NewBroker(&config)
			log.SetLogLevel(log.INFO)

			// Handle termination signals (e.g., Ctrl+C)
			signalChannel := make(chan os.Signal, 1)
			signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				sig := <-signalChannel
				log.Info("Received signal: %s. Shutting down...", sig)
				broker.Shutdown()
				os.Exit(0)
			}()

			// Start the broker
			broker.Startup()
		},
	}

	rootCmd.Flags().StringVar(&config.PubSubUrl, "pubsub-url", "nats://localhost:4222", "Hanzo PubSub server URL")
	rootCmd.Flags().StringVar(&config.PubSubCredFile, "pubsub-creds", "", "Hanzo PubSub credentials file")
	rootCmd.Flags().IntVar(&config.BrokerPort, "port", 9092, "Kafka listener port")
	rootCmd.Flags().IntVar(&config.AdminPort, "admin-port", 9093, "Admin HTTP port (0 to disable)")
	rootCmd.Flags().StringVar(&config.BrokerHost, "host", "localhost", "Advertised hostname")
	rootCmd.Flags().IntVar(&config.NodeID, "node-id", 1, "Broker node ID")
	rootCmd.Flags().IntVar(&config.StreamReplicas, "replicas", 1, "Hanzo Stream replica count")
	rootCmd.Flags().StringVar(&config.StorageType, "storage", "file", "Hanzo Stream storage type: file or memory")

	if err := rootCmd.Execute(); err != nil {
		log.Panic("Failed to execute root command %v", err)
	}
}
