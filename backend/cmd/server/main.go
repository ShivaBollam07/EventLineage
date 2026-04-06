// Package main is the entry point for the EventLineage server.
// It starts the REST API server and Kafka ingestion consumer.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eventlineage/backend/internal/api"
	"github.com/eventlineage/backend/internal/kafka"
	"github.com/eventlineage/backend/internal/store"
)

// Config holds server configuration from environment variables.
type Config struct {
	// Server
	Port string

	// Neo4j
	Neo4jURI      string
	Neo4jUser     string
	Neo4jPassword string

	// Kafka
	KafkaBrokers string
	KafkaGroupID string
}

func loadConfig() *Config {
	return &Config{
		Port:          getEnv("PORT", "8080"),
		Neo4jURI:      getEnv("NEO4J_URI", "bolt://localhost:7687"),
		Neo4jUser:     getEnv("NEO4J_USER", "neo4j"),
		Neo4jPassword: getEnv("NEO4J_PASSWORD", "password"),
		KafkaBrokers:  getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaGroupID:  getEnv("KAFKA_GROUP_ID", "eventlineage-ingestion"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	log.Println("Starting EventLineage server...")

	cfg := loadConfig()

	// Initialize Neo4j store
	neo4jStore, err := store.NewNeo4jStore(cfg.Neo4jURI, cfg.Neo4jUser, cfg.Neo4jPassword)
	if err != nil {
		log.Fatalf("Failed to connect to Neo4j: %v", err)
	}
	defer neo4jStore.Close()
	log.Println("Connected to Neo4j")

	// Run schema migrations
	if err := neo4jStore.RunMigrations(context.Background()); err != nil {
		log.Fatalf("Failed to run Neo4j migrations: %v", err)
	}
	log.Println("Neo4j migrations applied")

	// Initialize repositories
	messageRepo := store.NewMessageRepository(neo4jStore)
	chainRepo := store.NewChainRepository(neo4jStore)
	serviceRepo := store.NewServiceRepository(neo4jStore)

	// Initialize Kafka consumer
	// Uses Kafka offset commit as retry mechanism - no separate retry queue needed
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, messageRepo)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	// Start Kafka consumer in background
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("Kafka consumer error: %v", err)
		}
	}()
	log.Println("Kafka consumer started")

	// Initialize API router
	router := api.NewRouter(messageRepo, chainRepo, serviceRepo, neo4jStore, cfg.KafkaBrokers)

	// Create HTTP server
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		log.Printf("API server listening on port %s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// Cancel Kafka consumer
	cancel()

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}
