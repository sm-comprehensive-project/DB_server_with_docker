package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/mux"
)

// 이벤트 유형 정의 (consumer와 동일하게)
type EventType string

const (
	EventLiked            EventType = "LIKED_LIVE"
	EventWatched          EventType = "WATCHED"
	EventClicked          EventType = "CLICKED"
	EventCategoryInterest EventType = "CATEGORY_INTEREST"
	EventSearch           EventType = "SEARCH"
)

// 이벤트 기본 구조체 (consumer와 동일하게)
type UserEvent struct {
	UserID    string      `json:"userId"`
	EventType EventType   `json:"type"`
	Data      interface{} `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
}

// 각 이벤트 타입별 데이터 구조체 정의
type LikedLiveData struct {
	LiveID string `json:"liveId"`
}

type WatchedData struct {
	ContentID     string  `json:"contentId"`
	WatchDuration int     `json:"watchDuration"`
	Progress      float64 `json:"progress"`
}

type ClickedItemData struct {
	ItemID   string `json:"itemId"`
	ItemType string `json:"itemType"`
	Category string `json:"category"`
}

type CategoryInterestData struct {
	Category string  `json:"category"`
	Score    float64 `json:"score"`
}

type SearchData struct {
	Query       string `json:"query"`
	ResultCount int    `json:"resultCount"`
}

var producer *kafka.Producer

func main() {
	// Kafka 프로듀서 설정
	var err error
	producer, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_BROKERS"),
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	// 전송 결과 모니터링
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					log.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	// API 라우터 설정
	r := mux.NewRouter()
	r.HandleFunc("/events", handleEvent).Methods("POST")
	r.HandleFunc("/health", healthCheck).Methods("GET")

	// 서버 시작
	log.Println("Go Server starting on port 8000...")
	if err := http.ListenAndServe(":8000", r); err != nil {
		log.Fatal(err)
	}
}

func handleEvent(w http.ResponseWriter, r *http.Request) {
	var event UserEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 타임스탬프 설정 (없으면 현재 시간)
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// 이벤트 JSON으로 변환
	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Kafka로 이벤트 전송 (토픽명을 user-events로 수정)
	topic := "user-events"
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          eventJSON,
		Key:            []byte(event.UserID), // UserID를 키로 사용
	}, nil)
	if err != nil {
		http.Error(w, "Failed to produce message: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "event accepted"})
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}