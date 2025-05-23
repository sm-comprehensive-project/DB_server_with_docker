// package main

// import (
// 	"encoding/json"
// 	"log"
// 	"net/http"
// 	"os"
// 	"time"

// 	"github.com/confluentinc/confluent-kafka-go/kafka"
// 	"github.com/gorilla/mux"
// )

// // 이벤트 유형 정의 (consumer와 동일하게)
// type EventType string

// const (
// 	EventLiked            EventType = "LIKED_LIVE"
// 	EventWatched          EventType = "WATCHED"
// 	EventClicked          EventType = "CLICKED"
// 	EventCategoryInterest EventType = "CATEGORY_INTEREST"
// 	EventSearch           EventType = "SEARCH"
// )

// // 이벤트 기본 구조체 (consumer와 동일하게)
// type UserEvent struct {
// 	UserID    string      `json:"userId"`
// 	EventType EventType   `json:"type"`
// 	Data      interface{} `json:"data"`
// 	Timestamp time.Time   `json:"timestamp"`
// }

// // 각 이벤트 타입별 데이터 구조체 정의
// type LikedLiveData struct {
// 	LiveID string `json:"liveId"`
// }

// type WatchedData struct {
// 	ContentID     string  `json:"contentId"`
// 	Progress      float64 `json:"progress"`
// }

// type ClickedItemData struct {
// 	ItemID   string `json:"itemId"`
// 	ItemType string `json:"itemType"`
// 	Category string `json:"category"`
// }

// type CategoryInterestData struct {
// 	Category string  `json:"category"`
// 	Score    float64 `json:"score"`
// }

// type SearchData struct {
// 	Query       string `json:"query"`
// 	ResultCount int    `json:"resultCount"`
// }

// var producer *kafka.Producer

// func main() {
// 	log.Println("Starting Go Server...")
	
// 	// 환경변수 확인
// 	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
// 	if kafkaBrokers == "" {
// 		kafkaBrokers = "kafka:29092" // 기본값 설정
// 	}
// 	log.Printf("Using Kafka brokers: %s", kafkaBrokers)

// 	// Kafka 프로듀서 설정 - 재시도 로직 추가
// 	var err error
// 	for i := 0; i < 10; i++ {
// 		producer, err = kafka.NewProducer(&kafka.ConfigMap{
// 			"bootstrap.servers":        kafkaBrokers,
// 			"delivery.timeout.ms":      300000, // 5분
// 			"request.timeout.ms":       30000,  // 30초
// 			"message.timeout.ms":       300000, // 5분
// 			"socket.keepalive.enable":  true,
// 			"retries":                  10,
// 		})
// 		if err == nil {
// 			break
// 		}
// 		log.Printf("Failed to create producer (attempt %d/10): %s", i+1, err)
// 		time.Sleep(5 * time.Second)
// 	}
	
// 	if err != nil {
// 		log.Fatalf("Failed to create producer after 10 attempts: %s", err)
// 	}
// 	defer producer.Close()
// 	log.Println("Kafka producer created successfully")

// 	// 전송 결과 모니터링
// 	go func() {
// 		for e := range producer.Events() {
// 			switch ev := e.(type) {
// 			case *kafka.Message:
// 				if ev.TopicPartition.Error != nil {
// 					log.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
// 				} else {
// 					log.Printf("Delivered message to topic %s [%d] at offset %v\n",
// 						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
// 				}
// 			}
// 		}
// 	}()

// 	// API 라우터 설정
// 	r := mux.NewRouter()
// 	r.HandleFunc("/events", handleEvent).Methods("POST")
// 	r.HandleFunc("/health", healthCheck).Methods("GET")

// 	// CORS 미들웨어 추가
// 	r.Use(func(next http.Handler) http.Handler {
// 		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 			w.Header().Set("Access-Control-Allow-Origin", "*")
// 			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
// 			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			
// 			if r.Method == "OPTIONS" {
// 				w.WriteHeader(http.StatusOK)
// 				return
// 			}
			
// 			next.ServeHTTP(w, r)
// 		})
// 	})

// 	// 서버 시작
// 	log.Println("Go Server starting on port 8000...")
// 	if err := http.ListenAndServe(":8000", r); err != nil {
// 		log.Fatal(err)
// 	}
// }

// func handleEvent(w http.ResponseWriter, r *http.Request) {
// 	log.Printf("Received event request from %s", r.RemoteAddr)
	
// 	var event UserEvent
// 	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
// 		log.Printf("Error parsing request body: %v", err)
// 		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
// 		return
// 	}

// 	// 필수 필드 검증
// 	if event.UserID == "" {
// 		http.Error(w, "userId is required", http.StatusBadRequest)
// 		return
// 	}
// 	if event.EventType == "" {
// 		http.Error(w, "type is required", http.StatusBadRequest)
// 		return
// 	}

// 	// 타임스탬프 설정 (없으면 현재 시간)
// 	if event.Timestamp.IsZero() {
// 		event.Timestamp = time.Now()
// 	}

// 	log.Printf("Processing event: UserID=%s, Type=%s", event.UserID, event.EventType)

// 	// 이벤트 JSON으로 변환
// 	eventJSON, err := json.Marshal(event)
// 	if err != nil {
// 		log.Printf("Error marshaling event: %v", err)
// 		http.Error(w, "Error processing event: "+err.Error(), http.StatusInternalServerError)
// 		return
// 	}

// 	// Kafka로 이벤트 전송
// 	topic := "user-events"
// 	err = producer.Produce(&kafka.Message{
// 		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
// 		Value:          eventJSON,
// 		Key:            []byte(event.UserID), // UserID를 키로 사용
// 	}, nil)
// 	if err != nil {
// 		log.Printf("Failed to produce message: %v", err)
// 		http.Error(w, "Failed to produce message: "+err.Error(), http.StatusInternalServerError)
// 		return
// 	}

// 	// 전송 완료 대기 (선택적)
// 	producer.Flush(1000) // 1초 대기

// 	log.Printf("Event sent to Kafka successfully: UserID=%s, Type=%s", event.UserID, event.EventType)
	
// 	w.Header().Set("Content-Type", "application/json")
// 	w.WriteHeader(http.StatusAccepted)
// 	json.NewEncoder(w).Encode(map[string]string{"status": "event accepted"})
// }

// func healthCheck(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Content-Type", "application/json")
// 	json.NewEncoder(w).Encode(map[string]bool{"status": true})
// }

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

// 이벤트 유형 정의
type EventType string

const (
	EventLiked            EventType = "LIKED_LIVE"
	EventWatched          EventType = "WATCHED"
	EventClicked          EventType = "CLICKED"
	EventCategoryInterest EventType = "CATEGORY_INTEREST"
	EventSearch           EventType = "SEARCH"
)

// 이벤트 기본 구조체
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
	ContentID string  `json:"contentId"`
	Progress  float64 `json:"progress"`
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
	log.Println("🚀 Starting Go Event Server...")
	
	// 환경변수 확인
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "kafka:29092" // 기본값 설정
	}
	log.Printf("📡 Using Kafka brokers: %s", kafkaBrokers)

	// Kafka 프로듀서 설정 - 재시도 로직 추가
	var err error
	for i := 0; i < 10; i++ {
		producer, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers":        kafkaBrokers,
			"delivery.timeout.ms":      300000, // 5분
			"request.timeout.ms":       30000,  // 30초
			"message.timeout.ms":       300000, // 5분
			"socket.keepalive.enable":  true,
			"retries":                  10,
		})
		if err == nil {
			break
		}
		log.Printf("⚠️  Failed to create producer (attempt %d/10): %s", i+1, err)
		time.Sleep(5 * time.Second)
	}
	
	if err != nil {
		log.Fatalf("❌ Failed to create producer after 10 attempts: %s", err)
	}
	defer producer.Close()
	log.Println("✅ Kafka producer created successfully")

	// 전송 결과 모니터링
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("❌ Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					log.Printf("✅ Message delivered to topic %s [%d] at offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	// API 라우터 설정
	r := mux.NewRouter()
	r.HandleFunc("/events", handleEvent).Methods("POST")
	r.HandleFunc("/health", healthCheck).Methods("GET")

	// CORS 미들웨어 추가
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			
			next.ServeHTTP(w, r)
		})
	})

	// 서버 시작
	log.Println("🌐 Go Server starting on port 8000...")
	if err := http.ListenAndServe(":8000", r); err != nil {
		log.Fatal(err)
	}
}

func handleEvent(w http.ResponseWriter, r *http.Request) {
	log.Printf("📨 Received event request from %s", r.RemoteAddr)
	
	var event UserEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		log.Printf("❌ Error parsing request body: %v", err)
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// 필수 필드 검증
	if event.UserID == "" {
		log.Printf("❌ Missing userId in request")
		http.Error(w, "userId is required", http.StatusBadRequest)
		return
	}
	if event.EventType == "" {
		log.Printf("❌ Missing event type in request")
		http.Error(w, "type is required", http.StatusBadRequest)
		return
	}

	// 이벤트 타입 검증
	validEventTypes := map[EventType]bool{
		EventLiked:            true,
		EventWatched:          true,
		EventClicked:          true,
		EventCategoryInterest: true,
		EventSearch:           true,
	}
	
	if !validEventTypes[event.EventType] {
		log.Printf("❌ Invalid event type: %s", event.EventType)
		http.Error(w, "Invalid event type", http.StatusBadRequest)
		return
	}

	// 타임스탬프 설정 (없으면 현재 시간)
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	log.Printf("🔄 Processing event: UserID=%s, Type=%s", event.UserID, event.EventType)

	// 이벤트 데이터 검증
	if err := validateEventData(event); err != nil {
		log.Printf("❌ Event data validation failed: %v", err)
		http.Error(w, "Invalid event data: "+err.Error(), http.StatusBadRequest)
		return
	}

	// 이벤트 JSON으로 변환
	eventJSON, err := json.Marshal(event)
	if err != nil {
		log.Printf("❌ Error marshaling event: %v", err)
		http.Error(w, "Error processing event: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Kafka로 이벤트 전송
	topic := "user-events"
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          eventJSON,
		Key:            []byte(event.UserID), // UserID를 키로 사용
	}, nil)
	if err != nil {
		log.Printf("❌ Failed to produce message: %v", err)
		http.Error(w, "Failed to produce message: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 전송 완료 대기
	producer.Flush(1000) // 1초 대기

	log.Printf("✅ Event sent to Kafka successfully: UserID=%s, Type=%s", event.UserID, event.EventType)
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "event accepted",
		"userId":    event.UserID,
		"eventType": event.EventType,
		"timestamp": event.Timestamp,
	})
}

func validateEventData(event UserEvent) error {
	switch event.EventType {
	case EventLiked:
		var data LikedLiveData
		dataBytes, _ := json.Marshal(event.Data)
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return err
		}
		if data.LiveID == "" {
			return json.NewDecoder(nil).Decode(nil) // 간단한 에러 생성
		}
	case EventWatched:
		var data WatchedData
		dataBytes, _ := json.Marshal(event.Data)
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return err
		}
		if data.ContentID == "" {
			return json.NewDecoder(nil).Decode(nil)
		}
	case EventClicked:
		var data ClickedItemData
		dataBytes, _ := json.Marshal(event.Data)
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return err
		}
		if data.ItemID == "" {
			return json.NewDecoder(nil).Decode(nil)
		}
	case EventSearch:
		var data SearchData
		dataBytes, _ := json.Marshal(event.Data)
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return err
		}
		if data.Query == "" {
			return json.NewDecoder(nil).Decode(nil)
		}
	}
	return nil
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"service":   "event-producer",
	})
}