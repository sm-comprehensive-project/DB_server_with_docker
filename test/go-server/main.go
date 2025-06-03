package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/mux"
)

// 이벤트 유형 정의 - 사용자가 수행할 수 있는 액션들
type EventType string

const (
	EventLiked            EventType = "LIKED_LIVE"        // 라이브 방송 좋아요
	EventWatched          EventType = "WATCHED"           // 영상/콘텐츠 시청
	EventClicked          EventType = "CLICKED"           // 특정 아이템 클릭
	EventCategoryInterest EventType = "CATEGORY_INTEREST" // 카테고리에 관심 표시
	EventSearch           EventType = "SEARCH"            // 검색 실행
)

// 모든 이벤트의 기본 구조체 - 공통 필드들
type UserEvent struct {
	UserID    string      `json:"userId"`    // 사용자 식별자 (이메일)
	EventType EventType   `json:"type"`      // 어떤 종류의 이벤트인지
	Data      interface{} `json:"data"`      // 이벤트별 상세 데이터
	Timestamp time.Time   `json:"timestamp"` // 이벤트 발생 시간
}

// 각 이벤트 타입별로 필요한 데이터 구조체들
type LikedLiveData struct {
	ObjectId string `json:"ObjectId"` // 좋아요한 라이브 방송 ID
}

type WatchedData struct {
	ObjectId string `json:"ObjectId"` // 시청한 콘텐츠 ID
}

type ClickedItemData struct {
	ObjectId string `json:"ObjectId"` // 클릭한 아이템 ID
}

type CategoryInterestData struct {
	Category string `json:"category"` // 관심 표시한 카테고리명
}

type SearchData struct {
	Query       string `json:"query"`       // 검색어
	ResultCount int    `json:"resultCount"` // 검색 결과 개수
}

// Kafka 프로듀서 - 전역 변수로 선언하여 재사용
var producer *kafka.Producer

func main() {
	log.Println("🚀 Starting Go Event Server...")
	
	// 환경변수에서 Kafka 브로커 주소 읽기 (없으면 기본값 사용)
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "kafka:29092" // Docker 환경에서의 기본 주소
	}
	log.Printf("📡 Using Kafka brokers: %s", kafkaBrokers)

	// Kafka 프로듀서 생성 - 연결 실패시 재시도 로직
	var err error
	for i := 0; i < 10; i++ {
		producer, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers":        kafkaBrokers,
			"delivery.timeout.ms":      300000, // 메시지 전송 타임아웃: 5분
			"request.timeout.ms":       30000,  // 요청 타임아웃: 30초
			"message.timeout.ms":       300000, // 메시지 타임아웃: 5분
			"socket.keepalive.enable":  true,   // 소켓 연결 유지
			"retries":                  10,     // 재시도 횟수
		})
		if err == nil {
			break // 성공하면 루프 종료
		}
		log.Printf("⚠️  Failed to create producer (attempt %d/10): %s", i+1, err)
		time.Sleep(5 * time.Second) // 5초 대기 후 재시도
	}
	
	if err != nil {
		log.Fatalf("❌ Failed to create producer after 10 attempts: %s", err)
	}
	defer producer.Close() // 프로그램 종료시 프로듀서 정리
	log.Println("✅ Kafka producer created successfully")

	// Kafka 메시지 전송 결과를 백그라운드에서 모니터링
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

	// HTTP API 라우터 설정
	r := mux.NewRouter()
	r.HandleFunc("/events", handleEvent).Methods("POST")   // 이벤트 수신 엔드포인트
	r.HandleFunc("/health", healthCheck).Methods("GET")    // 헬스체크 엔드포인트

	// CORS 설정 - 웹 브라우저에서 API 호출을 허용
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			
			// OPTIONS 요청 처리 (브라우저의 preflight 요청)
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			
			next.ServeHTTP(w, r)
		})
	})

	// HTTP 서버 시작 (포트 8000)
	log.Println("🌐 Go Server starting on port 8000...")
	if err := http.ListenAndServe(":8000", r); err != nil {
		log.Fatal(err)
	}
}

// POST /events 엔드포인트 핸들러 - 사용자 이벤트를 받아서 Kafka로 전송
func handleEvent(w http.ResponseWriter, r *http.Request) {
	log.Printf("📨 Received event request from %s", r.RemoteAddr)
	
	// HTTP 요청 본문에서 JSON 파싱
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

	// 유효한 이벤트 타입인지 검증
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

	// 타임스탬프가 없으면 현재 시간으로 설정
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	log.Printf("🔄 Processing event: UserID=%s, Type=%s", event.UserID, event.EventType)

	// 이벤트별 데이터 유효성 검증
	if err := validateEventData(event); err != nil {
		log.Printf("❌ Event data validation failed: %v", err)
		http.Error(w, "Invalid event data: "+err.Error(), http.StatusBadRequest)
		return
	}

	// 이벤트를 JSON으로 직렬화
	eventJSON, err := json.Marshal(event)
	if err != nil {
		log.Printf("❌ Error marshaling event: %v", err)
		http.Error(w, "Error processing event: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Kafka 토픽으로 메시지 전송
	topic := "user-events"
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          eventJSON,           // 메시지 내용
		Key:            []byte(event.UserID), // 파티셔닝을 위한 키 (같은 사용자는 같은 파티션)
	}, nil)
	if err != nil {
		log.Printf("❌ Failed to produce message: %v", err)
		http.Error(w, "Failed to produce message: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 메시지 전송 완료 대기 (최대 1초)
	producer.Flush(1000)

	log.Printf("✅ Event sent to Kafka successfully: UserID=%s, Type=%s", event.UserID, event.EventType)
	
	// 성공 응답 반환
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "event accepted",
		"userId":    event.UserID,
		"eventType": event.EventType,
		"timestamp": event.Timestamp,
	})
}

// 이벤트 타입별 데이터 유효성 검증 함수 - 수정된 버전
func validateEventData(event UserEvent) error {
	switch event.EventType {
	case EventLiked:
		var data LikedLiveData
		dataBytes, err := json.Marshal(event.Data)
		if err != nil {
			return errors.New("failed to marshal event data")
		}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return errors.New("invalid data format for LIKED_LIVE event")
		}
		if data.ObjectId == "" {
			return errors.New("ObjectId is required for LIKED_LIVE event")
		}
		
	case EventWatched:
		var data WatchedData
		dataBytes, err := json.Marshal(event.Data)
		if err != nil {
			return errors.New("failed to marshal event data")
		}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return errors.New("invalid data format for WATCHED event")
		}
		if data.ObjectId == "" {
			return errors.New("ObjectId is required for WATCHED event")
		}
		
	case EventClicked:
		var data ClickedItemData
		dataBytes, err := json.Marshal(event.Data)
		if err != nil {
			return errors.New("failed to marshal event data")
		}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return errors.New("invalid data format for CLICKED event")
		}
		if data.ObjectId == "" {
			return errors.New("ObjectId is required for CLICKED event")
		}
		
	case EventCategoryInterest:
		var data CategoryInterestData
		dataBytes, err := json.Marshal(event.Data)
		if err != nil {
			return errors.New("failed to marshal event data")
		}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return errors.New("invalid data format for CATEGORY_INTEREST event")
		}
		if data.Category == "" {
			return errors.New("category is required for CATEGORY_INTEREST event")
		}
		
	case EventSearch:
		var data SearchData
		dataBytes, err := json.Marshal(event.Data)
		if err != nil {
			return errors.New("failed to marshal event data")
		}
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return errors.New("invalid data format for SEARCH event")
		}
		if data.Query == "" {
			return errors.New("query is required for SEARCH event")
		}
	}
	return nil
}

// GET /health 엔드포인트 - 서버 상태 확인
func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"service":   "event-producer",
	})
}