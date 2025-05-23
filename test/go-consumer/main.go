// package main

// import (
// 	"context"
// 	"encoding/json"
// 	"log"
// 	"os"
// 	"os/signal"
// 	"syscall"
// 	"time"

// 	"github.com/confluentinc/confluent-kafka-go/kafka"
// 	"go.mongodb.org/mongo-driver/bson"
// 	"go.mongodb.org/mongo-driver/mongo"
// 	"go.mongodb.org/mongo-driver/mongo/options"
// )

// // 이벤트 유형 정의
// type EventType string

// const (
// 	EventLiked            EventType = "LIKED_LIVE"
// 	EventWatched          EventType = "WATCHED"
// 	EventClicked          EventType = "CLICKED"
// 	EventCategoryInterest EventType = "CATEGORY_INTEREST"
// 	EventSearch           EventType = "SEARCH"
// )

// // 이벤트 기본 구조체
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

// // WatchHistory 항목 구조체
// type WatchHistoryItem struct {
// 	ContentID     string    `bson:"contentId"`
// 	Progress      float64   `bson:"progress"`
// 	Timestamp     time.Time `bson:"timestamp"`
// }

// // ClickedItem 구조체
// type ClickedItem struct {
// 	ItemID    string    `bson:"itemId"`
// 	ItemType  string    `bson:"itemType"`
// 	Category  string    `bson:"category"`
// 	Timestamp time.Time `bson:"timestamp"`
// }

// // SearchHistoryItem 구조체
// type SearchHistoryItem struct {
// 	Query       string    `bson:"query"`
// 	ResultCount int       `bson:"resultCount"`
// 	Timestamp   time.Time `bson:"timestamp"`
// }

// func main() {
// 	log.Println("Starting User Events Consumer...")

// 	// MongoDB 연결
// 	log.Printf("Connecting to MongoDB: %s", os.Getenv("MONGO_URI"))

// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()
// 	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URI")))
// 	if err != nil {
// 		log.Fatalf("Failed to connect to MongoDB: %v", err)
// 	}
// 	defer mongoClient.Disconnect(ctx)

// 	// MongoDB 연결 확인
// 	err = mongoClient.Ping(ctx, nil)
// 	if err != nil {
// 		log.Fatalf("Failed to ping MongoDB: %v", err)
// 	}
// 	log.Println("Connected to MongoDB")

// 	// User 컬렉션 참조 (데이터베이스명 수정)
// 	userCollection := mongoClient.Database("damoa").Collection("User")

// 	// Kafka 컨슈머 설정
// 	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
// 		"bootstrap.servers":        os.Getenv("KAFKA_BROKERS"),
// 		"group.id":                 "user-events-consumer-group",
// 		"auto.offset.reset":        "earliest",
// 		"enable.auto.commit":       true,
// 		"auto.commit.interval.ms":  5000,
// 		"session.timeout.ms":       6000,
// 		"heartbeat.interval.ms":    2000,
// 	})
// 	if err != nil {
// 		log.Fatalf("Failed to create consumer: %v", err)
// 	}
// 	defer consumer.Close()

// 	// 토픽 구독
// 	err = consumer.SubscribeTopics([]string{"user-events"}, nil)
// 	if err != nil {
// 		log.Fatalf("Failed to subscribe to topic: %v", err)
// 	}
// 	log.Println("Subscribed to 'user-events' topic")

// 	// 종료 시그널 처리
// 	sigchan := make(chan os.Signal, 1)
// 	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

// 	// 메시지 처리 루프
// 	run := true
// 	for run {
// 		select {
// 		case sig := <-sigchan:
// 			log.Printf("Caught signal %v: terminating\n", sig)
// 			run = false
// 		default:
// 			msg, err := consumer.ReadMessage(100 * time.Millisecond)
// 			if err != nil {
// 				// 타임아웃이면 무시
// 				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
// 					continue
// 				}
// 				log.Printf("Consumer error: %v\n", err)
// 				continue
// 			}

// 			log.Printf("Received message: %s", string(msg.Value))

// 			var event UserEvent
// 			if err := json.Unmarshal(msg.Value, &event); err != nil {
// 				log.Printf("Error parsing event: %v\n", err)
// 				continue
// 			}

// 			// 이벤트 타입에 따라 MongoDB 업데이트
// 			if err := processUserEvent(userCollection, event); err != nil {
// 				log.Printf("Error processing event: %v\n", err)
// 				continue
// 			}

// 			log.Printf("Event processed: UserID=%s, Type=%s\n", event.UserID, event.EventType)
// 		}
// 	}
// }

// // 사용자 이벤트 처리 함수
// func processUserEvent(collection *mongo.Collection, event UserEvent) error {
// 	ctx := context.Background()
// 	filter := bson.M{"email": event.UserID} // 이메일을 사용자 ID로 사용

// 	var update bson.M

// 	switch event.EventType {
// 	case EventLiked:
// 		// "좋아요" 이벤트 처리
// 		var data LikedLiveData
// 		dataBytes, _ := json.Marshal(event.Data)
// 		if err := json.Unmarshal(dataBytes, &data); err != nil {
// 			return err
// 		}

// 		// 배열에 고유한 LiveID만 추가 (중복 제거)
// 		update = bson.M{
// 			"$addToSet": bson.M{
// 				"likedLiveIds": data.LiveID,
// 			},
// 		}

// 	case EventWatched:
// 		// 시청 이벤트 처리
// 		var data WatchedData
// 		dataBytes, _ := json.Marshal(event.Data)
// 		if err := json.Unmarshal(dataBytes, &data); err != nil {
// 			return err
// 		}

// 		// 최근 시청 목록 업데이트 (중복 제거)
// 		updateRecent := bson.M{
// 			"$addToSet": bson.M{
// 				"recentWatchedIds": data.ContentID,
// 			},
// 		}

// 		if _, err := collection.UpdateOne(ctx, filter, updateRecent); err != nil {
// 			return err
// 		}

// 		// 시청 기록에 상세 정보 추가
// 		watchHistoryItem := WatchHistoryItem{
// 			ContentID:     data.ContentID,
// 			Progress:      data.Progress,
// 			Timestamp:     event.Timestamp,
// 		}

// 		update = bson.M{
// 			"$push": bson.M{
// 				"watchedHistory": watchHistoryItem,
// 			},
// 		}

// 	case EventClicked:
// 		// 클릭 이벤트 처리
// 		var data ClickedItemData
// 		dataBytes, _ := json.Marshal(event.Data)
// 		if err := json.Unmarshal(dataBytes, &data); err != nil {
// 			return err
// 		}

// 		clickedItem := ClickedItem{
// 			ItemID:    data.ItemID,
// 			ItemType:  data.ItemType,
// 			Category:  data.Category,
// 			Timestamp: event.Timestamp,
// 		}

// 		update = bson.M{
// 			"$push": bson.M{
// 				"clickedItems": clickedItem,
// 			},
// 		}

// 		// 관심 카테고리에도 추가
// 		if data.Category != "" {
// 			updateCategory := bson.M{
// 				"$addToSet": bson.M{
// 					"interestedCategories": data.Category,
// 				},
// 			}
// 			if _, err := collection.UpdateOne(ctx, filter, updateCategory); err != nil {
// 				return err
// 			}
// 		}

// 	case EventCategoryInterest:
// 		// 카테고리 관심 이벤트 처리
// 		var data CategoryInterestData
// 		dataBytes, _ := json.Marshal(event.Data)
// 		if err := json.Unmarshal(dataBytes, &data); err != nil {
// 			return err
// 		}

// 		update = bson.M{
// 			"$addToSet": bson.M{
// 				"interestedCategories": data.Category,
// 			},
// 		}

// 	case EventSearch:
// 		// 검색 이벤트 처리
// 		var data SearchData
// 		dataBytes, _ := json.Marshal(event.Data)
// 		if err := json.Unmarshal(dataBytes, &data); err != nil {
// 			return err
// 		}

// 		searchItem := SearchHistoryItem{
// 			Query:       data.Query,
// 			ResultCount: data.ResultCount,
// 			Timestamp:   event.Timestamp,
// 		}

// 		update = bson.M{
// 			"$push": bson.M{
// 				"searchHistory": searchItem,
// 			},
// 		}

// 	default:
// 		log.Printf("Unknown event type: %s", event.EventType)
// 		return nil
// 	}

// 	// MongoDB 업데이트 실행
// 	result, err := collection.UpdateOne(ctx, filter, update)
// 	if err != nil {
// 		return err
// 	}

// 	if result.MatchedCount == 0 {
// 		log.Printf("Warning: No user found with email: %s", event.UserID)
// 	}

// 	return nil
// }

package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

// WatchHistory 항목 구조체
type WatchHistoryItem struct {
	ContentID string    `bson:"contentId"`
	Progress  float64   `bson:"progress"`
	Timestamp time.Time `bson:"timestamp"`
}

// ClickedItem 구조체
type ClickedItem struct {
	ItemID    string    `bson:"itemId"`
	ItemType  string    `bson:"itemType"`
	Category  string    `bson:"category"`
	Timestamp time.Time `bson:"timestamp"`
}

func main() {
	log.Println("🚀 Starting User Events Consumer...")

	// MongoDB 연결
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://admin:secure_password@localhost:27017/damoa?authSource=admin"
	}
	log.Printf("📊 Connecting to MongoDB: %s", mongoURI)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("❌ Failed to connect to MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	// MongoDB 연결 확인
	err = mongoClient.Ping(ctx, nil)
	if err != nil {
		log.Fatalf("❌ Failed to ping MongoDB: %v", err)
	}
	log.Println("✅ Connected to MongoDB")

	// User 컬렉션 참조
	userCollection := mongoClient.Database("damoa").Collection("User")

	// Kafka 컨슈머 설정
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "kafka:29092"
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        kafkaBrokers,
		"group.id":                 "user-events-consumer-group",
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       true,
		"auto.commit.interval.ms":  5000,
		"session.timeout.ms":       6000,
		"heartbeat.interval.ms":    2000,
	})
	if err != nil {
		log.Fatalf("❌ Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// 토픽 구독
	err = consumer.SubscribeTopics([]string{"user-events"}, nil)
	if err != nil {
		log.Fatalf("❌ Failed to subscribe to topic: %v", err)
	}
	log.Println("✅ Subscribed to 'user-events' topic")

	// 종료 시그널 처리
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// 메시지 처리 루프
	run := true
	processedCount := 0
	
	for run {
		select {
		case sig := <-sigchan:
			log.Printf("🛑 Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// 타임아웃이면 무시
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("❌ Consumer error: %v\n", err)
				continue
			}

			log.Printf("📨 Received message: %s", string(msg.Value))

			var event UserEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("❌ Error parsing event: %v\n", err)
				continue
			}

			// 이벤트 타입에 따라 MongoDB 업데이트
			if err := processUserEvent(userCollection, event); err != nil {
				log.Printf("❌ Error processing event: %v\n", err)
				continue
			}

			processedCount++
			log.Printf("✅ Event processed (#%d): UserID=%s, Type=%s\n", processedCount, event.UserID, event.EventType)
		}
	}
	
	log.Printf("🏁 Consumer stopped. Total events processed: %d", processedCount)
}

// 사용자 이벤트 처리 함수
func processUserEvent(collection *mongo.Collection, event UserEvent) error {
	ctx := context.Background()
	filter := bson.M{"email": event.UserID} // 이메일을 사용자 ID로 사용

	// 사용자 존재 확인
	var existingUser bson.M
	err := collection.FindOne(ctx, filter).Decode(&existingUser)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			log.Printf("⚠️  User not found: %s", event.UserID)
			return nil // 사용자가 없으면 무시
		}
		return err
	}

	var update bson.M

	switch event.EventType {
	case EventLiked:
		// "좋아요" 이벤트 처리
		var data LikedLiveData
		dataBytes, _ := json.Marshal(event.Data)
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return err
		}

		log.Printf("💖 Processing LIKED event: LiveID=%s", data.LiveID)
		
		// 배열에 고유한 LiveID만 추가 (중복 제거)
		update = bson.M{
			"$addToSet": bson.M{
				"likedLiveIds": data.LiveID,
			},
		}

	case EventWatched:
		// 시청 이벤트 처리
		var data WatchedData
		dataBytes, _ := json.Marshal(event.Data)
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return err
		}

		log.Printf("👀 Processing WATCHED event: ContentID=%s, Progress=%.2f", data.ContentID, data.Progress)

		// 최근 시청 목록 업데이트 (중복 제거)
		updateRecent := bson.M{
			"$addToSet": bson.M{
				"recentWatchedIds": data.ContentID,
			},
		}

		if _, err := collection.UpdateOne(ctx, filter, updateRecent); err != nil {
			return err
		}

		// 시청 기록에 상세 정보 추가
		watchHistoryItem := WatchHistoryItem{
			ContentID: data.ContentID,
			Progress:  data.Progress,
			Timestamp: event.Timestamp,
		}

		update = bson.M{
			"$push": bson.M{
				"watchedHistory": watchHistoryItem,
			},
		}

	case EventClicked:
		// 클릭 이벤트 처리
		var data ClickedItemData
		dataBytes, _ := json.Marshal(event.Data)
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return err
		}

		log.Printf("🖱️  Processing CLICKED event: ItemID=%s, Category=%s", data.ItemID, data.Category)

		clickedItem := ClickedItem{
			ItemID:    data.ItemID,
			ItemType:  data.ItemType,
			Category:  data.Category,
			Timestamp: event.Timestamp,
		}

		update = bson.M{
			"$push": bson.M{
				"clickedItems": clickedItem,
			},
		}

		// 관심 카테고리에도 추가
		if data.Category != "" {
			updateCategory := bson.M{
				"$addToSet": bson.M{
					"interestedCategories": data.Category,
				},
			}
			if _, err := collection.UpdateOne(ctx, filter, updateCategory); err != nil {
				return err
			}
			log.Printf("📂 Added category to interests: %s", data.Category)
		}

	case EventCategoryInterest:
		// 카테고리 관심 이벤트 처리
		var data CategoryInterestData
		dataBytes, _ := json.Marshal(event.Data)
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return err
		}

		log.Printf("🎯 Processing CATEGORY_INTEREST event: Category=%s, Score=%.2f", data.Category, data.Score)

		update = bson.M{
			"$addToSet": bson.M{
				"interestedCategories": data.Category,
			},
		}

	case EventSearch:
		// 검색 이벤트 처리
		var data SearchData
		dataBytes, _ := json.Marshal(event.Data)
		if err := json.Unmarshal(dataBytes, &data); err != nil {
			return err
		}

		log.Printf("🔍 Processing SEARCH event: Query=%s, Results=%d", data.Query, data.ResultCount)

		// 검색 기록에 추가 (최근 20개만 유지)
		update = bson.M{
			"$push": bson.M{
				"searchHistory": bson.M{
					"$each":  []string{data.Query},
					"$slice": -20, // 최근 20개만 유지
				},
			},
		}

	default:
		log.Printf("❓ Unknown event type: %s", event.EventType)
		return nil
	}

	// MongoDB 업데이트 실행
	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}

	if result.MatchedCount == 0 {
		log.Printf("⚠️  No user found with email: %s", event.UserID)
	} else if result.ModifiedCount > 0 {
		log.Printf("✅ User data updated successfully")
	} else {
		log.Printf("ℹ️  No changes made (data already exists)")
	}

	return nil
}