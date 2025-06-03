package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
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
	UserID    string      `json:"userId"`    // 사용자 이메일
	EventType EventType   `json:"type"`      // 이벤트 타입
	Data      interface{} `json:"data"`      // 이벤트 상세 데이터
	Timestamp time.Time   `json:"timestamp"` // 발생 시간
}

// 각 이벤트 타입별 데이터 구조체 정의
type LikedLiveData struct {
	ObjectId string `json:"ObjectId"`
}

type WatchedData struct {
	ObjectId string `json:"ObjectId"`
}

type ClickedItemData struct {
	ObjectId   string `json:"ObjectId"`
}

type CategoryInterestData struct {
	Category string `json:"category"`
}

type SearchData struct {
	Query       string `json:"query"`
	ResultCount int    `json:"resultCount"`
}

// WatchHistory 항목 구조체
type WatchHistoryItem struct {
	ObjectId  string    `bson:"ObjectId"`
	Timestamp time.Time `bson:"timestamp"`
}

// ClickedItem 구조체
type ClickedItem struct {
	ObjectId    string    `bson:"ObjectId"`
	Timestamp time.Time `bson:"timestamp"`
}

// SearchHistoryItem 구조체 (검색 기록에 타임스탬프 추가)
type SearchHistoryItem struct {
	Query     string    `bson:"query"`
	Timestamp time.Time `bson:"timestamp"`
}

// User 구조체 정의 (디버깅용)
type User struct {
	ID                   interface{}   `bson:"_id,omitempty"`
	Email                string        `bson:"email"`
	Name                 string        `bson:"name,omitempty"`
	LikedLiveIds         []string      `bson:"likedLiveIds,omitempty"`
	RecentWatchedIds     []string      `bson:"recentWatchedIds,omitempty"`
	WatchedHistory       []interface{} `bson:"watchedHistory,omitempty"`
	ClickedItems         []interface{} `bson:"clickedItems,omitempty"`
	InterestedCategories []string      `bson:"interestedCategories,omitempty"`
	SearchHistory        []interface{} `bson:"searchHistory,omitempty"`
	CreatedAt            time.Time     `bson:"createdAt,omitempty"`
	UpdatedAt            time.Time     `bson:"updatedAt,omitempty"`
}

func main() {
	log.Println("🚀 Starting User Events Consumer...")

	// MongoDB 연결
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb+srv://admin:@clusterdamoa2.arr0haj.mongodb.net/damoa?retryWrites=true&w=majority&appName=ClusterDamoa2"
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
	userCollection := mongoClient.Database("damoa").Collection("user")

	// 🔍 사용자 컬렉션 상태 확인 (디버깅용)
	quickUserCheck(userCollection)

	// Kafka 컨슈머 설정
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "kafka:29092"
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       kafkaBrokers,
		"group.id":                "user-events-consumer-group",
		"auto.offset.reset":       "earliest",
		"enable.auto.commit":      true,
		"auto.commit.interval.ms": 5000,
		"session.timeout.ms":      6000,
		"heartbeat.interval.ms":   2000,
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

// 🔍 사용자 컬렉션 빠른 확인 함수
func quickUserCheck(collection *mongo.Collection) {
	ctx := context.Background()

	log.Println("🔍 ===========================================")
	log.Println("🔍 QUICK USER COLLECTION CHECK")
	log.Println("🔍 ===========================================")

	// 총 사용자 수
	count, err := collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		log.Printf("❌ Error counting users: %v", err)
		return
	}
	log.Printf("📊 Total users in collection: %d", count)

	if count == 0 {
		log.Println("⚠️  No users found in collection!")
		return
	}

	// 처음 10명의 사용자 이메일 확인
	cursor, err := collection.Find(ctx, bson.M{}, options.Find().SetProjection(bson.M{"email": 1, "name": 1}).SetLimit(10))
	if err != nil {
		log.Printf("❌ Error finding users: %v", err)
		return
	}
	defer cursor.Close(ctx)

	log.Println("📧 First 10 users:")
	i := 0
	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			continue
		}
		i++
		email := result["email"]
		name := result["name"]
		emailStr := fmt.Sprintf("%v", email)
		log.Printf("  %d. Email: '%s' (len: %d, type: %T) | Name: '%v'",
			i, emailStr, len(emailStr), email, name)

		// 공백 체크
		if emailStr != strings.TrimSpace(emailStr) {
			log.Printf("     ⚠️  WARNING: Email has leading/trailing spaces!")
		}
	}

	// 이메일 필드 분석
	pipeline := []bson.M{
		{
			"$project": bson.M{
				"email":      1,
				"emailType":  bson.M{"$type": "$email"},
				"emailLen":   bson.M{"$strLenCP": "$email"},
				"hasSpaces":  bson.M{"$ne": []interface{}{bson.M{"$trim": bson.M{"input": "$email"}}, "$email"}},
			},
		},
		{"$limit": 5},
	}

	cursor2, err := collection.Aggregate(ctx, pipeline)
	if err == nil {
		defer cursor2.Close(ctx)
		log.Println("🔬 Email field analysis (first 5):")
		for cursor2.Next(ctx) {
			var result bson.M
			if err := cursor2.Decode(&result); err != nil {
				continue
			}
			log.Printf("  📧 '%v' | Type: %v | Length: %v | Has Spaces: %v",
				result["email"], result["emailType"], result["emailLen"], result["hasSpaces"])
		}
	}

	log.Println("🔍 ===========================================")
}

// 상세한 사용자 컬렉션 확인 함수 (필요시 사용)
func detailedUserCheck(collection *mongo.Collection) {
	ctx := context.Background()

	log.Println("🔍 ===========================================")
	log.Println("🔍 DETAILED USER COLLECTION CHECK")
	log.Println("🔍 ===========================================")

	// 첫 3개 문서의 상세 정보 확인
	cursor, err := collection.Find(ctx, bson.M{}, options.Find().SetLimit(3))
	if err != nil {
		log.Printf("❌ Error finding sample documents: %v", err)
		return
	}
	defer cursor.Close(ctx)

	count := 0
	for cursor.Next(ctx) {
		count++
		var user User
		if err := cursor.Decode(&user); err != nil {
			log.Printf("❌ Error decoding user %d: %v", count, err)
			continue
		}

		log.Printf("👤 User %d:", count)
		log.Printf("  📧 Email: '%s'", user.Email)
		log.Printf("  👤 Name: '%s'", user.Name)
		log.Printf("  💖 Liked Lives: %d items", len(user.LikedLiveIds))
		log.Printf("  👀 Recent Watched: %d items", len(user.RecentWatchedIds))
		log.Printf("  📚 Watch History: %d items", len(user.WatchedHistory))
		log.Printf("  🖱️  Clicked Items: %d items", len(user.ClickedItems))
		log.Printf("  🎯 Interested Categories: %d items", len(user.InterestedCategories))
		log.Printf("  🔍 Search History: %d items", len(user.SearchHistory))
		log.Printf("  📅 Created: %v", user.CreatedAt)
		log.Printf("  🔄 Updated: %v", user.UpdatedAt)
		log.Println("  ---")
	}

	log.Println("🔍 ===========================================")
}

// 특정 이메일 검색 테스트 함수
func testEmailSearch(collection *mongo.Collection, testEmail string) {
	ctx := context.Background()
	log.Printf("🧪 Testing email search for: '%s'", testEmail)

	// 정확한 매치 테스트
	count1, _ := collection.CountDocuments(ctx, bson.M{"email": testEmail})
	log.Printf("  📊 Exact match: %d results", count1)

	// 대소문자 구분 없는 매치 테스트
	count2, _ := collection.CountDocuments(ctx, bson.M{
		"email": bson.M{
			"$regex":   "^" + regexp.QuoteMeta(testEmail) + "$",
			"$options": "i",
		},
	})
	log.Printf("  📊 Case-insensitive match: %d results", count2)

	// 트림된 매치 테스트
	trimmedEmail := strings.TrimSpace(testEmail)
	count3, _ := collection.CountDocuments(ctx, bson.M{"email": trimmedEmail})
	log.Printf("  📊 Trimmed match: %d results", count3)
}

// 사용자 이벤트 처리 함수
func processUserEvent(collection *mongo.Collection, event UserEvent) error {
	ctx := context.Background()

	// 이메일 값 정리 (앞뒤 공백 제거)
	userEmail := strings.TrimSpace(event.UserID)

	// 기본 필터 (정확한 매치)
	filter := bson.M{"email": userEmail}

	// 디버깅을 위한 로그 추가
	log.Printf("🔍 Searching for user with email: '%s'", userEmail)

	// 사용자 존재 확인
	var existingUser bson.M
	err := collection.FindOne(ctx, filter).Decode(&existingUser)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// 대소문자 구분 없이 재검색
			log.Printf("🔄 Retrying with case-insensitive search...")
			caseInsensitiveFilter := bson.M{
				"email": bson.M{
					"$regex":   "^" + regexp.QuoteMeta(userEmail) + "$",
					"$options": "i", // case-insensitive
				},
			}

			err = collection.FindOne(ctx, caseInsensitiveFilter).Decode(&existingUser)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					// 더 자세한 디버깅을 위해 모든 사용자 조회해보기
					cursor, debugErr := collection.Find(ctx, bson.M{})
					if debugErr == nil {
						var allUsers []bson.M
						cursor.All(ctx, &allUsers)
						log.Printf("🐛 DEBUG: Total users in database: %d", len(allUsers))
						for i, user := range allUsers {
							if email, ok := user["email"]; ok {
								log.Printf("🐛 DEBUG: User %d email: '%v' (type: %T)", i+1, email, email)
							}
						}
						cursor.Close(ctx)
					}

					log.Printf("⚠️  User not found: %s", userEmail)
					return nil // 사용자가 없으면 무시
				}
				return err
			}

			// 대소문자 구분 없는 검색에서 찾았으면 필터를 업데이트
			filter = caseInsensitiveFilter
		} else {
			return err
		}
	}

	// 사용자를 찾았을 때 로그
	if email, ok := existingUser["email"]; ok {
		log.Printf("✅ Found user with email: '%v'", email)
	}

	var update bson.M

	switch event.EventType {
	case EventLiked:
		// "좋아요" 이벤트 처리
		var data LikedLiveData
		if err := parseEventData(event.Data, &data); err != nil {
			return err
		}

		log.Printf("💖 Processing LIKED event: ObjectId=%s", data.ObjectId)

		// 입력값 검증
		if data.ObjectId == "" {
			log.Printf("⚠️  Empty ObjectId in LIKED event")
			return nil
		}

		update = bson.M{
			"$addToSet": bson.M{
				"likedLiveIds": data.ObjectId,
			},
		}

	case EventWatched:
		// 시청 이벤트 처리
		var data WatchedData
		if err := parseEventData(event.Data, &data); err != nil {
			return err
		}

		log.Printf("👀 Processing WATCHED event: ObjectId=%s", data.ObjectId)

		// 입력값 검증
		if data.ObjectId == "" {
			log.Printf("⚠️  Empty ObjectId in WATCHED event")
			return nil
		}

		// 트랜잭션으로 여러 업데이트 처리
		session, err := collection.Database().Client().StartSession()
		if err != nil {
			return err
		}
		defer session.EndSession(ctx)

		_, err = session.WithTransaction(ctx, func(sc mongo.SessionContext) (interface{}, error) {
			// 최근 시청 목록 업데이트
			updateRecent := bson.M{
				"$addToSet": bson.M{
					"recentWatchedIds": data.ObjectId,
				},
			}
			if _, err := collection.UpdateOne(sc, filter, updateRecent); err != nil {
				return nil, err
			}

			// 시청 기록에 상세 정보 추가
			watchHistoryItem := WatchHistoryItem{
				ObjectId:  data.ObjectId,
				Timestamp: event.Timestamp,
			}
			updateHistory := bson.M{
				"$push": bson.M{
					"watchedHistory": watchHistoryItem,
				},
			}
			if _, err := collection.UpdateOne(sc, filter, updateHistory); err != nil {
				return nil, err
			}

			return nil, nil
		})

		return err

	case EventClicked:
		// 클릭 이벤트 처리
		var data ClickedItemData
		if err := parseEventData(event.Data, &data); err != nil {
			return err
		}

		log.Printf("🖱️  Processing CLICKED event: ObjectId=%s", data.ObjectId)

		// 입력값 검증
		if data.ObjectId == "" {
			log.Printf("⚠️  Empty ObjectId in CLICKED event")
			return nil
		}

		clickedItem := ClickedItem{
			ObjectId:    data.ObjectId,
			Timestamp: event.Timestamp,
		}

		update = bson.M{
			"$push": bson.M{
				"clickedItems": clickedItem,
			},
		}

	case EventCategoryInterest:
		// 카테고리 관심 이벤트 처리
		var data CategoryInterestData
		if err := parseEventData(event.Data, &data); err != nil {
			return err
		}

		log.Printf("🎯 Processing CATEGORY_INTEREST event: Category=%s", data.Category)

		// 입력값 검증
		if data.Category == "" {
			log.Printf("⚠️  Empty Category in CATEGORY_INTEREST event")
			return nil
		}

		update = bson.M{
			"$addToSet": bson.M{
				"interestedCategories": data.Category,
			},
		}

	case EventSearch:
		// 검색 이벤트 처리
		var data SearchData
		if err := parseEventData(event.Data, &data); err != nil {
			return err
		}

		log.Printf("🔍 Processing SEARCH event: Query=%s, Results=%d", data.Query, data.ResultCount)

		// 입력값 검증
		if data.Query == "" {
			log.Printf("⚠️  Empty Query in SEARCH event")
			return nil
		}

		// 검색 기록에 타임스탬프와 함께 추가
		searchItem := SearchHistoryItem{
			Query:     data.Query,
			Timestamp: event.Timestamp,
		}

		update = bson.M{
			"$push": bson.M{
				"searchHistory": bson.M{
					"$each":  []SearchHistoryItem{searchItem},
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
		log.Printf("⚠️  No user found with email: %s", userEmail)
	} else if result.ModifiedCount > 0 {
		log.Printf("✅ User data updated successfully")
	} else {
		log.Printf("ℹ️  No changes made (data already exists)")
	}

	return nil
}

// 이벤트 데이터 파싱 헬퍼 함수
func parseEventData(data interface{}, target interface{}) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(dataBytes, target)
}