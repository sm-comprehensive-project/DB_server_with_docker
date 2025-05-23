// Server URL
const SERVER_URL = "http://localhost:8000";

console.log("Testing event system...");

// Helper function to make requests and log responses
async function makeRequest(description, endpoint, method = "GET", data = null) {
  console.log(`\n${description}...`);
  
  const options = {
    method,
    headers: data ? { "Content-Type": "application/json" } : {}
  };
  
  if (data) {
    options.body = JSON.stringify(data);
  }
  
  try {
    const response = await fetch(`${SERVER_URL}${endpoint}`, options);
    const result = await response.json();
    console.log(JSON.stringify(result, null, 2));
    return result;
  } catch (error) {
    console.error(`Error: ${error.message}`);
  }
}

async function runTests() {
  // 1. Check server health
  await makeRequest("1. Checking server health", "/health");
  
  // 2. Send LIKED_LIVE event
  await makeRequest("2. Sending LIKED_LIVE event", "/events", "POST", {
    userId: "a@a.com",
    type: "LIKED_LIVE",
    data: {
      liveId: "live_test_12345"
    }
  });
  
  // 3. Send WATCHED event
  await makeRequest("3. Sending WATCHED event", "/events", "POST", {
    userId: "a@a.com",
    type: "WATCHED",
    data: {
      contentId: "content_test_67890",
      progress: 0.85
    }
  });
  
  // 4. Send CLICKED event
  await makeRequest("4. Sending CLICKED event", "/events", "POST", {
    userId: "a@a.com",
    type: "CLICKED",
    data: {
      itemId: "item_test_999",
      itemType: "product",
      category: "electronics"
    }
  });
  
  // 5. Send SEARCH event
  await makeRequest("5. Sending SEARCH event", "/events", "POST", {
    userId: "a@a.com",
    type: "SEARCH",
    data: {
      query: "테스트 스마트폰",
      resultCount: 42
    }
  });
  
  // 6. Send CATEGORY_INTEREST event
  await makeRequest("6. Sending CATEGORY_INTEREST event", "/events", "POST", {
    userId: "a@a.com",
    type: "CATEGORY_INTEREST",
    data: {
      category: "fashion",
      score: 0.9
    }
  });
  
  console.log("\n✅ All events sent! Check MongoDB for results.");
  console.log("\n📋 To check results in MongoDB, run:");
  console.log("docker exec -it $(docker ps -q -f name=mongo1) mongosh --authenticationDatabase admin -u admin -p secure_password");
  console.log("\n🔍 Then in MongoDB shell:");
  console.log("use damoa");
  console.log('db.User.findOne({"email": "a@a.com"})');
  console.log("\n📊 Check specific arrays:");
  console.log('db.User.findOne({"email": "a@a.com"}, {"likedLiveIds": 1, "recentWatchedIds": 1, "watchedHistory": 1, "clickedItems": 1, "searchHistory": 1, "interestedCategories": 1})');
}

runTests();