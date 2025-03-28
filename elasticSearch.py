from elasticsearch import Elasticsearch
import urllib.parse

# Elasticsearch 클라이언트 생성
es = Elasticsearch(["http://localhost:9200"])

def search_kakao_products(index_name='kakao_product', **kwargs):
    """
    카카오 상품 검색을 위한 유연한 검색 함수
    
    사용 가능한 검색 파라미터:
    - title: 제목 검색 (부분 일치)
    - min_price: 최소 가격
    - max_price: 최대 가격
    - category: 카테고리 검색
    - seller: 판매자 검색
    """
    # 기본 쿼리 구조
    query = {
        "query": {
            "bool": {
                "must": []
            }
        },
        "sort": [{"_id": {"order": "desc"}}]  # 최근 추가된 순으로 정렬
    }

    # 동적 검색 조건 추가
    if 'title' in kwargs:
        query['query']['bool']['must'].append({
            "match": {"title": kwargs['title']}
        })
    
    if 'min_price' in kwargs or 'max_price' in kwargs:
        price_range = {}
        if 'min_price' in kwargs:
            price_range["gte"] = kwargs['min_price']
        if 'max_price' in kwargs:
            price_range["lte"] = kwargs['max_price']
        
        query['query']['bool']['must'].append({
            "range": {"price": price_range}
        })
    
    if 'category' in kwargs:
        query['query']['bool']['must'].append({
            "term": {"category.keyword": kwargs['category']}
        })
    
    if 'seller' in kwargs:
        query['query']['bool']['must'].append({
            "match": {"seller": kwargs['seller']}
        })

    # 검색 실행
    try:
        results = es.search(index=index_name, body=query, size=100)  # 최대 100개 결과
        return results['hits']['hits']
    except Exception as e:
        print(f"검색 중 오류 발생: {e}")
        return []

# 검색 결과 출력 함수
def print_search_results(results):
    if not results:
        print("검색 결과가 없습니다.")
        return
    
    for result in results:
        source = result['_source']
        print("상품 정보:")
        for key, value in source.items():
            print(f"{key}: {value}")
        print("-" * 50)

# 다양한 검색 예시
def main():
    print("1. 제목에 '이어폰' 포함된 상품 검색:")
    results1 = search_kakao_products(title='이어폰')
    print_search_results(results1)

    print("\n2. 10000원에서 20000원 사이 상품 검색:")
    results2 = search_kakao_products(min_price=10000, max_price=20000)
    print_search_results(results2)

    print("\n3. 특정 카테고리 상품 검색:")
    results3 = search_kakao_products(category='식품')
    print_search_results(results3)

    print("\n4. 특정 판매자 상품 검색:")
    results4 = search_kakao_products(seller='kakao')
    print_search_results(results4)

if __name__ == "__main__":
    main()