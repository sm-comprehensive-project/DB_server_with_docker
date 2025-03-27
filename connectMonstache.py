from elasticsearch import Elasticsearch
import json
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_elasticsearch_client():
    """
    Elasticsearch 클라이언트 생성 및 보안 설정
    """
    try:
        es = Elasticsearch(
            ["http://localhost:9200"],
            # 보안 설정 옵션 (필요시 주석 해제 및 수정)
            # basic_auth=("elastic", "your_password"),
            # ssl_verify=True,
            # ca_certs='/path/to/ca.crt'
        )
        
        # 클라이언트 연결 확인
        if not es.ping():
            raise ValueError("Elasticsearch 연결 실패")
        
        return es
    except Exception as e:
        logger.error(f"Elasticsearch 클라이언트 생성 중 오류: {e}")
        raise

def create_kakao_product_index(es):
    """
    kakao_product 인덱스 생성 및 매핑 설정
    
    Args:
        es (Elasticsearch): Elasticsearch 클라이언트
    """
    index_name = 'kakao_product'

    # 인덱스 매핑 정의
    index_mapping = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "korean_analyzer": {
                        "type": "custom",
                        "tokenizer": "nori_tokenizer"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "title": {
                    "type": "text", 
                    "analyzer": "korean_analyzer",
                    "search_analyzer": "standard"
                },
                "price": {"type": "float"},
                "category": {"type": "keyword"},
                "seller": {"type": "keyword"},
                "description": {
                    "type": "text", 
                    "analyzer": "korean_analyzer"
                },
                "createdAt": {"type": "date"},
                "updatedAt": {"type": "date"}
            }
        }
    }

    try:
        # 기존 인덱스 존재 시 삭제
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            logger.info(f"기존 {index_name} 인덱스 삭제")
        
        # 새 인덱스 생성
        es.indices.create(index=index_name, body=index_mapping)
        logger.info(f"{index_name} 인덱스 생성 완료")
    
    except Exception as e:
        logger.error(f"인덱스 생성 중 오류: {e}")
        raise

def check_monstache_configuration():
    """
    Monstache 설정 확인을 위한 체크리스트
    """
    logger.info("Monstache 설정 확인 체크리스트:")
    
    monstache_config_checklist = [
        "1. MongoDB 연결 설정이 정확한지 확인",
        "2. Elasticsearch 연결 설정이 정확한지 확인",
        "3. 'namespace' 설정에 'damoa.kakao_product' 포함되어 있는지 확인"
    ]
    
    for item in monstache_config_checklist:
        logger.info(item)

    # Monstache 설정 예시
    example_config = {
        "mongodb-uri": "mongodb://localhost:27017/damoa",
        "elasticsearch-urls": ["http://localhost:9200"],
        "namespace-regex": "damoa\\.kakao_product",
        "merge-namespaces": True,
        "worker-size": 2
    }
    
    logger.info("\nMonstache 설정 예시:")
    logger.info(json.dumps(example_config, indent=2))

def test_elasticsearch_connection(es):
    """
    Elasticsearch 연결 테스트
    
    Args:
        es (Elasticsearch): Elasticsearch 클라이언트
    """
    try:
        # 클러스터 상태 확인
        cluster_health = es.cluster.health()
        logger.info("Elasticsearch 클러스터 상태:")
        logger.info(f"상태: {cluster_health['status']}")
        logger.info(f"노드 수: {cluster_health['number_of_nodes']}")
        
        # 현재 존재하는 인덱스 목록 출력
        logger.info("\n현재 존재하는 인덱스:")
        indices = es.indices.get_alias("*")
        for index in indices:
            logger.info(index)
    
    except Exception as e:
        logger.error(f"Elasticsearch 연결 중 오류: {e}")
        raise

def insert_sample_document(es):
    """
    샘플 문서 삽입
    
    Args:
        es (Elasticsearch): Elasticsearch 클라이언트
    """
    index_name = 'kakao_product'
    sample_doc = {
        "title": "카카오 프렌즈 인형",
        "price": 25000,
        "category": "인형",
        "seller": "카카오스토어",
        "description": "귀여운 카카오 프렌즈 인형입니다.",
        "createdAt": "2024-03-26T12:00:00",
        "updatedAt": "2024-03-26T12:00:00"
    }
    
    try:
        result = es.index(index=index_name, body=sample_doc)
        logger.info(f"샘플 문서 삽입 완료: {result['_id']}")
    except Exception as e:
        logger.error(f"문서 삽입 중 오류: {e}")

def main():
    try:
        # Elasticsearch 클라이언트 생성
        es = create_elasticsearch_client()
        
        # Elasticsearch 연결 테스트
        test_elasticsearch_connection(es)
        
        # Monstache 설정 확인
        check_monstache_configuration()
        
        # 인덱스 생성
        create_kakao_product_index(es)
        
        # 샘플 문서 삽입
        insert_sample_document(es)
    
    except Exception as e:
        logger.error(f"주요 작업 중 오류 발생: {e}")

if __name__ == "__main__":
    main()