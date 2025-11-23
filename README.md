# Kafka Management API

Kafka Topic 관리를 위한 REST API 서버입니다.

## 프로젝트 목적

Kafka Topic 생성 및 관리 기능을 제공하는 REST API 서버입니다.

## 주요 기능

- **Kafka Topic 생성**: 새로운 Topic을 생성합니다.
- **Topic Partition 수 변경 (증가)**: 기존 Topic의 Partition 수를 증가시킵니다.
- **Topic 정보 조회**: Topic의 상세 정보를 조회합니다.
- **(확장 여지) Producer/Consumer 데이터 조작**: 향후 확장 가능한 기능입니다.

## 기술 스택

- **Java 21**
- **Spring Boot 4.0.0**
- **Gradle Kotlin DSL**
- **Spring Kafka**
- **Apache Kafka Clients**

## 설정

### application.yml

Kafka 연결 정보와 보안 설정을 `application.yml`에서 관리합니다:

```yaml
kafka:
  bootstrap-servers: localhost:9092
  security:
    protocol: PLAINTEXT
    # SSL/SASL 설정 예시
    # protocol: SSL
    # mechanism: PLAIN
    # username: your-username
    # password: your-password
```

## API 엔드포인트

### 1. Topic 생성

```http
POST /api/kafka/topics
Content-Type: application/json

{
  "topicName": "my-topic",
  "numPartitions": 3,
  "replicationFactor": 1
}
```

**응답:**
- `200 OK`: Topic 생성 성공
- `409 Conflict`: Topic이 이미 존재함

### 2. Partition 수 증가

```http
PUT /api/kafka/topics/{topicName}/partitions
Content-Type: application/json

{
  "newPartitionCount": 5
}
```

**응답:**
- `200 OK`: Partition 수 증가 성공
- `400 Bad Request`: 요청 실패 (새 파티션 수가 현재보다 작거나 같음)

### 3. Topic 정보 조회

```http
GET /api/kafka/topics/{topicName}
```

**응답:**
```json
{
  "topicName": "my-topic",
  "partitionCount": 3
}
```

## 실행 방법

### 1. 빌드

```bash
./gradlew clean build
```

### 2. 실행

```bash
./gradlew bootRun
```

### 3. 테스트

```bash
./gradlew test
```

## 사용 예시

### Topic 생성

```bash
curl -X POST http://localhost:8080/api/kafka/topics \
  -H "Content-Type: application/json" \
  -d '{
    "topicName": "test-topic",
    "numPartitions": 3,
    "replicationFactor": 1
  }'
```

### Partition 수 증가

```bash
curl -X PUT http://localhost:8080/api/kafka/topics/test-topic/partitions \
  -H "Content-Type: application/json" \
  -d '{
    "newPartitionCount": 5
  }'
```

### Topic 정보 조회

```bash
curl http://localhost:8080/api/kafka/topics/test-topic
```

## 향후 확장 계획

- Producer/Consumer 데이터 조작 기능
- Topic 삭제 기능
- Consumer Group 관리 기능
- 메시지 조회 및 전송 기능

