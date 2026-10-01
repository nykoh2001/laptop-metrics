### Architecture (Diagram)

```text
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                      LOCAL HOST                                          │
│                                                                                          │
│  ┌──────────────────────────────────────┐                                                │
│  │       Security Telemetry Collector   │                                                │
│  │                                      │                                                │
│  │  ┌──────────────┐  ┌──────────────┐  │                                                │
│  │  │ Process      │  │ Network      │  │                                                │
│  │  │ Sensor       │  │ Sensor       │  │                                                │
│  │  └──────┬───────┘  └──────┬───────┘  │                                                │
│  │         │                 │          │                                                │
│  │  ┌──────▼─────────────────▼───────┐  │                                                │
│  │  │ Common Event Normalizer        │  │                                                │
│  │  │                                │  │                                                │
│  │  │ - event_id / event_time        │  │                                                │
│  │  │ - host_id / event_type         │  │                                                │
│  │  │ - allowlisted attributes only  │  │                                                │
│  │  │ - secret and PII redaction     │  │                                                │
│  │  └──────────────┬─────────────────┘  │                                                │
│  │                 │                    │                                                │
│  │  ┌──────────────▼─────────────────┐  │                                                │
│  │  │ Kafka Producer                │  │                                                │
│  │  │ bootstrap: localhost:9092     │  │                                                │
│  │  └──────────────┬─────────────────┘  │                                                │
│  │                                      │                                                │
│  │  Runtime: native, unprivileged        │                                                │
│  │  Collection policy: field allowlist   │                                                │
│  └─────────────────┼────────────────────┘                                                │
│                    │ JSON security events                                                 │
└────────────────────┼─────────────────────────────────────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────── DOCKER NETWORK ─────────────────────────────────────────┐
│                              security-telemetry-net                                       │
│                                                                                           │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                  KAFKA BROKER                                       │  │
│  │                                                                                     │  │
│  │  Topic: security-telemetry.raw                                                      │  │
│  │  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐                                │  │
│  │  │ Partition 0 │   │ Partition 1 │   │ Partition 2 │                                │  │
│  │  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘                                │  │
│  │         └──────────────────┼──────────────────┘                                       │  │
│  │                            │                                                          │  │
│  │  Topic: security-telemetry.dlq                                                       │  │
│  │  ┌───────────────────────────────────────────────┐                                   │  │
│  │  │ Invalid schema / parsing failure / retry     │                                   │  │
│  │  └───────────────────────────────────────────────┘                                   │  │
│  │                                                                                     │  │
│  │  Mounted volume: kafka-data:/var/lib/kafka/data                                     │  │
│  └────────────────────────────┬────────────────────────────────────────────────────────┘  │
│                               │                                                           │
│                               │ Consumer Group: security-telemetry-spark                  │
│                               ▼                                                           │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                              SPARK STRUCTURED STREAMING                              │  │
│  │                                                                                     │  │
│  │  ┌──────────────────┐       ┌────────────────────────────────────────────────────┐   │  │
│  │  │ Streaming Driver │──────▶│ Streaming Query                                  │   │  │
│  │  └──────────────────┘       │                                                    │   │  │
│  │                             │ - JSON parsing                                     │   │  │
│  │  ┌──────────────────┐       │ - schema validation                                │   │  │
│  │  │ Worker / Executor│──────▶│ - timestamp normalization                          │   │  │
│  │  └──────────────────┘       │ - duplicate filtering by event_id                  │   │  │
│  │                             │ - invalid event routing to DLQ                      │   │  │
│  │                             └──────────────────────┬─────────────────────────────┘   │  │
│  │                                                    │                                 │  │
│  │  Mounted volume: spark-checkpoints:/opt/spark/checkpoints                            │  │
│  └────────────────────────────────────────────────────┼────────────────────────────────┘  │
│                                                       │ JDBC batch insert                 │
│                                                       ▼                                   │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                   CLICKHOUSE                                        │  │
│  │                                                                                     │  │
│  │  Database: security_telemetry                                                       │  │
│  │                                                                                     │  │
│  │  ┌──────────────────────────┐     ┌──────────────────────────┐                       │  │
│  │  │ raw_security_events      │     │ pipeline_ingestion_log   │                       │  │
│  │  │                          │     │                          │                       │  │
│  │  │ - event_id               │     │ - batch_id               │                       │  │
│  │  │ - event_time             │     │ - consumed_count         │                       │  │
│  │  │ - host_id                │     │ - inserted_count         │                       │  │
│  │  │ - event_type             │     │ - rejected_count         │                       │  │
│  │  │ - attributes             │     │ - processed_at           │                       │  │
│  │  └──────────────────────────┘     └──────────────────────────┘                       │  │
│  │                                                                                     │  │
│  │  Mounted volume: clickhouse-data:/var/lib/clickhouse                                │  │
│  └─────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                           │
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

### Summary

- 범용 프로세스·시스템 메트릭 파이프라인에서 보안 텔레메트리 파이프라인으로 전환
- 로컬 호스트의 프로세스 및 네트워크 상태를 수집하는 비특권 네이티브 콜렉터 추가
- 수집 필드 allowlist와 비밀정보·개인정보 제외 정책 적용
- `event_id`, `event_time`, `host_id`, `event_type`, `attributes` 기반 공통 이벤트 스키마 도입
- Kafka `security-telemetry.raw` 토픽과 오류 이벤트용 `security-telemetry.dlq` 토픽 구성
- Spark Structured Streaming 기반 스키마 검증, 시간 정규화, 중복 제거 및 DLQ 분기 추가
- ClickHouse `raw_security_events`, `pipeline_ingestion_log` 테이블 구성
- Kafka 데이터, Spark 체크포인트, ClickHouse 데이터를 위한 영속 볼륨 분리
- 기존 Kafka–Spark–ClickHouse 실행 구조 유지로 이후 탐지 규칙과 분석 단계의 확장 기반 확보
- 호스트 특권 컨테이너, Docker 소켓 마운트, 전체 명령행 및 환경변수 수집 제외

### Verification

1. **End-to-end event flow**
   - 로컬 콜렉터에서 생성한 테스트 이벤트의 Kafka 수신 확인
   - Spark 처리 이후 ClickHouse `raw_security_events` 적재 확인

2. **Schema validation**
   - 필수 필드가 포함된 이벤트의 정상 처리 확인
   - `event_id` 또는 `event_time` 누락 이벤트의 DLQ 분기 확인

3. **Duplicate handling**
   - 동일한 `event_id`를 가진 이벤트 반복 발행
   - ClickHouse 중복 적재 방지 확인

4. **Restart recovery**
   - Spark 스트리밍 작업 재시작 후 체크포인트 기반 처리 재개 확인
   - Kafka 미처리 이벤트의 유실 없는 재소비 확인

5. **Security boundary**
   - 콜렉터의 비특권 사용자 실행 확인
   - 전체 환경변수, 파일 내용, 인증정보 및 불필요한 명령행 인자의 미수집 확인
   - Docker privileged 모드와 Docker 소켓 마운트 미사용 확인