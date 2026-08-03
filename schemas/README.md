# Kafka metric input contract

이 디렉터리는 collector가 Kafka에 발행하는 JSON 메시지의 입력 계약을 문서화한다.
`host_metric.json`과 `process_metric.json`은 각 메시지 유형의 예시다.

## 공통 규칙

| 항목 | 계약 |
|---|---|
| 인코딩 | UTF-8 |
| 메시지 형식 | JSON object |
| Kafka message key | 없음 (`null`) |
| 필드 필수 여부 | 아래에 나열된 모든 필드는 필수 |
| `timestamp` | metric 수집 시각을 나타내는 UTC ISO 8601 문자열 |
| `timestamp` 형식 | `YYYY-MM-DDTHH:mm:ss.ffffffZ` (마이크로초 정밀도) |
| percent 필드 | JSON `number`, 단위 `%` |
| bytes 필드 | JSON `integer`, 단위 byte |

한 번의 수집 cycle에서 생성된 host metric과 process metric은 동일한 `timestamp`를
사용한다. Topic 이름은 환경 변수로 변경할 수 있으며 아래 이름은 기본값이다.

## Host metric

- 예시: [`host_metric.json`](./host_metric.json)
- 기본 Kafka topic: `host_metrics`
- topic 환경 변수: `HOST_METRICS_TOPIC`
- 발행 단위: 수집 cycle마다 host당 1건

| 필드 | JSON 타입 | 단위 | 의미 |
|---|---|---|---|
| `timestamp` | string | UTC | metric 수집 시각 |
| `hostname` | string | - | metric을 수집한 host 이름 |
| `cpu_usage_percent` | number | % | host 전체 CPU 사용률 |
| `memory_usage_percent` | number | % | 물리 메모리 사용률 |
| `total_memory_bytes` | integer | byte | 전체 물리 메모리 크기 |
| `available_memory_bytes` | integer | byte | 즉시 사용 가능한 물리 메모리 크기 |
| `swap_usage_percent` | number | % | swap 사용률 |
| `total_swap_bytes` | integer | byte | 전체 swap 크기 |
| `used_swap_bytes` | integer | byte | 사용 중인 swap 크기 |
| `disk_usage_percent` | number | % | collector가 관찰하는 파일시스템(`/`)의 공간 사용률 |
| `disk_read_bytes` | integer | byte | host 시작 이후 disk에서 읽은 누적 byte 수 |
| `disk_write_bytes` | integer | byte | host 시작 이후 disk에 쓴 누적 byte 수 |
| `network_bytes_sent` | integer | byte | host 시작 이후 모든 network interface가 보낸 누적 byte 수 |
| `network_bytes_received` | integer | byte | host 시작 이후 모든 network interface가 받은 누적 byte 수 |

`disk_read_bytes`, `disk_write_bytes`, `network_bytes_sent`,
`network_bytes_received`는 구간별 사용량이 아닌 누적 counter다. 처리 단계에서 두 측정값의
차이를 경과 시간으로 나누면 초당 처리량을 계산할 수 있다. Host가 재시작되거나 운영체제의
counter가 초기화되면 값이 감소할 수 있다.

## Process metric

- 예시: [`process_metric.json`](./process_metric.json)
- 기본 Kafka topic: `process_metrics`
- topic 환경 변수: `PROCESS_METRICS_TOPIC`
- 발행 단위: 수집 cycle마다 읽을 수 있는 process당 1건

| 필드 | JSON 타입 | 단위 | 의미 |
|---|---|---|---|
| `timestamp` | string | UTC | metric 수집 시각 |
| `pid` | integer | - | 수집 시점의 process ID |
| `process_name` | string | - | 운영체제가 제공한 process 이름 |
| `cpu_usage_percent` | number | % | process CPU 사용률. 여러 CPU core를 사용하면 100보다 클 수 있음 |
| `memory_usage_percent` | number | % | 전체 물리 메모리 대비 process 메모리 사용률 |
| `rss_memory_bytes` | integer | byte | process가 점유한 resident set size |
| `process_status` | string | - | 운영체제와 `psutil`이 제공한 process 상태 문자열 |

PID는 process 종료 후 다른 process에 재사용될 수 있으므로 장기간 유지되는 고유 식별자로
간주하지 않는다. 권한 부족, 수집 도중 종료 등의 이유로 읽을 수 없는 process는 해당 cycle의
메시지가 발행되지 않는다.

## 계약 변경 규칙

필드를 추가·삭제하거나 타입, 단위 또는 의미를 변경할 때는 다음 항목을 함께 수정한다.

1. `collector/models.py`의 metric 모델
2. 이 디렉터리의 해당 JSON 예시
3. 이 문서의 필드 정의
4. serializer 및 downstream parsing 테스트

기존 필드의 삭제나 타입·의미 변경은 Spark consumer와 저장 데이터에 영향을 주는 호환성
파괴 변경이다. 스키마 변경이 빈번해지거나 여러 consumer가 생기기 전까지는 별도의 Schema
Registry 없이 이 문서와 자동화된 테스트로 계약을 관리한다.
