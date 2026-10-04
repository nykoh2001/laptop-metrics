# Phase 1 Implementation Report

## 1. 기존 구조에서 파악한 내용

- 기존 collector는 호스트 CPU, 메모리, 디스크, 네트워크 사용량과 프로세스별 자원 사용량을 수집했다.
- Kafka에는 `host_metrics`, `process_metrics` 두 topic으로 메트릭을 발행했다.
- Spark job은 Kafka 메시지를 파싱하여 console sink로 출력했으며, ClickHouse로 데이터를 전달하는 실제 sink는 없었다.
- ClickHouse에는 호스트 및 프로세스 메트릭용 테이블 DDL이 있었지만 Spark와 연결되지 않았다.
- macOS collector는 native process로 실행하고 Kafka, Spark, ClickHouse는 Docker Compose로 실행하는 구조였다.
- 저장소에 osquery 연동은 없었다. 이번 단계에서도 osquery 설치, 감사 설정 변경 또는 추가 권한 부여를 도입하지 않았다.
- 기존 테스트는 collector, serializer, Kafka integration, Spark parsing 및 schema 일관성 검증을 중심으로 구성되어 있었다.

## 2. 변경한 파일과 역할

| 파일 또는 디렉토리 | 역할 |
| --- | --- |
| `collector/collector.py` | 프로세스 상태, 네트워크 연결 및 optional Docker lifecycle collector 구현 |
| `collector/events.py` | 결정적 event ID와 공통 `SecurityEvent` 생성 규칙 구현 |
| `collector/models.py` | 버전이 있는 공통 보안 이벤트 모델 정의 |
| `collector/serializers.py` | 이벤트별 payload allowlist와 직렬화 안전성 검증 |
| `collector/utils.py` | UTC 시각, HMAC 기반 host/user 비식별화, 홈 경로 정규화 및 제한된 환경변수 로딩 |
| `collector/config.py` | 단일 `security_events` topic과 collector 환경 설정 정의 |
| `collector/kafka_producer.py` | `event_id`를 Kafka message key로 사용하는 보안 이벤트 발행 |
| `collector/main.py` | 일반 사용자 권한의 native collector 실행 진입점 |
| `spark/jobs/security_events_to_clickhouse.py` | Kafka 이벤트 파싱, 최소 저장 안전성 검증 및 ClickHouse HTTP 적재 |
| `clickhouse/init/001_create_security_events.sql` | 공통 보안 이벤트 테이블 및 보존 정책 정의 |
| `schemas/security_event.schema.json` | 공통 이벤트와 이벤트별 payload의 폐쇄형 JSON Schema 정의 |
| `schemas/security_event.example.json` | 공통 이벤트 예시 |
| `docker-compose.yml` | Kafka 이후 파이프라인 설정, loopback port binding 및 Spark checkpoint 경로 구성 |
| `launchd/com.local.security-telemetry-collector.plist.example` | macOS native collector 실행 예시 |
| `scripts/` | collector 실행·서비스 설치와 ClickHouse schema 적용 스크립트 |
| `.env.example` | phase 1 실행에 필요한 최소 환경변수 예시 |
| `README.md` | phase 1 범위와 최소 실행·확인 방법으로 전면 재작성 |
| `AGENTS.md` | endpoint security telemetry 프로젝트 기준으로 개요와 지침 갱신 |
| `docs/prompts/` | 이번 작업에 입력된 요구사항 원문 보관 |
| `tests/phase-1-collector/` | 이벤트 모델, collector, privacy, 권한 저하 관련 테스트와 이슈 기록 |
| `tests/retention-and-spark-sink/` | Kafka·ClickHouse 보존, Spark sink, 보안 경계 테스트와 이슈 기록 |

기존 CPU·메모리 중심 모델, schema, Spark console job, ClickHouse metric DDL과 이에 종속된 테스트는 참조 관계를 확인한 뒤 제거했다.

## 3. 새로운 이벤트 흐름

```text
macOS native collector
  ├─ process.state
  ├─ network.connection
  └─ docker.container.lifecycle (optional)
        ↓
SecurityEvent 1.0.0
        ↓
Kafka security_events (event_id key)
        ↓
Spark parse + minimum storage safety guard
        ↓
ClickHouse security_events
```

- 공통 필드는 `schema_version`, `event_id`, `event_time`, `collected_at`, `host_id`, `source`, `event_type`, `action`, `payload`이다.
- `event_id`는 schema, host, source, event type, action, event time과 canonical payload를 UUIDv5 입력으로 사용한다. 같은 이벤트를 다시 수집하면 동일한 ID가 생성된다.
- `event_time`은 관측 대상 이벤트의 시각이며 `collected_at`은 collector가 이벤트를 만든 시각이다.
- host ID와 사용자 식별자는 원본 값을 전송하지 않고 비밀 salt를 사용하는 HMAC-SHA256 값으로 표현한다.
- 사용자 홈이 포함된 실행 파일 경로는 가능한 경우 `$HOME`으로 정규화한다.
- command line, arguments, 환경변수, 파일 내용, 브라우저 정보, credential, token 및 secret 관련 필드는 payload allowlist에 포함하지 않으며 serializer에서 거부한다.
- system-wide 네트워크 조회가 제한되면 현재 권한으로 읽을 수 있는 프로세스별 연결 조회로 저하하며, 권한 부족은 민감하지 않은 집계값으로만 기록한다.
- Docker lifecycle 수집은 Docker CLI에 접근 가능한 경우에만 동작한다. Docker socket mount 없이 최소 필드만 요청하며, 사용할 수 없으면 다른 collector는 계속 실행된다.

## 4. 테스트 및 end-to-end 확인 결과

| 검증 항목 | 결과 |
| --- | --- |
| 전체 pytest (Docker Kafka integration 활성화) | 42 passed |
| Kafka integration test | Docker Kafka `localhost:9092` 대상 실제 publish/consume 통과 |
| Spark parsing, storage guard 및 mocked ClickHouse HTTP 전달 | 통과 |
| Ruff format 및 lint | 통과 |
| mypy strict | 통과 |
| Python compileall | 통과 |
| Docker Compose config validation | 통과 |
| shell syntax 및 launchd plist lint | 통과 |
| sdist/wheel build 및 pip dependency check | 통과 |
| 실제 Kafka → Spark → ClickHouse smoke test | Docker Kafka와 Spark를 거쳐 ClickHouse 적재 확인 |
| 실제 host process/network collector smoke test | macOS 일반 사용자 권한의 native collector로 확인 |

최근 실제 smoke test에서는 Kafka `security_events` offset이 2,241에서 2,735로 494 증가했고, 같은 실행 구간에 ClickHouse의 행도 494 증가했다 (`process.state` 389건, `network.connection` 105건). collector는 로컬 `.env`의 안정적인 host ID salt를 읽어 실행했고, 일부 프로세스 네트워크 레코드의 접근 거부는 정상적인 저하 상태로 처리되었다. 누적 ClickHouse 행은 `process.state` 2,076건, `network.connection` 561건, `docker.container.lifecycle` 32건이었다. host telemetry 권한을 확대하거나 sudo, root, privileged container 등의 권한은 사용하지 않았다.

ClickHouse CPU 사용량 조사 시 persistent volume에 `system.trace_log` 약 10.16 GiB, `system.text_log` 약 984 MiB가 쌓여 있었고, `system.metric_log` 및 `system.asynchronous_metric_log`의 background merge가 관찰되었다. 이후 과거 메트릭 partition과 사용하지 않는 애플리케이션 테이블을 삭제하고 ClickHouse 데이터 볼륨을 제거했다. ClickHouse는 현재 데이터 디렉터리를 컨테이너 내부 tmpfs에 기록하므로 재시작·재생성 시 데이터가 사라지며, 내장 system log 테이블은 이미지 설정에 따라 다시 생성·적재된다. 로그 적재 최적화는 이번 변경에 포함하지 않았다.

Kafka 4.0.2 이미지가 `/mnt/shared/config` 및 `/etc/kafka/secrets`도 `VOLUME`으로 선언해, Compose에서 별도 경로를 지정하지 않으면 빈 anonymous volume 두 개를 자동 생성했다. 두 경로는 조사 당시 각각 4 KiB의 빈 디렉터리였고 broker log는 `/var/lib/kafka/data` 아래 `laptop-metrics-kafka-data` named volume에 저장되어 있었다. Compose에서 두 scratch 경로를 tmpfs로 덮어 anonymous volume 생성을 방지하고, named Kafka 데이터 볼륨만 유지한다.

### 2026-10-04 phase 1 보완: 주기, 보존 및 sink 배치

- Collector 기본 주기를 30초로 설정하고, collector 설정값을 `.env`에서 읽도록 정리했다. `.env.example`에는 실제 비밀값 대신 실행용 샘플 값을 유지한다. Collector는 한 주기를 끝낸 뒤 interval만큼 대기하므로 수집 주기가 겹치지 않는다. process/network collector는 매 주기 전체 상태 스냅샷을 발행하며, 이벤트 의미를 바꾸거나 요구된 이벤트를 누락시키는 deduplication은 추가하지 않았다.
- Kafka `security_events` topic에 시간 기반 `retention.ms=86400000`을 적용했다. 저유량 topic의 만료 세그먼트도 정리되도록 `segment.ms=3600000` 및 broker cleanup 주기를 확인했다. Kafka 삭제는 세그먼트 단위이므로 1일은 정확한 초 단위 삭제 시각이 아니다. Spark가 1일 넘게 중단되면 Kafka에서 재처리할 이벤트가 만료될 수 있다.
- ClickHouse 기본 압축과 MergeTree 설정을 확인하고 원본 `security_events`에 `collected_at + INTERVAL 7 DAY` TTL을 사용했다. 수집 후 7일 보관이 목적이므로 과거 process 상태의 `event_time`보다 collector 수집 시각을 기준으로 삼았다. TTL은 비동기 merge 시 정리되며, 기존 7일 초과 데이터도 TTL 대상이지만 즉시 삭제되지는 않는다. 테이블 재생성이나 데이터 볼륨 삭제를 통한 TTL 적용은 하지 않았다.
- `write_to_clickhouse`의 30초 processing-time trigger는 30초마다 스냅샷을 내보내는 현재 collector에 맞춘 설정이다. 이전 1초 trigger는 새 관측 이벤트가 없는 구간에도 빈 micro-batch 확인을 반복하고 수집 주기 사이에 30회 실행될 수 있었다. 30초 trigger는 불필요한 polling을 줄이며, batch 처리 완료 시점에 따라 sink 지연이 최대 한 수집 간격 정도 늘 수 있다. phase 1에는 실시간 alert 기능이 없어 이 지연을 감수하는 선택이다.
- Kafka 레코드별 insert는 사용하지 않는다. `write_to_clickhouse`가 Spark micro-batch를 `insert_batch`로 전달하고, `insert_batch`가 DataFrame의 `toJSON().foreachPartition(...)`으로 분산 처리한다. 각 partition의 `insert_partition`이 최대 500행 또는 1MiB로 버퍼링한 뒤 `_post_clickhouse_rows`를 호출한다. 따라서 ClickHouse 요청 단위는 Spark partition 내의 bounded chunk이며, 대량 데이터를 driver로 `collect()`하지 않는다.
- Spark checkpoint는 기존과 같이 컨테이너 내부 `/tmp/laptop-metrics/security_events_clickhouse`를 사용한다. 재시도 또는 같은 topic을 읽는 별도 query는 중복 insert를 만들 수 있다. exactly-once는 보장하지 않으며 컨테이너 재생성으로 checkpoint가 사라지면 새 query가 `latest`부터 시작해 미처리 이벤트를 놓칠 수 있다.

#### 실제 용량 관측과 추정

- 측정 당시 호스트 파일시스템은 약 20 GiB 사용 가능했고 약 96% 사용 상태였다. 따라서 수십 GiB 규모의 여유가 있다고 단정할 수 없으며, 호스트 디스크를 계속 관찰해야 한다. Docker daemon의 데이터 루트는 `/var/lib/docker`이고 Kafka 로그는 `laptop-metrics-kafka-data` named volume에 저장된다. ClickHouse 데이터 디렉터리는 2 GiB 컨테이너 tmpfs이며 측정 시 약 153 MiB를 사용하고 약 1.8 GiB가 남아 있었다.
- Kafka 과거 topic 측정에서는 296,302건의 JSON 본문이 146,820,753 bytes(평균 약 495.5 bytes/event)였고 topic 디스크 사용량은 약 178.2 MB였다. retention 적용 후 과거 세그먼트는 정리되어 topic 크기가 0으로 관측되었으며, 이후 smoke test 직후 topic log는 약 1.04 MB였다.
- 실제 collector smoke test에서는 572건, 직렬화 크기 282,597 bytes(평균 약 494.1 bytes/event)가 생성됐다. 같은 구간 Kafka offset이 572 증가하고 ClickHouse 행도 572 증가했다.
- ClickHouse smoke 데이터는 2,312행, `system.parts` 기준 68,162 bytes였고 평균 물리 크기는 약 29.5 bytes/row였다. 작은 표본이며 중복 sink 결과가 포함되어 장기 저장량의 보장값으로 쓰지 않는다. 이 값으로 210만 행을 단순 환산하면 약 62 MB다. 원본 이벤트 직렬화 평균 494~496 bytes를 기준으로 210만 건의 논리 JSON 크기는 약 1.04 GB다. Kafka 30만 건은 약 149 MB의 논리 JSON 크기에 해당하며, 실제 broker 디스크 크기는 세그먼트 및 Kafka 저장 오버헤드와 압축 상태에 따라 달라진다.
- 210만 건 추정은 약 3.5 records/sec를 7일 유지한다고 가정한 수치일 뿐이다. 실제 수집량이 달라지면 저장량도 비례해 달라진다. ClickHouse merge 작업에는 추가 임시 공간이 필요하다. 현재 CH tmpfs의 약 1.8 GiB 여유는 위 단순 환산보다 크지만, 실제 파티션 크기·진단 로그·동시 merge에 필요한 공간까지 보장하지 않는다. 호스트 전체가 96% 사용 상태여서 디스크 부족 위험은 남아 있다.

#### 추가 검증

| 검증 항목 | 결과 |
| --- | --- |
| 전체 pytest | 48 passed, 1 skipped (외부 Kafka integration marker 비활성) |
| collector 30초 기본값 및 `.env.example` 설정 일관성 | 테스트 통과 |
| Kafka retention 및 ClickHouse 7일 TTL 설정 | 설정 검증 테스트 통과 |
| Spark ClickHouse 배치 요청 및 30초 trigger mock 검증 | 통과; ClickHouse 요청은 레코드별이 아닌 partition chunk 단위 |
| Ruff lint/format, mypy, `git diff --check` | 통과 |
| 실제 collector → Kafka → Spark → ClickHouse smoke test | 572개 수집, Kafka offset +572, ClickHouse 행 +572 확인 |

실제 Spark insert query log에서 요청별 행 수 `[37, 500, 35]`를 관측했다. 이는 여러 partition/chunk의 bounded insert 결과이며 단일 요청을 이벤트마다 반복하지 않는다. 별도 checkpoint로 같은 topic을 소비한 임시 Spark query 두 개가 동시에 실행된 이전 smoke 구간에서는 중복 행이 생겼다. 이 결과는 병렬 consumer/query 구성에서 중복 쓰기 가능성을 보여주며 exactly-once 보장을 뜻하지 않는다.

## 5. 남아 있는 환경 제약

### 현재 권한으로 확보한 가시성

- collector를 실행한 일반 사용자가 조회할 수 있는 프로세스의 PID, PPID, 이름, 실행 파일 경로, 비식별 사용자 ID 및 시작 시각
- 운영체제가 일반 사용자에게 제공하는 listening port와 현재 네트워크 연결 정보
- Docker CLI와 daemon에 이미 접근 가능한 환경의 container lifecycle 이벤트
- 개별 필드 접근 실패와 Docker 비가용 상태에 대한 비민감 집계 및 상태 로그

### 수집하지 못하거나 의도적으로 제외한 데이터

- 권한이 없어 읽을 수 없는 프로세스의 실행 파일 경로, 사용자 및 시작 시각은 `null`이거나 해당 이벤트가 안전하게 생략될 수 있다.
- 시스템 전체 네트워크 연결 조회가 제한되면 fallback 결과에 포함되지 않는 연결이 있을 수 있다.
- Docker CLI가 없거나 daemon 접근 권한이 없으면 lifecycle 이벤트를 수집하지 않는다.
- Spark checkpoint는 컨테이너 내부 `/tmp/laptop-metrics/security_events_clickhouse`에 저장하며 호스트 bind mount나 Docker volume으로 지속 보존하지 않는다. Spark 컨테이너를 재생성하면 checkpoint가 사라지므로 장애 복구 보장은 후속 단계에서 지속 저장소와 함께 다룬다.
- ClickHouse `/var/lib/clickhouse`는 Docker named/anonymous volume 대신 컨테이너 tmpfs에 둔다. 컨테이너 중지·재생성 시 ClickHouse 데이터가 삭제되며, 활성화된 내장 진단 로그가 tmpfs의 메모리를 계속 사용할 수 있다.
- process command line과 arguments, 환경변수, 파일 내용, 브라우저 정보, credential, token 및 secret은 의도적으로 수집하지 않는다.
- 커널 감사, Endpoint Security, eBPF 또는 osquery 기반 이벤트는 이번 단계 범위에 포함하지 않았다.

### 의도적으로 부여하지 않은 권한과 이유

| 권한 또는 접근 | 부여하지 않은 이유 | 저권한 대안과 영향 |
| --- | --- | --- |
| sudo/root, host PID namespace, host network | 다른 사용자의 프로세스 및 host namespace 노출 범위를 크게 확대함 | 일반 사용자 조회 결과와 `null` 필드, 권한 거부 집계를 사용함 |
| privileged container, `CAP_SYS_PTRACE`, `CAP_SYS_ADMIN` | 프로세스 메모리 및 광범위한 host 제어 권한을 제공함 | collector를 macOS native 일반 사용자 process로 실행함 |
| Full Disk Access, Endpoint Security entitlement | 파일·사용자 활동의 민감한 가시성과 강한 시스템 권한을 제공함 | 현재 OS API로 얻을 수 있는 process/network 상태만 수집함 |
| Docker socket mount | 사실상 host Docker daemon 제어 권한을 container에 제공함 | native collector가 기존 Docker CLI 접근 권한을 사용할 때만 optional 수집함 |
| 감사 설정 변경, eBPF 또는 추가 센서 | 시스템 전역 관측과 운영 위험을 추가하며 phase 1 요구에 필요하지 않음 | 향후 구체적인 탐지 시나리오가 요구할 때 별도 단계로 검토함 |

저장소 보안 정책 중 데이터 최소화, 권한 제한, secret 처리, dependency 및 artifact 검증, 로깅·오류 처리, 테스트, 데이터 보존 정책을 적용했다. Docker daemon을 사용한 smoke test에서도 Compose에 privileged mode, 추가 capability, host PID/network namespace 또는 민감한 host mount를 추가하지 않았다. Spark job은 image의 일반 사용자 `spark`로 실행됨을 확인했다.
