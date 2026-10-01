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
| `tests/` | 이벤트 모델, collector, privacy, 권한 저하, Kafka, Spark 및 ClickHouse 경계 테스트 |
| `tests/docs/` | 테스트 중 확인한 실패 사례, 원인 및 개선 방법 기록 |

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

ClickHouse CPU 사용량 조사 중 persistent ClickHouse volume에 `system.trace_log` 약 10.16 GiB, `system.text_log` 약 984 MiB가 쌓인 것을 확인했다. 조사 시점에 `system.metric_log`와 `system.asynchronous_metric_log`의 background merge가 진행 중이었고, ClickHouse CPU는 약 180–306%까지 관측되었다. 일반 쿼리 부하는 낮았으므로 누적 system log merge가 높은 사용량의 주된 원인으로 판단했다.

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
