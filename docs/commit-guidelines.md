# Commit Message Guidelines

## 기본 구조

커밋 제목은 영어로 작성하고, 본문의 `역할` 항목은 한글로 작성한다. GitHub 커밋 목록에서는 제목으로 변경 목적을 빠르게 확인하고, 커밋 상세 화면에서는 한글 본문으로 책임과 결과를 확인할 수 있도록 한다.

```text
<type>(<scope>): <English summary>

역할:
- <이 커밋이 담당하는 역할을 한글로 작성한다.>
- <변경으로 보장하거나 제거하는 동작을 한글로 작성한다.>
```

제목은 Conventional Commits 형식을 사용한다.

| 항목 | 규칙 | 예시 |
| --- | --- | --- |
| `type` | 변경 성격을 나타내는 영어 소문자 | `feat`, `fix`, `docs`, `test`, `chore` |
| `scope` | 주요 변경 영역을 나타내는 영어 소문자 | `collector`, `pipeline`, `runtime` |
| summary | 명령형 영어 문장, 마침표 생략 | `collect privacy-preserving endpoint telemetry` |
| 역할 | 커밋의 책임과 결과를 한글 bullet로 작성 | `민감 필드를 payload allowlist에서 제외한다.` |

본문은 구현 과정을 시간순으로 나열하지 않는다. 해당 커밋이 맡는 책임, 외부에서 관찰할 수 있는 변화, 보장하는 안전 속성을 기록한다. 서로 관련 없는 역할이 섞이면 커밋을 나눈다.

## 템플릿 설정과 사용

저장소의 `.gitmessage`를 로컬 commit template으로 설정한다.

```bash
git config --local commit.template .gitmessage
```

파일을 stage한 다음 인자 없이 `git commit`을 실행하면 편집기에 템플릿이 열린다.

```bash
git commit
```

설정 여부는 다음 명령으로 확인한다.

```bash
git config --local --get commit.template
```

커밋 후 제목과 본문은 다음 명령으로 확인한다.

```bash
git show --stat --format=fuller HEAD
```

## Phase 1 권장 커밋

### 1. 프로젝트 범위와 보안 정책

포함 파일:

```text
.gitmessage
AGENTS.md
docs/commit-guidelines.md
docs/security-policy.md
docs/prompts/
```

커밋 메시지:

```text
docs: define phase 1 security telemetry scope

역할:
- 저장소 지침을 endpoint security telemetry 프로젝트 기준으로 갱신한다.
- 최소 권한과 데이터 최소화 요구사항을 명문화한다.
- 1단계 원본 요구사항과 보안 추가 지침을 docs/prompts에 보존한다.
- AGENTS.md가 참조하는 보안 정책을 같은 변경 단위로 관리한다.
- 영어 제목과 한글 역할 본문을 사용하는 커밋 템플릿과 작성 규칙을 제공한다.
```

### 2. 공통 이벤트 모델과 collector

포함 파일:

```text
.env.example
pyproject.toml
collector/
schemas/
tests/test_collector.py
tests/test_collector_service.py
tests/test_config.py
tests/test_host_metric_schema_consistency.py
tests/test_kafka_integration.py
tests/test_process_metric_schema_consistency.py
tests/test_serializers.py
tests/test_spark_metric_parsing.py
tests/test_collectors.py
tests/test_events.py
```

커밋 메시지:

```text
feat(collector): collect privacy-preserving endpoint telemetry

역할:
- CPU와 메모리 중심 metric 모델을 버전이 있는 SecurityEvent 계약으로 교체한다.
- 프로세스 상태, 네트워크 연결, optional Docker lifecycle 이벤트를 수집한다.
- host와 사용자 식별자를 HMAC으로 비식별화하고 사용자 홈 경로를 정규화한다.
- 직렬화 payload를 명시적인 allowlist로 제한하고 민감 필드를 거부한다.
- 모든 1단계 이벤트를 단일 Kafka topic으로 발행하고 결정적 event ID를 message key로 사용한다.
- AccessDenied와 Docker 비가용 상태를 전체 수집 실패가 아닌 graceful degradation으로 처리한다.
- 이전 metric 모델에 종속된 schema와 테스트를 제거한다.
```

### 3. Spark와 ClickHouse 전달 경로

포함 파일:

```text
docker-compose.yml
spark/
clickhouse/
scripts/apply_clickhouse_schema.sh
tests/test_clickhouse_sink.py
tests/test_contract_consistency.py
tests/test_spark_event_parsing.py
```

커밋 메시지:

```text
feat(pipeline): deliver security events to ClickHouse

역할:
- 기존 metric console job을 Spark security event streaming job으로 교체한다.
- Kafka 보안 이벤트를 파싱하고 저장 전에 최소한의 trust-boundary 검증을 수행한다.
- 검증된 이벤트를 JSONEachRow HTTP 인터페이스로 ClickHouse에 적재한다.
- metric 테이블을 공통 security_events 테이블과 7일 보존 정책으로 교체한다.
- 외부 공개 port를 loopback에 한정하고 Spark checkpoint를 named volume에 보존한다.
- collector, JSON Schema, Spark sink의 payload allowlist가 일치하는지 검증한다.
```

### 4. Native 실행 설정과 권한 검증

포함 파일:

```text
launchd/
scripts/install_collector_service.sh
scripts/run_collector.sh
scripts/uninstall_collector_service.sh
tests/test_security_controls.py
```

커밋 메시지:

```text
chore(runtime): update native collector service controls

역할:
- macOS collector를 일반 사용자 권한의 native process로 실행한다.
- launchd service를 security telemetry collector 기준으로 변경하고 기존 service를 안전하게 정리한다.
- privileged container, 위험 capability, host namespace, Docker socket mount 사용을 방지한다.
- loopback port 공개, 제한된 보존 기간, collector 환경변수 격리를 검증한다.
- 권한이 필요한 telemetry는 자동으로 권한을 확대하지 않고 가시성 제약으로 유지한다.
```

### 5. 사용자 문서와 검증 기록

포함 파일:

```text
README.md
docs/phase-1-implementation-report.md
tests/docs/
```

커밋 메시지:

```text
docs: document phase 1 usage and verification

역할:
- README를 1단계 범위, 이벤트 흐름, 지원 이벤트, 최소 로컬 실행 명령으로 다시 작성한다.
- 구현 결과, end-to-end 확인 상태, 남은 환경 제약을 별도 보고서로 기록한다.
- 테스트 중 발생한 실패와 원인, 적용한 개선 방법을 보존한다.
- 확인된 테스트 결과와 Docker daemon 또는 host telemetry 접근이 필요한 미확인 항목을 구분한다.
```
