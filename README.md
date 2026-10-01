# Endpoint Security Telemetry Pipeline

macOS 호스트의 프로세스 상태, listening port와 현재 네트워크 연결, 선택적 Docker 컨테이너 lifecycle 이벤트를 개인정보 노출을 줄인 공통 보안 이벤트로 수집해 로컬 스트리밍 파이프라인으로 전달하는 학습 프로젝트입니다.

`Local collectors → Kafka (security_events) → Spark Structured Streaming → ClickHouse (security_events)`

## 로드맵

1. 로컬 프로세스·네트워크·Docker 텔레메트리와 공통 이벤트 스키마를 구현합니다.
2. Kafka raw/normalized/alert/DLQ topic과 replay 구조를 추가합니다.
3. Spark 검증·정규화·중복 제거·watermark·시간창·상관분석을 추가합니다.
4. 규칙 기반 위협 탐지·경보·시각화를 추가합니다.
5. 안전한 위협 시뮬레이션과 정답 라벨, 탐지 품질·지연·복구·재처리를 검증합니다.
6. 통계적 행동 baseline과 Isolation Forest 이상 탐지를 추가합니다.

현재 구현 범위는 **1단계**이며 이후 단계 기능은 포함하지 않습니다.

## 로컬 실행 및 확인

```bash
cp .env.example .env
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
docker compose up -d --wait kafka clickhouse spark
sh scripts/apply_clickhouse_schema.sh
```

Spark job과 collector를 각각 실행합니다.

```bash
docker compose exec spark /opt/spark/bin/spark-submit --master 'local[*]' --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 /opt/laptop-metrics/spark/jobs/security_events_to_clickhouse.py
.venv/bin/python -m collector
```

적재 결과를 확인합니다.

```bash
docker compose exec clickhouse sh -c 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database "$CLICKHOUSE_DB" --query "SELECT event_type, count() FROM security_events GROUP BY event_type"'
```

## 현재 이벤트와 공통 필드

| 이벤트 | 내용 |
|---|---|
| `process.state` | PID, PPID, 프로세스명, 실행 파일 경로, 비식별 사용자 ID, 시작 시각 |
| `network.connection` | local/remote 주소·port, 연결 상태, PID; listening socket 포함 |
| `docker.container.lifecycle` | Docker daemon 사용 시 container lifecycle action과 container ID |

모든 이벤트는 `schema_version`, `event_id`, `event_time`, `collected_at`, `host_id`, `source`, `event_type`, `action`, `payload` 필드를 가집니다.

## 알려진 환경 제약

- collector는 macOS 일반 사용자 권한의 native process로 실행하며, 권한에 따라 일부 프로세스 필드가 `null`이거나 연결이 보이지 않을 수 있습니다.
- host ID와 사용자 ID는 비밀 salt를 사용한 HMAC-SHA-256 기반 pseudonym이고, 확인 가능한 사용자 홈 경로는 `$HOME`으로 치환합니다.
- `.env`의 `HOST_ID_SALT`는 외부에 공유하지 않을 안정적인 로컬 값으로 변경해야 합니다.
- Docker CLI 또는 daemon이 없으면 Docker 이벤트만 비활성화되고 다른 collector는 계속 동작합니다.
- Docker socket mount, sudo/root, privileged container, host PID/network, 추가 capability, Full Disk Access, Endpoint Security entitlement를 사용하거나 요구하지 않습니다.
- Docker 서비스 포트는 loopback에만 공개되며 Kafka·ClickHouse 이벤트는 7일 후 삭제됩니다.
- 최초 Spark 실행은 Kafka connector package 다운로드를 위한 인터넷 연결이 필요합니다.
- osquery 설치, sudo 실행, 감사 설정 변경, Endpoint Security 권한 요청은 수행하지 않습니다.
