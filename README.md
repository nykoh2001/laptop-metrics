# laptop-metrics

macOS 호스트의 시스템 메트릭을 Kafka → Spark Structured Streaming → ClickHouse → Grafana로 처리하기 위한 로컬 학습용 인프라입니다. 컨테이너는 Docker Desktop에서 실행하지만, collector는 실제 macOS 호스트와 프로세스를 관찰하도록 호스트의 `launchd`에서 실행합니다.

## 구성

| 서비스 | 이미지 | 호스트 주소 | 용도 |
|---|---|---|---|
| Kafka | `apache/kafka:4.0.2` | `localhost:9092` | KRaft 단일 브로커 |
| Spark | `apache/spark:3.5.8-scala2.12-java17-python3-ubuntu` | - | `local[*]` 실행 환경 |
| ClickHouse | `clickhouse/clickhouse-server:25.3` | `localhost:8123`, `localhost:9000` | 시계열 저장소 |
| Grafana | `grafana/grafana:11.6.0` | <http://localhost:3000> | 시각화 |
| Kafka UI | `provectuslabs/kafka-ui:v0.7.2` | <http://localhost:8080> | Kafka 관찰 |

선택한 Kafka, Spark, ClickHouse, Grafana, Kafka UI 이미지 manifest는 모두 `linux/arm64`를 제공하므로 Apple Silicon에서 에뮬레이션 없이 실행됩니다.

## 빠른 시작

Docker Desktop을 시작하고 다음을 실행합니다.

```bash
cp .env.example .env
docker compose config --quiet
docker compose up -d
docker compose ps
```

기본 비밀번호는 로컬 개발용 예시입니다. `.env`를 Git에 커밋하지 말고 필요에 맞게 변경하십시오. 종료 및 재시작은 다음과 같습니다.

```bash
docker compose stop
docker compose start
docker compose down
```

`down`은 named volume을 보존합니다. 모든 Kafka/ClickHouse/Grafana 데이터를 제거하는 완전 초기화는 명시적으로 다음을 실행합니다.

```bash
docker compose down --volumes
```

## Kafka listener 설계

Kafka는 client에게 최초 접속 주소가 아니라 `advertised.listeners`의 주소를 돌려줍니다. 따라서 client 위치별로 다른 주소를 광고해야 합니다.

| client 위치 | bootstrap server | advertised address |
|---|---|---|
| Compose network 내부 | `kafka:19092` | `INTERNAL://kafka:19092` |
| macOS collector/도구 | `localhost:9092` | `EXTERNAL://localhost:9092` |
| KRaft controller | client 사용 금지 | `CONTROLLER://kafka:29093` |

컨테이너 안의 `localhost`는 해당 컨테이너 자신이므로 Spark와 Kafka UI는 반드시 `kafka:19092`를 사용합니다. 반대로 macOS의 collector는 Docker DNS 이름 `kafka`를 해석할 수 없으므로 `localhost:9092`를 사용합니다.

## Spark 사용 방식

초기 단계에는 master/worker cluster를 만들지 않습니다. `spark` 컨테이너는 제출 환경으로 대기하며 향후 job은 `local[*]`로 실행합니다. Kafka connector는 Spark와 같은 3.5.8 버전을 사용합니다.

### Host metric console job

```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  /opt/laptop-metrics/spark/jobs/host_metrics_console.py
```

Spark 이미지의 실행 사용자 home은 쓸 수 없는 경로이므로 Ivy package cache를 `/tmp/.ivy2`로
지정합니다. 최초 package 해석에는 인터넷 연결이 필요합니다.

`host_metrics_console.py`는 `host_metrics` topic의 JSON을 명시적인 Spark schema로 파싱하고,
`timestamp`를 UTC `TimestampType`으로 변환한 뒤 console에 출력하는 최소 Structured Streaming
job입니다. 기본적으로 job 시작 이후 도착한 메시지부터 읽습니다. 실행 후 collector가 metric을
발행하면 명령을 실행한 터미널에서 구조화된 행을 확인할 수 있습니다.

### Process metric console job

```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  /opt/laptop-metrics/spark/jobs/process_metrics_console.py
```

`process_metrics_console.py`는 `process_metrics` topic의 JSON을 명시적인 Spark schema로 파싱하고,
Kafka의 binary `value`를 `timestamp`, `pid`, `process_name`, CPU, memory 등의 typed column으로
변환합니다. `timestamp`는 UTC `TimestampType`으로 변환하며 결과는 명령을 실행한 터미널의
console에 micro-batch 단위로 출력됩니다. 두 console job 모두 종료할 때는 `Ctrl+C`를 누릅니다.

| 환경 변수 | 기본값 | 설명 |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:19092` | Spark 컨테이너에서 접근할 Kafka broker |
| `HOST_METRICS_TOPIC` | `host_metrics` | 구독할 Kafka topic |
| `HOST_METRICS_CHECKPOINT_LOCATION` | `/opt/laptop-metrics/spark/checkpoints/host_metrics_console` | offset과 query 진행 상태 저장 위치 |
| `PROCESS_METRICS_TOPIC` | `process_metrics` | 구독할 Kafka topic |
| `PROCESS_METRICS_CHECKPOINT_LOCATION` | `/opt/laptop-metrics/spark/checkpoints/process_metrics_console` | offset과 query 진행 상태 저장 위치 |
| `SPARK_LOG_LEVEL` | `WARN` | Spark 내부 로그 수준 |

각 job은 서로 다른 checkpoint 경로를 사용합니다. Checkpoint가 존재하면 Spark는 저장된
offset부터 이어서 읽으며 `startingOffsets=latest` 설정은 새 query를 처음 시작할 때만
적용됩니다. Console sink는 개발 검증용이며 출력 결과를 영구 저장하지 않습니다.

## collector의 launchd 설치

Docker 컨테이너는 Docker Desktop Linux VM의 메트릭과 프로세스만 관찰합니다. privileged container, host network, host path mount도 macOS의 실제 프로세스 트리를 Linux VM에 노출하지 않으므로 collector를 컨테이너화하지 않습니다.

collector는 `python -m collector`로 실행됩니다. 먼저 저장소의 명시적인 가상환경을 만듭니다. `launchd`는 shell profile, pyenv 또는 Poetry activation을 읽지 않습니다.

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp .env.example .env
./scripts/install_collector_service.sh
```

설치 스크립트는 실제 저장소 절대 경로와 `.venv/bin/python` 경로를 plist에 기록하고 `~/Library/LaunchAgents/com.local.system-metric-collector.plist`를 등록합니다. 로그인 시 자동 시작하며 비정상 종료 시 재시작합니다. 로그는 `~/Library/Logs/laptop-metrics/collector.stdout.log`와 `collector.stderr.log`에 기록됩니다.

```bash
launchctl print "gui/$(id -u)/com.local.system-metric-collector"
launchctl kickstart -k "gui/$(id -u)/com.local.system-metric-collector"
./scripts/uninstall_collector_service.sh
```

wrapper는 `.env`를 읽고 명시적인 Python으로 collector를 실행합니다. `launchd`가 보내는 `SIGTERM`은 `exec`를 통해 Python 프로세스에 직접 전달되며, 진행 중인 수집 cycle을 마친 뒤 Kafka producer를 flush하고 닫습니다.

## System Metric Collector

Collector는 실제 macOS host에서 host metric 한 건과 실행 중인 process별 metric 한 건을 매 cycle 수집하여 각각 `host_metrics`, `process_metrics` topic에 JSON으로 발행합니다.

| 환경 변수 | 기본값 | 설명 |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | 쉼표로 구분한 Kafka 주소 |
| `HOST_METRICS_TOPIC` | `host_metrics` | host metric topic |
| `PROCESS_METRICS_TOPIC` | `process_metrics` | process metric topic |
| `COLLECTION_INTERVAL_SECONDS` | `5` | 양수 polling 주기 |
| `KAFKA_CLIENT_ID` | `system-metric-collector` | Kafka client ID |
| `LOG_LEVEL` | `INFO` | Python logging level |

직접 실행할 때는 현재 작업 디렉터리의 `.env`를 자동으로 읽습니다. 이미 설정된 환경 변수는 `.env`보다 우선합니다.

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp .env.example .env
.venv/bin/python -m collector
```

개발 검증 도구까지 설치하려면 다음을 사용합니다.

```bash
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/ruff format --check .
.venv/bin/ruff check .
.venv/bin/mypy
.venv/bin/pytest
```

Kafka topic 자동 생성이 활성화된 현재 로컬 broker에서는 첫 발행 시 topic이 생성됩니다. 명시적으로 먼저 만들려면 다음 명령을 실행합니다.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --create --if-not-exists \
  --topic host_metrics --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --create --if-not-exists \
  --topic process_metrics --partitions 1 --replication-factor 1
```

발행된 JSON 확인:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 --topic host_metrics \
  --from-beginning --max-messages 1
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 --topic process_metrics \
  --from-beginning --max-messages 1
```

연속 실행과 graceful shutdown은 짧은 interval로 실행한 뒤 `Ctrl+C`를 눌러 확인할 수 있습니다. 종료 로그에 `Flushing pending Kafka messages`와 `stopped gracefully`가 순서대로 출력됩니다.

```bash
COLLECTION_INTERVAL_SECONDS=1 LOG_LEVEL=DEBUG .venv/bin/python -m collector
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:19092 --topic host_metrics,process_metrics
```

## CI

GitHub Actions는 `main` 브랜치를 대상으로 하는 pull request와 `main` 브랜치 push에서
실행됩니다. `ci-quality.yml`은 Python 문법, Ruff format, Ruff lint를 순서대로 검사하고,
`ci-test.yml`은 전체 pytest suite를 실행합니다. `ci-build.yml`은 collector의 source distribution과
wheel을 만들고 깨끗한 가상환경에 wheel을 설치하여 의존성, import, CLI entry point를 검증합니다.
PySpark 테스트는 Java 17의 로컬 Spark session에서 collector JSON의 typed column 변환을 확인합니다.
Kafka 통합 테스트는 Kafka 4.0.2 컨테이너를 시작하고 실제 producer/consumer round-trip을
확인합니다. Build, test, Ruff 의존성은 각각 `requirements-build.txt`, `requirements-test.txt`,
`requirements-quality.txt`로 분리하며 type check는 아직 CI에 포함하지 않습니다.

Kafka 통합 테스트는 외부 broker가 필요한 테스트임을 명시하기 위해 기본 로컬 실행에서는
skip됩니다. 로컬 Compose Kafka를 포함한 전체 CI suite는 다음과 같이 재현할 수 있습니다.

```bash
docker compose up -d --wait kafka
RUN_KAFKA_INTEGRATION_TESTS=1 .venv/bin/pytest --durations=10
```

검증 실패 시 실제 병합을 차단하려면 GitHub repository ruleset 또는 branch protection에서
`main` 브랜치의 `Build Python package`, `Validate syntax and Ruff`, `Run tests` status check를 모두
필수로 지정해야 합니다.

## 검증 명령

```bash
docker compose config --quiet
docker compose up -d --wait

# Kafka topic 생성 및 내부 listener 확인
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --create --if-not-exists \
  --topic infrastructure-check --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --describe --topic infrastructure-check

# macOS host의 external listener에서 produce/consume (kcat이 설치된 경우)
printf 'host-listener-check\n' | kcat -P -b localhost:9092 \
  -t infrastructure-check
kcat -C -b localhost:9092 -t infrastructure-check -o beginning -c 1

# ClickHouse와 Grafana
curl --fail --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
  'http://localhost:8123/?query=SELECT%201'
curl --fail http://localhost:3000/api/health
curl --fail http://localhost:8080/actuator/health
```

ClickHouse metric table definitions and live schema inspection commands are documented in
[`clickhouse/README.md`](clickhouse/README.md).

이 구현 검증에서는 별도 client를 설치하지 않고 `docker cp`로 Kafka 배포본을 `/tmp`에 복사한 뒤, macOS에 설치된 Java 17로 다음 host-native 명령을 실행했습니다.

```bash
docker cp laptop-metrics-kafka-1:/opt/kafka /tmp/laptop-metrics-kafka-client/kafka
printf 'host-listener-check\n' | \
  /tmp/laptop-metrics-kafka-client/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic infrastructure-check
/tmp/laptop-metrics-kafka-client/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic infrastructure-check \
  --from-beginning --max-messages 1 --timeout-ms 15000
```

volume 보존은 임시 topic/table 또는 Grafana 설정을 만든 뒤 `docker compose down && docker compose up -d` 후 남아 있는지 확인합니다. `down --volumes`는 이 검증에서 사용하지 않습니다.

plist 문법은 template과 설치 결과 각각 확인할 수 있습니다.

```bash
plutil -lint launchd/com.local.system-metric-collector.plist.example
plutil -lint "$HOME/Library/LaunchAgents/com.local.system-metric-collector.plist"
```

## 문제 해결과 자원

- `Cannot connect to the Docker daemon`: Docker Desktop을 먼저 시작합니다.
- Kafka host client가 연결 후 끊김: client가 `localhost:9092`, 컨테이너가 `kafka:19092`를 사용하는지 확인합니다.
- port 충돌: `.env`의 host port를 변경합니다. Kafka external advertised port도 `KAFKA_EXTERNAL_PORT`와 동일해야 합니다.
- 초기 기동은 이미지 pull과 Spark package download 때문에 느릴 수 있습니다.
- Docker Desktop 메모리는 최소 6GB, 여유가 있으면 8GB를 권장합니다. Spark driver memory 기본값은 1GB로 제한했습니다.
