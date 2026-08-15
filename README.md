# laptop-metrics

## 개요

macOS의 호스트·프로세스 메트릭을 실시간으로 수집하고 처리하는 프로젝트입니다. Kafka → Spark Structured Streaming → ClickHouse → Grafana로 이어지는 스트리밍 데이터의 수집·전달·변환·저장·시각화 과정을 직접 다루고, 반복 가능한 실행 환경과 CI를 통해 운영 작업을 자동화 및 장애 상황을 실험하고 대응 가능한 시스템을 구축하는 것이 목표입니다.

## 주요 흐름

```text
macOS
└─ launchd → collector
             ├─ host_metrics
             └─ process_metrics
                    │
                    ▼
              Kafka (KRaft)
                    │
                    ▼
        Spark Structured Streaming
        JSON 파싱 → 타입 변환 → UTC 정규화
                    │
           현재: console sink
           목표: ClickHouse → Grafana
```

collector는 CPU, memory, swap, disk, network와 프로세스별 CPU, memory, disk I/O를 주기적으로 수집합니다.

## 구성

| 구성 요소 | 실행 위치 | 역할 |
|---|---|---|
| collector | macOS `launchd` | 실제 Mac과 실행 중인 프로세스의 메트릭 수집 |
| Kafka 4.0.2 | Docker Compose | `host_metrics`, `process_metrics` 이벤트 전달 |
| Spark 3.5.8 | Docker Compose | JSON 파싱, 타입 변환, 스트리밍 처리 |
| ClickHouse 25.3 | Docker Compose | 시계열 메트릭 저장소 |
| Grafana 11.6.0 | Docker Compose | 메트릭 시각화 |
| Kafka UI 0.7.2 | Docker Compose | 토픽과 메시지 관찰 |

## 실행법

Docker Desktop과 Python 3.11 이상이 필요합니다.

### 1. 환경 설정과 인프라 실행

```bash
cp .env.example .env
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
docker compose up -d --wait
```

`.env`의 비밀번호는 로컬 환경에 맞게 변경하고 Git에 커밋하지 않습니다.

| 서비스 | 주소 |
|---|---|
| Kafka | `localhost:9092` |
| ClickHouse HTTP | `localhost:8123` |
| Grafana | <http://localhost:3000> |
| Kafka UI | <http://localhost:8080> |

### 2. collector 실행

로그인 시 자동 시작하고 비정상 종료 시 재시작되도록 `launchd`에 설치합니다.

```bash
./scripts/install_collector_service.sh
```

상태 확인, 재시작, 제거 명령은 다음과 같습니다.

```bash
launchctl print "gui/$(id -u)/com.local.system-metric-collector"
launchctl kickstart -k "gui/$(id -u)/com.local.system-metric-collector"
./scripts/uninstall_collector_service.sh
```

로그는 `~/Library/Logs/laptop-metrics/`에 저장됩니다. 일회성으로 직접 실행하려면 `launchd`에 설치하지 않고 다음 명령을 사용합니다.

```bash
.venv/bin/python -m collector
```

### 3. Spark streaming job 실행

Host metric:

```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  /opt/laptop-metrics/spark/jobs/host_metrics_console.py
```

Process metric:

```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  /opt/laptop-metrics/spark/jobs/process_metrics_console.py
```

최초 실행 시 Kafka connector를 내려받기 위해 인터넷 연결이 필요합니다. job은 `Ctrl+C`로 종료합니다.

인프라를 중지하거나 제거하려면 다음 명령을 사용합니다. `down`은 named volume을 보존합니다.

```bash
docker compose stop
docker compose down
```

## 주요 설계 근거

| 설계 | 근거 |
|---|---|
| collector를 Docker가 아닌 macOS에서 실행 | Docker Desktop 컨테이너는 Linux VM 내부만 관찰하므로 실제 macOS 프로세스와 호스트 메트릭을 정확히 수집할 수 없습니다. |
| collector를 `launchd`로 관리 | macOS 로그인 시 자동 시작, 비정상 종료 시 재시작, 로그 파일 관리를 운영체제 기본 기능으로 처리할 수 있습니다. |
| `launchd`에서 저장소의 `.venv`를 명시적으로 사용 | `launchd`는 shell profile이나 pyenv/Poetry activation을 읽지 않으므로 실행 환경을 고정해야 합니다. |
| Kafka listener를 내부/외부로 분리 | 컨테이너는 `kafka:19092`, macOS collector는 `localhost:9092`로 접속해야 각 네트워크에서 broker의 advertised address를 해석할 수 있습니다. |
| Kafka를 단일 KRaft broker로 실행 | 로컬 학습 환경에서는 ZooKeeper와 다중 broker 운영 복잡도 없이 스트리밍 흐름에 집중할 수 있습니다. |
| Spark를 cluster 대신 `local[*]`로 실행 | 현재 처리량에는 master/worker 구성이 불필요하며, 필요해질 때 확장할 수 있도록 job과 실행 환경만 분리합니다. |
| 메트릭별 명시적 Spark schema와 checkpoint 사용 | JSON 타입을 예측 가능하게 유지하고, 각 query가 자신의 offset과 진행 상태를 독립적으로 복구할 수 있습니다. |
| 타임스탬프를 UTC로 정규화 | 저장·분석 계층에서 로컬 시간대와 DST에 따른 해석 차이를 줄입니다. |
| Kafka·ClickHouse·Grafana 데이터에 named volume 사용 | 컨테이너를 다시 만들어도 로컬 학습 데이터를 유지합니다. |

## CI

반복적인 품질 검사와 테스트, 패키지 빌드를 자동화해 변경 사항을 일관된 환경에서 확인합니다. GitHub Actions는 `main` 브랜치 대상 pull request와 `main` 브랜치 push에서 실행됩니다.

| Workflow | 검사 내용 |
|---|---|
| `ci-quality.yml` | Python 문법, Ruff formatting, Ruff lint |
| `ci-test.yml` | Java 17 기반 Spark 테스트, Kafka producer/consumer 통합 테스트, 전체 pytest suite |
| `ci-build.yml` | source distribution·wheel 빌드, 깨끗한 가상환경 설치, 의존성·import·CLI entry point 확인 |
