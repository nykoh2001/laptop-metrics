# Phase 1 security telemetry implementation prompt

현재 저장소의 기존 Local → Kafka → Spark → ClickHouse 메트릭 파이프라인을 실시간 엔드포인트 보안 텔레메트리 파이프라인으로 전환하려고 한다.

먼저 저장소 전체 구조와 AGENTS.md 등의 저장소 지침을 확인하고, 현재 collector, Kafka producer/topic, Spark 처리, ClickHouse 적재, Docker Compose, 테스트 및 실행 방법을 파악하라. 기존 코드와 동작을 충분히 확인한 후 작업하라. 단, 현재 AGENTS.md는 기존 프로젝트에 대한 내용도 포함되어 있으므로, 새로운 프로젝트와 호환되는 'Coding Guidelines', 'Project Structure' 과 같은 섹션들을 참고하고, 'Project Overview'와 같이 기존 프로젝트에만 연관된 내용은 업데이트 해라. AGENTS.md는 영어로 작성해라.

전체 프로젝트 로드맵은 다음과 같다.

1. 프로세스·네트워크·Docker 등 로컬 텔레메트리 수집과 공통 보안 이벤트 스키마 정의
2. Kafka raw/normalized/alert/DLQ topic과 replay 구조
3. Spark 기반 검증·정규화·중복 제거·watermark·시간창·이벤트 상관분석
4. 규칙 기반 위협행위 탐지와 경보·시각화
5. 안전한 위협행위 시뮬레이션, 정답 라벨, 탐지율·오탐률·지연·장애 복구·재처리 검증
6. 통계적 행동 baseline과 Isolation Forest 이상 탐지

이번 작업에서는 1단계만 구현하라. 이후 단계의 기능을 미리 구현하지 마라.

1단계 요구사항은 다음과 같다.

- 기존의 일반적인 CPU·메모리 중심 시스템 메트릭 수집을 보안 텔레메트리 수집으로 전환한다.
- 기존 파이프라인의 재사용 가능한 Kafka, Spark, ClickHouse 및 실행 인프라는 최대한 유지한다.
- 프로세스 상태를 수집한다. 최소 필드는 PID, PPID, 프로세스명, 실행 파일 경로, 사용자, 시작 시각이다. 운영체제에서 얻을 수 없는 필드는 null을 허용한다.
- listening port와 현재 네트워크 연결 정보를 수집한다. 최소 필드는 local address/port, remote address/port, connection status, PID이다.
- Docker daemon을 사용할 수 있는 환경이라면 Docker container lifecycle 이벤트를 수집한다. Docker를 사용할 수 없어도 전체 collector가 실패하지 않도록 optional collector로 구현한다.
- 현재 저장소에 이미 적절한 osquery 연동이 있다면 재사용한다. 없다면 osquery 설치, sudo 실행, 감사 설정 변경, Endpoint Security 권한 요청을 자동으로 수행하지 마라.
- 명령행 인자, 환경변수, 파일 내용, 브라우저 정보처럼 secret이나 개인정보가 포함될 수 있는 값은 기본적으로 수집하지 마라.
- 호스트 ID는 원본 장치명이나 개인 식별 정보를 그대로 사용하지 말고 안정적인 비식별 ID로 표현한다.
- 모든 수집 결과는 하나의 버전이 있는 공통 이벤트 모델로 변환한다.
- 공통 이벤트에는 최소한 schema_version, event_id, event_time, collected_at, host_id, source, event_type, action, payload 필드를 둔다.
- event_id는 동일 이벤트의 중복 여부를 판정할 수 있도록 결정적이거나 명확한 생성 규칙을 갖게 한다.
- event_time과 수집 시각을 구분한다.
- 현재 Kafka 경로를 통해 보안 텔레메트리 이벤트를 발행한다. 2단계에서 topic 구조를 확장할 예정이므로 이번 단계에서는 필요한 최소 topic만 사용한다.
- Spark와 ClickHouse에는 새 이벤트가 end-to-end로 도달하는 데 필요한 최소한의 호환 수정만 적용한다. 정규화, window, 탐지, 경보, DLQ, replay는 구현하지 마라.
- 기존 메트릭 코드가 더 이상 필요하지 않다면 참조 관계를 확인한 후 안전하게 제거하거나 명확히 deprecated 처리한다. 관련 없는 사용자 변경은 수정하지 마라.
- 이벤트 모델 직렬화 테스트, 각 collector의 단위 테스트, Docker 비활성 환경의 graceful degradation 테스트를 추가한다.
- 가능한 범위에서 실제 파이프라인 smoke test를 실행하여 프로세스 또는 네트워크 이벤트가 Kafka를 거쳐 ClickHouse까지 도달하는지 확인한다.
- 실행한 테스트와 확인하지 못한 부분을 최종 보고에 구분하여 적는다.

README.md는 기존 내용을 보존하지 말고 완전히 초기화하여 처음부터 다시 작성하라. 단, 문서는 최소한으로 유지한다.

README에는 다음 내용만 담는다.

- 프로젝트를 설명하는 한 문단
- Local collectors → Kafka → Spark → ClickHouse 전체 흐름 한 줄
- 위 1~6단계 로드맵을 단계별 한 줄로 요약
- 현재 구현 범위가 1단계라는 설명
- 1단계를 로컬에서 실행하고 확인하는 최소 명령
- 현재 수집하는 이벤트 종류와 공통 이벤트 필드
- 알려진 환경 제약

탐지 규칙, 시뮬레이션 설계, 머신러닝 방식, 장기 아키텍처 등 아직 구현하지 않은 상세 내용은 README에 추가하지 마라. 내가 직접 요청한 내용만 문서화하라.

작업이 끝나면 다음 순서로 보고하라.

1. 기존 구조에서 파악한 내용
2. 변경한 파일과 역할
3. 새로운 이벤트 흐름
4. 테스트 및 end-to-end 확인 결과
5. 남아 있는 환경 제약
6. 2단계로 넘긴 항목

현재 저장소 구조에 맞게 구현하되, 범위를 확장하지 말고 1단계의 작동 가능한 최소 기반을 완성하라.

1단계와 연결되지 않는 기존 시스템 구현 로직들은 모두 삭제해도 좋다. 있는 것들을 재사용 하는 것보다 어느정도 초기화를 하고 구현하는 것이 커밋 이력에서 확인할 때 파악이 쉽다. 또한, 1단계 구현이 끝나면 테스트 코드를 작성해서 각 핵심 함수별, 기능별, 전체 흐름이 정상 동작하는지 확인해라. 만약 테스트 도중에 실패 사례가 생기면, 해당 사례를 기록하고 실패 이유와 기존 방법에서 실패가 난 원인, 개선한 방법을 tests/docs 디렉토리에 추가해라.

또한, docs/prompts 디렉토리를 생성하고 입력받은 명령을 파일로 저장해라.
