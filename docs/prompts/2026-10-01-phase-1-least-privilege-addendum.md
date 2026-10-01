# Phase 1 least-privilege addendum

하던 일을 재개하되, 아래 내용을 진행 중이던 작업의 맥락에 추가해라. 보안과 권한에 대해서는 다음 원칙을 최우선으로 적용하라.

- 일부 프로세스나 필드를 수집하지 못하더라도 완전한 가시성을 얻기 위해 권한을 확대하지 마라.
- collector는 기본적으로 host 운영체제의 일반 사용자 권한으로 실행한다. macOS에서는 가능하면 collector를 Docker 컨테이너가 아닌 native process로 실행하고, Kafka 이후의 파이프라인만 기존 Docker 환경을 사용한다.
- sudo, root 실행, privileged container, `pid: host`, host network, `CAP_SYS_PTRACE`, `CAP_SYS_ADMIN`, Full Disk Access, Endpoint Security entitlement를 자동으로 추가하거나 요구하지 마라.
- 위 권한 중 하나가 특정 데이터를 수집하는 데 필요하다면 구현 중 자동 적용하지 말고, 수집하지 못하는 데이터와 필요한 권한, 해당 권한의 보안 위험, 가능한 저권한 대안을 최종 보고에 제시하라.
- Docker lifecycle 수집을 위해 Docker socket을 collector 컨테이너에 mount하지 마라. 안전한 방법으로 접근할 수 없다면 Docker collector를 optional/unavailable 상태로 두고 다른 collector가 계속 동작하게 하라.
- 수집 데이터는 allowlist 방식으로 구성하라. 허용 필드는 PID, PPID, 프로세스명, 실행 파일 경로, 사용자 식별자, 시작 시각과 명시된 네트워크 필드로 제한한다.
- 프로세스 command line, arguments, 환경변수, 파일 내용, 브라우저 정보, credential, token, secret은 수집하거나 로그에 기록하지 마라.
- 사용자 홈이 포함된 실행 파일 경로는 가능한 경우 `$HOME`과 같은 비식별 표현으로 정규화하고, host ID와 사용자 식별자는 원본 값을 직접 전송하지 마라.
- 권한 부족으로 일부 프로세스 필드를 읽지 못하는 것은 정상 동작으로 처리한다. 전체 collector를 실패시키지 말고 해당 필드를 null로 두거나 이벤트를 안전하게 건너뛰며, `access_denied_count`처럼 민감하지 않은 집계값만 기록한다.
- 현재 권한에서 수집 가능한 범위를 먼저 구현하고, 미래의 탐지 시나리오가 실제로 요구할 때에만 추가 센서나 권한을 별도 단계로 제안하라.

다음 보안 테스트를 추가하라.

- 직렬화된 이벤트에 command line, 환경변수, secret 관련 필드가 존재하지 않는지 검증
- 일부 프로세스에서 AccessDenied가 발생해도 collector가 계속 실행되는지 검증
- host ID와 사용자 식별자가 원본 값으로 전송되지 않는지 검증
- Docker를 사용할 수 없거나 접근 권한이 없을 때 graceful degradation하는지 검증
- Docker Compose와 실행 설정에 privileged, `CAP_SYS_PTRACE`, `CAP_SYS_ADMIN`, host PID namespace, 불필요한 host mount가 추가되지 않았는지 검증

최종 보고에는 현재 권한으로 확보한 가시성, 수집하지 못한 데이터, 의도적으로 부여하지 않은 권한과 그 이유를 구분해 적어라.
