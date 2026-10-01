<!--
작성 지침:
- `.github/PR_DESCRIPTION_EXAMPLE.md`를 구성, 다이어그램 형식, 표현 방식의 참고 자료로 사용한다.
- 예시 내용을 그대로 복사하지 말고 현재 PR의 실제 diff, 설정, 실행 환경을 기준으로 작성한다.
- 코드나 설정에서 확인되지 않은 컴포넌트, 토픽, 파티션, 컨슈머 그룹, 테이블, 볼륨을 임의로 추가하지 않는다.
- 검증하지 않은 내용을 성공한 것처럼 작성하지 않는다. 실행하지 못한 검증은 `미실행 (사유: ...)`으로 표시한다.
- 작성 완료 후 안내용 HTML 주석은 PR 본문에 노출되지 않도록 유지한다.
-->

### Architecture (Diagram)

<!--
- `.github/PR_DESCRIPTION_EXAMPLE.md`와 `.github/PULL_REQUEST_TEMPLATE/archi_diagram.png` 참고
- 영어 ASCII art 다이어그램으로 작성
- 전체 아키텍처와 데이터 흐름을 화살표로 표현
- 로컬 콜렉터와 Docker 네트워크 내부의 Kafka, Spark, ClickHouse 포함
- 실제 변경사항과 관련된 세부 컴포넌트만 표현
  - Kafka topics, partitions, consumer groups
  - Spark driver, workers, streaming jobs, checkpoints
  - ClickHouse databases, tables
  - Docker networks, ports, mounted volumes
- 이번 PR에서 변경되지 않은 구성도 전체 흐름 이해에 필요하면 표시 가능
- 아직 구현되지 않은 구성은 `(planned)`로 명확하게 구분
-->

### Summary

<!--
- `.github/PR_DESCRIPTION_EXAMPLE.md`의 표현 방식 참고
- PR의 변경사항, 변경 이유, 변경에 따른 영향을 한글로 요약
- 실제 diff에서 확인되는 내용만 작성
- `~했습니다`, `~했다`와 같은 서술형 종결 표현 제외
- `~ 추가`, `~ 변경`, `~ 전환`, `~ 제거`, `~ 확인` 등의 명사형 표현 사용
-->

### Verification

<!--
- `.github/PR_DESCRIPTION_EXAMPLE.md`의 표현 방식 참고
- 실제 실행한 테스트 결과와 주요 트러블슈팅을 최대 5개까지 작성
- 테스트 명령, 확인 대상, 결과가 드러나도록 작성
- 실행하지 않은 테스트를 통과한 것으로 작성하지 않음
- 미실행 항목은 `미실행 (사유: ...)` 형식으로 표시
- `~했습니다`, `~했다`와 같은 서술형 종결 표현 제외
- `~ 통과`, `~ 확인`, `~ 실패`, `~ 해결`, `~ 미실행` 등의 명사형 표현 사용
-->