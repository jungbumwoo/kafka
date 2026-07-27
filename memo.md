 kafka consumer client에서 poll 요청 시 해당 request data 요청 format이 궁금. 그리고 리더 파티션 브로커에 따라 각각 
브로커로 부터 요청을 보낼텐데 어떤 식으로 받아온 데이터를 aggregation해서 사용자 스레드에 poll 요청을 반환하는지도 궁금

1. ClassicKafkaConsumer.java:689 - pollForFetches()
  사용자 스레드 진입점. poll의 3단계 흐름(버퍼 fast path → fetch 전송 → 응답 대기/집계)을 설명.

  2. AbstractFetch.java:421 - prepareFetchRequests() (질문 2에 해당)
  할당된 partition들을 리더 브로커(Node) 기준으로 그룹핑하여 "노드마다 하나의 FetchRequest"를 만드는 핵심 로직. computeIfAbsent로 같은 노드의 partition들을 묶고, in-flight/버퍼 링된 노드는 건너뛰는 이유를 설명.

  3. FetchRequest.java:247 - Builder.build() (질문 1에 해당)
  브로커로 전송하는 fetch 요청 포맷:
  - 요청 전체: maxWaitMs, minBytes, maxBytes, isolationLevel, session 정보
  - topic → partition 계층 구조
  - partition별: fetchOffset, partitionMaxBytes, currentLeaderEpoch, logStartOffset

  4. Fetcher.java:183 - sendFetchesInternal()
  노드별로 만든 요청을 각 브로커에 비동기 전송하고, 응답 리스너가 handleFetchSuccess를 호출하도록 등록.

  5. AbstractFetch.java:151 - handleFetchSuccess() (질문 2 aggregation)
  한 브로커 응답을 partition 단위 CompletedFetch로 분해해 공용 FetchBuffer에 축적. 여러 브로커 응답이 모두 이 버퍼로 모이는 것이 집계의 핵심 지점.

  6. FetchCollector.java:91 - collectFetch() (질문 2 aggregation)
  버퍼에서 CompletedFetch를 하나씩 꺼내 역직렬화하고 max.poll.records까지 하나의 Fetch로 누적하여 사용자에게 반환.

  전체 흐름 한 줄 요약

  poll() → pollForFetches()
    → prepareFetchRequests() [partition을 리더 노드별로 그룹핑]
    → sendFetchesInternal() [노드마다 FetchRequest 비동기 전송]
    → client.poll() [응답 대기]
    → handleFetchSuccess() [응답을 partition별 CompletedFetch로 FetchBuffer에 축적]
    → collectFetch() [버퍼의 여러 partition 결과를 하나의 Fetch로 집계 → ConsumerRecords 반환]

  핵심 포인트: 각 브로커로는 독립적인 요청/응답이 오가지만, 모든 응답이 하나의 공용 FetchBuffer로 수렴되고, 사용자 스레드가 collectFetch()에서 이를 꺼내 max.poll.records 한도 내에서 합쳐 반환하는 구조입니다.

