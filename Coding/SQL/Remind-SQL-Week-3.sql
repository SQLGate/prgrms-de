-- Assignment - 1
-- 오늘 살펴본 SQL들을 다시 한번 실행해보고 요약해서 제출
-- 포맷은 중요하지 않음 (파이썬 노트북, 텍스트 파일 등등)
-- 하지만 Colab SQL Notebook 사용해보는 것을 추천 ^^

/*********************************************************
-- 251번 사용자의 사용자별 처음 채널과 마지막 채널만 찾기
*********************************************************/
select
  *
from
  (
    SELECT
      ts,
      channel,
      ROW_NUMBER() OVER (
        PARTITION BY usc.userid
        ORDER BY
          ts
      ) as n,
      ROW_NUMBER() OVER (
        PARTITION BY usc.userid
        ORDER BY
          ts desc
      ) as n2
    FROM
      raw_data.user_session_channel usc
      JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
    WHERE
      userid = 251
    ORDER BY
      1
  ) a
where  a.n = 1
or a.n2 = 1;

/*********************************************************
-- WITH 문을 이용하여 서브 쿼리 실행
-- 251번 사용자의 사용자별 처음 채널과 마지막 채널 알아내기
*********************************************************/

with channel as (
  SELECT
    ts,
    channel,
    ROW_NUMBER() OVER (
      PARTITION BY usc.userid
      ORDER BY
        ts
    ) as n,
    ROW_NUMBER() OVER (
      PARTITION BY usc.userid
      ORDER BY
        ts desc
    ) as n2
  FROM
    raw_data.user_session_channel usc
    JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
  WHERE
    userid = 251
  ORDER BY
    1
)
select
  *
from
  channel;

