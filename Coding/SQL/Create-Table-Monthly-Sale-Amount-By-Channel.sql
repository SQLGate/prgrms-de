/*********************************************************
-- 채널별 월 매출액 테이블 만들기
-- session_timestamp, user_session_channel, channel, session_transaction 테이블들을 사용
-- channel에 있는 모든 채널에 대해 구성해야함 (값이 없는 경우라도)
-- 아래와 같은 필드로 구성
-- month
-- channel
-- uniqueUsers (총방문 사용자)
-- paidUsers (구매 사용자: refund한 경우도 판매로 고려)
-- conversionRate (구매사용자 / 총방문 사용자)
-- grossRevenue (refund 포함)
-- netRevenue (refund 제외)
*********************************************************/
WITH revenueChannel AS
(
    SELECT
            LEFT(d.ts, 7) AS month,
            a.channelname,
            COUNT(DISTINCT(b.userid)) AS uniqueUsers,
            COUNT(
                DISTINCT CASE
                WHEN c.amount > 0 THEN b.userid
                ELSE NULL
                END
            ) paidUsers,
            SUM(c.amount) AS grossRevenue,
            SUM(
                CASE
                c.refunded
                WHEN False THEN c.amount
                END
            ) AS netRevenue
    FROM
            raw_data.channel a
            LEFT JOIN raw_data.user_session_channel b
              JOIN raw_data.session_transaction c 
                ON b.sessionid = c.sessionid
              JOIN raw_data.session_timestamp d 
                ON b.sessionid = d.sessionid 
            ON a.channelname = b.channel
    GROUP BY
        1,2
    ORDER BY
        1,2
)
SELECT   a.month,
         a.channelname,
         a.uniqueUsers,
         a.paidUsers,
         ROUND(( a.paidUsers::NUMERIC / NULLIF( a.uniqueUsers::NUMERIC,0 ) )*100,2 ) AS conversionRate,
         a.grossRevenue,
         a.netRevenue
FROM     revenueChannel a