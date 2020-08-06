/*********************************************************
-- Assignment - 2
-- Gross Revenue가 가장 큰 UserID 10개 찾기
-- Gross revenue: Refund 포함한 매출
*********************************************************/
SELECT   a.userid,
         SUM( b.amount ) AS gross_revenue
FROM     raw_data.user_session_channel a
         JOIN raw_data.session_transaction b
         ON a.sessionid = b.sessionid
GROUP BY a.userid
ORDER BY 2 DESC
LIMIT 10