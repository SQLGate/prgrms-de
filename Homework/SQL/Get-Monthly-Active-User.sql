SELECT
  to_char(st.ts, 'YYYY-MM') as Monthly,
  count(DISTINCT usc.userid) as ActiveUser
FROM
  raw_data.session_timestamp st
  JOIN raw_data.user_session_channel usc ON st.sessionid = usc.sessionid
GROUP BY 
  to_char(st.ts, 'YYYY-MM')
ORDER BY
  1