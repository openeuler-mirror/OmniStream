-- -------------------------------------------------------------------------------------------------
-- Query 11: User Sessions (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids did a user make in each session they were active? Illustrates session windows.
--
-- Group bids by the same user into sessions with max session gap.
-- Emit the number of bids per session.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE bid (
  bidder BIGINT,
  bid_count BIGINT,
  starttime TIMESTAMP(3),
  endtime TIMESTAMP(3)
) WITH (
    'connector' = 'filesystem',
    'path' = '/repo/codehub/test_tool/csv_files/nexmark_q11.csv',
    'format' = 'csv',
    'csv.field-delimiter' = ',',
    'csv.ignore-parse-errors' = 'true'
);

-- INSERT INTO nexmark_q11
SELECT
    B.bidder,
    count(*) as bid_count,
    SESSION_START(B.dateTime, INTERVAL '10' SECOND) as starttime,
    SESSION_END(B.dateTime, INTERVAL '10' SECOND) as endtime
FROM bid B
GROUP BY B.bidder, SESSION(B.dateTime, INTERVAL '10' SECOND);