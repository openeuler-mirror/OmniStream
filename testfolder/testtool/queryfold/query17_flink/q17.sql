SET sql-client.execution.result-mode=TABLEAU;
CREATE TABLE bid (

                     auction BIGINT,
                     bidder BIGINT,
                     price  BIGINT,
                     dateTime TIMESTAMP(3)
) WITH (
      'connector' = 'filesystem',
      'path' = '/repo/codehub/test_tool/csv_files/bid17.csv',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'csv.ignore-parse-errors' = 'true'
      );



-- CREATE TABLE nexmark_q0_output (
--                                    auction BIGINT,
--                                    `day` VARCHAR,
--                                    total_bids BIGINT,
--                                    rank1_bids BIGINT,
--                                    rank2_bids BIGINT,
--                                    rank3_bids BIGINT,
--                                    min_price BIGINT,
--                                    max_price BIGINT,
--                                    avg_price BIGINT,
--                                    sum_price BIGINT
-- ) WITH (
--
--
--                                      'connector' = 'filesystem',
--                                      'path' = '/repo/codehub/test_tool/csv_files/nexmark_q17_output',
--                                      'format' = 'csv',
--                                      'csv.field-delimiter' = ',',
--                                      'csv.ignore-parse-errors' = 'true',
--                                      'sink.rolling-policy.rollover-interval' = '1h',  -- 控制文件滚动策略
--                                      'sink.partition-commit.policy.kind' = 'success-file',  -- 确保成功文件标记
--                                      'sink.rolling-policy.check-interval' = '10s',  -- 检查滚动条件的间隔
--                                      'sink.rolling-policy.file-size' = '128mb'  -- 文件大小限制
--
--                                      );
--

-- INSERT INTO nexmark_q0_output

SELECT
    auction,
    DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
    count(*) AS total_bids,
    count(*) filter (where price < 10000) AS rank1_bids,
        count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
        count(*) filter (where price >= 1000000) AS rank3_bids,
        min(price) AS min_price,
    max(price) AS max_price,
    avg(price) AS avg_price,
    sum(price) AS sum_price
FROM bid
GROUP BY auction, DATE_FORMAT(dateTime, 'yyyy-MM-dd');