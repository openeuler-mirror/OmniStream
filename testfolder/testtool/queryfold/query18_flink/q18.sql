create table  bid(
                     auction BIGINT,
                     bidder  BIGINT,
                     price BIGINT,
                     channel VARCHAR,
                     url  VARCHAR,
                     dateTime TIMESTAMP(3),
                     extra VARCHAR
)WITH(
     'connector' = 'filesystem',
     'path' = '/repo/codehub/test_tool/bids_data.csv',
     'format' = 'csv',
     'csv.field-delimiter' = ',',
     'csv.ignore-parse-errors' = 'true'

     );



SELECT auction, bidder, price, channel, url, dateTime, extra
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY dateTime DESC) AS rank_number
      FROM bid)
WHERE rank_number <= 1;