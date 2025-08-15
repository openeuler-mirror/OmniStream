CREATE TABLE OrderOut(
                         price        BIGINT,
                         order_number BIGINT,
                         buyer         BIGINT
) WITH (
      'connector' = 'blackhole'
      );


CREATE TABLE Orders10 (
                          order_number BIGINT,
                          price        BIGINT,
                          buyer         BIGINT
) WITH (
      'connector' = 'datagen',
      'number-of-rows' = '10'
      );


CREATE TABLE Orders1M (
                          order_number BIGINT,
                          price        BIGINT,
                          buyer         BIGINT
) WITH (
      'connector' = 'datagen',
      'number-of-rows' = '1000000'
    )
;


CREATE TABLE Orders10MT1 (
                             order_number BIGINT,
                             price        BIGINT,
                             buyer         BIGINT
) WITH (
      'connector' = 'datagen',
      'number-of-rows' = '10000000',
      'rows-per-second' = '500000',
      'scan.parallelism' = '1'
      );


CREATE TABLE Orders10MT1VS (
                             order_number BIGINT,
                             price        BIGINT,
                             buyer         BIGINT
) WITH (
      'connector' = 'datagen',
      'number-of-rows' = '10000000',
      'rows-per-second' =   '50000',
      'scan.parallelism' = '1'
      );


CREATE TABLE Orders100MT1 (
                              order_number BIGINT,
                              price        BIGINT,
                              buyer         BIGINT
) WITH (
      'connector' = 'datagen',
      'number-of-rows' = '100000000',
      'rows-per-second' =   '500000',
      'scan.parallelism' = '1'
      );


CREATE TABLE OrdersT1N1GR1M (
                              order_number BIGINT,
                              price        BIGINT,
                              buyer         BIGINT
) WITH (
      'connector' = 'datagen',
      'number-of-rows' = '1000000000',
      'rows-per-second' = '1000000',
      'scan.parallelism' = '1'
      );

CREATE TABLE OrdersT4N2K (
                                order_number BIGINT,
                                price        BIGINT,
                                buyer         BIGINT
) WITH (
      'connector' = 'datagen',
      'number-of-rows' = '2000',
      'scan.parallelism' = '4'
      );



CREATE TABLE OrdersT4N100K (
                             order_number BIGINT,
                             price        BIGINT,
                             buyer         BIGINT
) WITH (
      'connector' = 'datagen',
      'number-of-rows' = '100000',
      'scan.parallelism' = '4'
      );
