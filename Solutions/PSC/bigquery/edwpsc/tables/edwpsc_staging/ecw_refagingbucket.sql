CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refagingbucket
(
  ageindays INT64 NOT NULL,
  bucketdescription1 STRING,
  bucketdescription2 STRING,
  bucketdescription3 STRING,
  percnt NUMERIC(31, 2),
  percentbucket_0_10_gt100 STRING,
  percentbucket_0_10_gt100sort INT64,
  agingbucket_0_30_nomax STRING,
  agingbucket_0_30_nomaxsort STRING,
  agingbucket_000_030_720plus STRING,
  agingbucket_000_030_720plussort INT64,
  agingbucket_a000_a030_nomax STRING,
  agingbucket_a000_a030_nomaxsort STRING,
  agingbucket_0_6_gt365 STRING,
  agingbucket_0_6_gt365sort INT64,
  agingbucket_0_30_gt360 STRING,
  agingbucket_0_30_gt360sort INT64
)
;