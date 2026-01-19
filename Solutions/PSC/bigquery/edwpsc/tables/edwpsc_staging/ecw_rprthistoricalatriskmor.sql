CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_rprthistoricalatriskmor
(
  snapshotdate DATE,
  atrisksnapshotdate DATE,
  centerdescription STRING,
  group_name STRING,
  division_name STRING,
  lob_code STRING,
  coid STRING,
  coid_name STRING,
  coidandname STRING,
  currentmonthatrisk NUMERIC(33, 4),
  priormonthatrisk NUMERIC(33, 4),
  nonparadj NUMERIC(33, 4),
  cmdosatrisk NUMERIC(33, 4),
  total_balance_2mp NUMERIC(33, 4),
  total_balance_3mp NUMERIC(33, 4),
  total_balance_4mp NUMERIC(33, 4),
  adjamt_pm NUMERIC(33, 4),
  adjamt_2mp NUMERIC(33, 4),
  adjamt_3mp NUMERIC(33, 4),
  insertedby STRING,
  inserteddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;