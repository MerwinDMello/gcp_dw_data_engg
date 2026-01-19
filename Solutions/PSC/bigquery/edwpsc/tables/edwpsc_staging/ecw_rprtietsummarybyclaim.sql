CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_rprtietsummarybyclaim
(
  snapshotdate DATE NOT NULL,
  groupname STRING,
  divisionname STRING,
  marketname STRING,
  coidlob STRING,
  coidsublob STRING,
  coid STRING NOT NULL,
  coidname STRING,
  claimkey INT64,
  claimnumber INT64,
  ageindays INT64,
  errortype STRING,
  subcategoryname STRING,
  placeofservicecode STRING,
  totalbalance NUMERIC(33, 4),
  ieterrorcount INT64,
  claimunbilledstatuskey INT64,
  unbilledstatuscategory STRING,
  unbilledstatussubcategory STRING,
  servicingproviderid INT64,
  servicingprovidername STRING,
  servicingprovidernpi STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;