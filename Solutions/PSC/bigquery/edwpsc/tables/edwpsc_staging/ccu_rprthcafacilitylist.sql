CREATE TABLE IF NOT EXISTS edwpsc_staging.ccu_rprthcafacilitylist
(
  contenttypeid STRING,
  facilityname STRING,
  designation STRING,
  poskey INT64,
  coid STRING,
  complianceassetid STRING,
  id INT64,
  contenttype STRING,
  modified DATETIME,
  created DATETIME,
  createdbyid INT64,
  modifiedbyid INT64,
  owshiddenversion INT64,
  version STRING,
  path STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME
)
;