CREATE TABLE IF NOT EXISTS edwpsc.epic_juncgroupcoid
(
  juncgroupcoidkey INT64 NOT NULL,
  groupkey INT64 NOT NULL,
  coid STRING NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (juncgroupcoidkey) NOT ENFORCED
)
;