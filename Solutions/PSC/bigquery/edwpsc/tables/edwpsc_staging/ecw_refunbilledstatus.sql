CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refunbilledstatus
(
  unbilledstatuskey INT64 NOT NULL,
  unbilledstatuscategory STRING NOT NULL,
  unbilledstatussubcategory STRING NOT NULL,
  unbilledonholdflag INT64 NOT NULL,
  unbilledunbilledflag INT64 NOT NULL,
  unbilledbilledflag INT64 NOT NULL,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64
)
;