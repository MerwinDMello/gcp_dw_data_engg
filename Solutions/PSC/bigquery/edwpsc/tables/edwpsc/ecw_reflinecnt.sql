CREATE TABLE IF NOT EXISTS edwpsc.ecw_reflinecnt
(
  linecnt INT64 NOT NULL,
  linedesc STRING NOT NULL,
  linesubgroup STRING NOT NULL,
  sourcearecordlastupdated DATETIME,
  sourcebrecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  linesubgrouporder INT64,
  PRIMARY KEY (linecnt) NOT ENFORCED
)
;