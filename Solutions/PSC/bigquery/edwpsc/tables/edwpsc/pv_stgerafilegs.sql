CREATE TABLE IF NOT EXISTS edwpsc.pv_stgerafilegs
(
  gs_id INT64 NOT NULL,
  fileid INT64,
  gs01 STRING,
  gs02 STRING,
  gs03 STRING,
  gs04 STRING,
  gs05 STRING,
  gs06 STRING,
  gs07 STRING,
  gs08 STRING,
  gssegment STRING,
  inserteddtm DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL
)
;