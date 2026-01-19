CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_refvisittypecrosswalk
(
  sourceaprimarykeyvalue INT64,
  dispenctypename STRING,
  visittyperequiredclaim INT64 NOT NULL,
  visittyperequiredcopay INT64 NOT NULL,
  visittypepregnancyvisit INT64 NOT NULL,
  visittypeactiveflag INT64 NOT NULL,
  visittypeorthovisit INT64 NOT NULL,
  visittypeobgynvisit INT64 NOT NULL,
  visittypeisvisit INT64 NOT NULL,
  visittypewebvisit INT64 NOT NULL,
  visittypephysicaltherapyvisit INT64 NOT NULL
)
;