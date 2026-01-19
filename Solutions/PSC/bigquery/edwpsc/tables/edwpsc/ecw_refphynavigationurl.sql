CREATE TABLE IF NOT EXISTS edwpsc.ecw_refphynavigationurl
(
  dwcrefphynavigationurlkey INT64 NOT NULL,
  defaultaccess STRING,
  document_id STRING,
  project STRING,
  image_path STRING,
  url STRING,
  PRIMARY KEY (dwcrefphynavigationurlkey) NOT ENFORCED
)
;