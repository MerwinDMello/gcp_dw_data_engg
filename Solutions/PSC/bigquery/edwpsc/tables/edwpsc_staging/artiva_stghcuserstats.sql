CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stghcuserstats
(
  hcususerid STRING NOT NULL,
  hcusyear INT64,
  hcusmonth INT64,
  hcusday STRING,
  hcusdept STRING,
  hcusfacility STRING,
  hcussubunit STRING,
  hcuspayer STRING,
  hcusacctviewed NUMERIC(29),
  hcusacctwrkd NUMERIC(29),
  hcusbrknnum NUMERIC(29),
  hcuscalls NUMERIC(29),
  hcuscontacts NUMERIC(29),
  hcusinsfound NUMERIC(29),
  hcusinsptp NUMERIC(31, 2),
  hcusinsptpnum NUMERIC(29),
  hcusltrs NUMERIC(29),
  hcuspayments NUMERIC(31, 2),
  hcusplaced NUMERIC(29),
  hcusptpamt NUMERIC(31, 2),
  hcusptpnum NUMERIC(29),
  hcusrecall NUMERIC(29),
  hcussettleamt NUMERIC(31, 2),
  hcussettlenum NUMERIC(29),
  hcustime STRING
)
;