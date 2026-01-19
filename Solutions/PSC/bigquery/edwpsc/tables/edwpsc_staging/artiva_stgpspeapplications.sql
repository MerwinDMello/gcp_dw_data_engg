CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeapplications
(
  pspeappactiongroup STRING,
  pspeappactive STRING,
  pspeappamtfield1 STRING,
  pspeappamtfield2 STRING,
  pspeappchrfield1 STRING,
  pspeappchrfield10 STRING,
  pspeappchrfield2 STRING,
  pspeappchrfield3 STRING,
  pspeappchrfield4 STRING,
  pspeappchrfield5 STRING,
  pspeappchrfield6 STRING,
  pspeappchrfield7 STRING,
  pspeappchrfield8 STRING,
  pspeappchrfield9 STRING,
  pspeappdesc STRING,
  pspeappdtefield1 STRING,
  pspeappdtefield2 STRING,
  pspeappenableins STRING,
  pspeappextfile STRING,
  pspeappkey NUMERIC(29) NOT NULL,
  PRIMARY KEY (pspeappkey) NOT ENFORCED
)
;