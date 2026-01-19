CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_juncclaimtopatientaccounting
(
  claimkey INT64 NOT NULL,
  meditechcocid STRING,
  patientaccountnumber INT64,
  patient_dw_id INT64,
  hospitalcoidname STRING,
  hospitalcoid STRING NOT NULL,
  meditechvisitnumber STRING,
  ecw_claimnumber INT64 NOT NULL,
  ecw_claimservicedate DATE NOT NULL,
  ecw_region INT64 NOT NULL,
  ecw_primaryfc INT64,
  ecw_claimdeleteflag INT64 NOT NULL,
  ecw_claimvoidflag INT64 NOT NULL,
  ecw_totalbalanceamt NUMERIC(33, 4) NOT NULL,
  ecw_totalinsurancepaymentsamt NUMERIC(33, 4) NOT NULL,
  ecw_totalpatientpaymentsamt NUMERIC(33, 4) NOT NULL,
  pa_empi INT64,
  ecw_empi STRING,
  foundintype INT64 NOT NULL
)
;