CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncclaimtopatientaccounting`
AS SELECT
  `ecw_juncclaimtopatientaccounting`.claimkey,
  `ecw_juncclaimtopatientaccounting`.meditechcocid,
  `ecw_juncclaimtopatientaccounting`.patientaccountnumber,
  `ecw_juncclaimtopatientaccounting`.patient_dw_id,
  `ecw_juncclaimtopatientaccounting`.hospitalcoidname,
  `ecw_juncclaimtopatientaccounting`.hospitalcoid,
  `ecw_juncclaimtopatientaccounting`.meditechvisitnumber,
  `ecw_juncclaimtopatientaccounting`.ecw_claimnumber,
  `ecw_juncclaimtopatientaccounting`.ecw_claimservicedate,
  `ecw_juncclaimtopatientaccounting`.ecw_region,
  `ecw_juncclaimtopatientaccounting`.ecw_primaryfc,
  `ecw_juncclaimtopatientaccounting`.ecw_claimdeleteflag,
  `ecw_juncclaimtopatientaccounting`.ecw_claimvoidflag,
  `ecw_juncclaimtopatientaccounting`.ecw_totalbalanceamt,
  `ecw_juncclaimtopatientaccounting`.ecw_totalinsurancepaymentsamt,
  `ecw_juncclaimtopatientaccounting`.ecw_totalpatientpaymentsamt,
  `ecw_juncclaimtopatientaccounting`.pa_empi,
  `ecw_juncclaimtopatientaccounting`.ecw_empi,
  `ecw_juncclaimtopatientaccounting`.foundintype
  FROM
    edwpsc.`ecw_juncclaimtopatientaccounting`
;