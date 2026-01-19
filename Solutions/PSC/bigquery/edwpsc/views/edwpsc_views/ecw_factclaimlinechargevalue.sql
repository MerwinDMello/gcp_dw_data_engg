CREATE OR REPLACE VIEW edwpsc_views.`ecw_factclaimlinechargevalue`
AS SELECT
  `ecw_factclaimlinechargevalue`.claimlinechargevaluekey,
  `ecw_factclaimlinechargevalue`.processeddate,
  `ecw_factclaimlinechargevalue`.processedchargevalueprioritynum,
  `ecw_factclaimlinechargevalue`.processedchargevaluetype,
  `ecw_factclaimlinechargevalue`.processedlastrunflag,
  `ecw_factclaimlinechargevalue`.chargevaluekey,
  `ecw_factclaimlinechargevalue`.claimkey,
  `ecw_factclaimlinechargevalue`.claimnumber,
  `ecw_factclaimlinechargevalue`.coid,
  `ecw_factclaimlinechargevalue`.claimlinechargekey,
  `ecw_factclaimlinechargevalue`.practiceid,
  `ecw_factclaimlinechargevalue`.adjcode,
  `ecw_factclaimlinechargevalue`.cptbalance,
  `ecw_factclaimlinechargevalue`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_factclaimlinechargevalue`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factclaimlinechargevalue`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;