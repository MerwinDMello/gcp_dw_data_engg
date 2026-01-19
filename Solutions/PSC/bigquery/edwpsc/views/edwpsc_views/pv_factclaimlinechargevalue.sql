CREATE OR REPLACE VIEW edwpsc_views.`pv_factclaimlinechargevalue`
AS SELECT
  `pv_factclaimlinechargevalue`.claimlinechargevaluekey,
  `pv_factclaimlinechargevalue`.processeddate,
  `pv_factclaimlinechargevalue`.processedchargevalueprioritynum,
  `pv_factclaimlinechargevalue`.processedchargevaluetype,
  `pv_factclaimlinechargevalue`.processedlastrunflag,
  `pv_factclaimlinechargevalue`.chargevaluekey,
  `pv_factclaimlinechargevalue`.claimkey,
  `pv_factclaimlinechargevalue`.claimnumber,
  `pv_factclaimlinechargevalue`.coid,
  `pv_factclaimlinechargevalue`.claimlinechargekey,
  `pv_factclaimlinechargevalue`.practiceid,
  `pv_factclaimlinechargevalue`.adjcode,
  `pv_factclaimlinechargevalue`.cptbalance,
  `pv_factclaimlinechargevalue`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_factclaimlinechargevalue`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factclaimlinechargevalue`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;