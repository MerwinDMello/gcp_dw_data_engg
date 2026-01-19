CREATE OR REPLACE VIEW edwpsc_views.`pv_juncartivappitoclaim`
AS SELECT
  `pv_juncartivappitoclaim`.artivappitoclaimkey,
  `pv_juncartivappitoclaim`.claimkey,
  `pv_juncartivappitoclaim`.claimnumber,
  `pv_juncartivappitoclaim`.regionkey,
  `pv_juncartivappitoclaim`.coid,
  `pv_juncartivappitoclaim`.practicefederaltaxid,
  `pv_juncartivappitoclaim`.cpid,
  `pv_juncartivappitoclaim`.providerid,
  `pv_juncartivappitoclaim`.providernpi,
  `pv_juncartivappitoclaim`.facilityid,
  `pv_juncartivappitoclaim`.payerfinancialclass,
  `pv_juncartivappitoclaim`.ppikey,
  `pv_juncartivappitoclaim`.ppieffectivedate,
  `pv_juncartivappitoclaim`.holdruleid,
  `pv_juncartivappitoclaim`.dwlastupdatedatetime,
  `pv_juncartivappitoclaim`.sourcesystemcode,
  `pv_juncartivappitoclaim`.insertedby,
  `pv_juncartivappitoclaim`.inserteddtm,
  `pv_juncartivappitoclaim`.modifiedby,
  `pv_juncartivappitoclaim`.modifieddtm,
  `pv_juncartivappitoclaim`.payer1practicefederaltaxid,
  `pv_juncartivappitoclaim`.payer1facilityid,
  `pv_juncartivappitoclaim`.payer1cpid,
  `pv_juncartivappitoclaim`.payer1ppikey,
  `pv_juncartivappitoclaim`.payer1ppieffectivedate,
  `pv_juncartivappitoclaim`.payer1financialclass
  FROM
    edwpsc_base_views.`pv_juncartivappitoclaim`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_juncartivappitoclaim`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;