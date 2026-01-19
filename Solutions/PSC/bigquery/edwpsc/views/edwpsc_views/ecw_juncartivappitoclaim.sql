CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncartivappitoclaim`
AS SELECT
  `ecw_juncartivappitoclaim`.artivappitoclaimkey,
  `ecw_juncartivappitoclaim`.claimkey,
  `ecw_juncartivappitoclaim`.claimnumber,
  `ecw_juncartivappitoclaim`.regionkey,
  `ecw_juncartivappitoclaim`.coid,
  `ecw_juncartivappitoclaim`.practicefederaltaxid,
  `ecw_juncartivappitoclaim`.cpid,
  `ecw_juncartivappitoclaim`.providerid,
  `ecw_juncartivappitoclaim`.providernpi,
  `ecw_juncartivappitoclaim`.facilityid,
  `ecw_juncartivappitoclaim`.payerfinancialclass,
  `ecw_juncartivappitoclaim`.ppikey,
  `ecw_juncartivappitoclaim`.ppieffectivedate,
  `ecw_juncartivappitoclaim`.holdruleid,
  `ecw_juncartivappitoclaim`.dwlastupdatedatetime,
  `ecw_juncartivappitoclaim`.sourcesystemcode,
  `ecw_juncartivappitoclaim`.insertedby,
  `ecw_juncartivappitoclaim`.inserteddtm,
  `ecw_juncartivappitoclaim`.modifiedby,
  `ecw_juncartivappitoclaim`.modifieddtm,
  `ecw_juncartivappitoclaim`.payer1practicefederaltaxid,
  `ecw_juncartivappitoclaim`.payer1facilityid,
  `ecw_juncartivappitoclaim`.payer1cpid,
  `ecw_juncartivappitoclaim`.payer1ppikey,
  `ecw_juncartivappitoclaim`.payer1ppieffectivedate,
  `ecw_juncartivappitoclaim`.payer1financialclass,
  `ecw_juncartivappitoclaim`.arclaimflag
  FROM
    edwpsc_base_views.`ecw_juncartivappitoclaim`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncartivappitoclaim`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;