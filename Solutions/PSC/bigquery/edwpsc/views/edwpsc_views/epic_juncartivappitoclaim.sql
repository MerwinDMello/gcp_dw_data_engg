CREATE OR REPLACE VIEW edwpsc_views.`epic_juncartivappitoclaim`
AS SELECT
  `epic_juncartivappitoclaim`.artivappitoclaimkey,
  `epic_juncartivappitoclaim`.claimkey,
  `epic_juncartivappitoclaim`.claimnumber,
  `epic_juncartivappitoclaim`.regionkey,
  `epic_juncartivappitoclaim`.coid,
  `epic_juncartivappitoclaim`.practicefederaltaxid,
  `epic_juncartivappitoclaim`.cpid,
  `epic_juncartivappitoclaim`.providerid,
  `epic_juncartivappitoclaim`.providernpi,
  `epic_juncartivappitoclaim`.facilityid,
  `epic_juncartivappitoclaim`.payerfinancialclass,
  `epic_juncartivappitoclaim`.ppikey,
  `epic_juncartivappitoclaim`.ppieffectivedate,
  `epic_juncartivappitoclaim`.holdruleid,
  `epic_juncartivappitoclaim`.dwlastupdatedatetime,
  `epic_juncartivappitoclaim`.sourcesystemcode,
  `epic_juncartivappitoclaim`.insertedby,
  `epic_juncartivappitoclaim`.inserteddtm,
  `epic_juncartivappitoclaim`.modifiedby,
  `epic_juncartivappitoclaim`.modifieddtm,
  `epic_juncartivappitoclaim`.payer1cpid,
  `epic_juncartivappitoclaim`.payer1ppikey,
  `epic_juncartivappitoclaim`.payer1ppieffectivedate,
  `epic_juncartivappitoclaim`.payer1ppialternateeffectivedate,
  `epic_juncartivappitoclaim`.payer1financialclass
  FROM
    edwpsc_base_views.`epic_juncartivappitoclaim`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_juncartivappitoclaim`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;