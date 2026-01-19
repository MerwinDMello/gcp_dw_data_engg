CREATE OR REPLACE VIEW edwpsc_views.`pv_factclaimbill`
AS SELECT
  `pv_factclaimbill`.billkey,
  `pv_factclaimbill`.claimkey,
  `pv_factclaimbill`.claimnumber,
  `pv_factclaimbill`.regionkey,
  `pv_factclaimbill`.coid,
  `pv_factclaimbill`.coidconfigurationkey,
  `pv_factclaimbill`.servicingproviderkey,
  `pv_factclaimbill`.claimpayer1iplankey,
  `pv_factclaimbill`.facilitykey,
  `pv_factclaimbill`.billtypekey,
  `pv_factclaimbill`.iplankey,
  `pv_factclaimbill`.billmessage,
  `pv_factclaimbill`.batchnumber,
  `pv_factclaimbill`.billdatekey,
  `pv_factclaimbill`.billtime,
  `pv_factclaimbill`.practicekey,
  `pv_factclaimbill`.practicename,
  `pv_factclaimbill`.sourceprimarykeyvalue,
  `pv_factclaimbill`.sourcerecordlastupdated,
  `pv_factclaimbill`.dwlastupdatedatetime,
  `pv_factclaimbill`.sourcesystemcode,
  `pv_factclaimbill`.insertedby,
  `pv_factclaimbill`.inserteddtm,
  `pv_factclaimbill`.modifiedby,
  `pv_factclaimbill`.modifieddtm
  FROM
    edwpsc_base_views.`pv_factclaimbill`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factclaimbill`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;