CREATE OR REPLACE VIEW edwpsc_views.`ecw_rprticcsubcategoryerrors`
AS SELECT
  `ecw_rprticcsubcategoryerrors`.snapshotdate,
  `ecw_rprticcsubcategoryerrors`.coid,
  `ecw_rprticcsubcategoryerrors`.claimkey,
  `ecw_rprticcsubcategoryerrors`.claimnumber,
  `ecw_rprticcsubcategoryerrors`.subcategorydescription,
  `ecw_rprticcsubcategoryerrors`.errorcount,
  `ecw_rprticcsubcategoryerrors`.insertedby,
  `ecw_rprticcsubcategoryerrors`.inserteddtm,
  `ecw_rprticcsubcategoryerrors`.modifiedby,
  `ecw_rprticcsubcategoryerrors`.modifieddtm,
  `ecw_rprticcsubcategoryerrors`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_rprticcsubcategoryerrors`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_rprticcsubcategoryerrors`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;