CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_rprticcsubcategoryerrors`
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
    edwpsc.`ecw_rprticcsubcategoryerrors`
;