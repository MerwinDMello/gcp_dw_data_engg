CREATE OR REPLACE VIEW edwpsc_views.`ecw_factecwcascodeline`
AS SELECT
  `ecw_factecwcascodeline`.ecwcascodelinekey,
  `ecw_factecwcascodeline`.claimkey,
  `ecw_factecwcascodeline`.claimnumber,
  `ecw_factecwcascodeline`.claimlinechargeskey,
  `ecw_factecwcascodeline`.coid,
  `ecw_factecwcascodeline`.coidconfigurationkey,
  `ecw_factecwcascodeline`.servicingproviderkey,
  `ecw_factecwcascodeline`.claimpayer1iplankey,
  `ecw_factecwcascodeline`.claimlinepaymentskey,
  `ecw_factecwcascodeline`.cascodekey,
  `ecw_factecwcascodeline`.regionkey,
  `ecw_factecwcascodeline`.casdetailid,
  `ecw_factecwcascodeline`.casgroupcode,
  `ecw_factecwcascodeline`.cascode,
  `ecw_factecwcascodeline`.casamount,
  `ecw_factecwcascodeline`.casdeleteflag,
  `ecw_factecwcascodeline`.caspostedas,
  `ecw_factecwcascodeline`.sourceaprimarykey,
  `ecw_factecwcascodeline`.dwlastupdatedatetime,
  `ecw_factecwcascodeline`.sourcesystemcode,
  `ecw_factecwcascodeline`.insertedby,
  `ecw_factecwcascodeline`.inserteddtm,
  `ecw_factecwcascodeline`.modifiedby,
  `ecw_factecwcascodeline`.modifieddtm,
  `ecw_factecwcascodeline`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_factecwcascodeline`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factecwcascodeline`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;