CREATE OR REPLACE VIEW edwpsc_views.`ecw_factecwremarkcodeline`
AS SELECT
  `ecw_factecwremarkcodeline`.ecwremarkcodelinekey,
  `ecw_factecwremarkcodeline`.claimkey,
  `ecw_factecwremarkcodeline`.claimnumber,
  `ecw_factecwremarkcodeline`.claimlinechargeskey,
  `ecw_factecwremarkcodeline`.coid,
  `ecw_factecwremarkcodeline`.coidconfigurationkey,
  `ecw_factecwremarkcodeline`.servicingproviderkey,
  `ecw_factecwremarkcodeline`.claimpayer1iplankey,
  `ecw_factecwremarkcodeline`.claimlinepaymentskey,
  `ecw_factecwremarkcodeline`.regionkey,
  `ecw_factecwremarkcodeline`.remarkdetailid,
  `ecw_factecwremarkcodeline`.remarkcodetype,
  `ecw_factecwremarkcodeline`.remarkcode,
  `ecw_factecwremarkcodeline`.remarkdeleteflag,
  `ecw_factecwremarkcodeline`.sourceaprimarykey,
  `ecw_factecwremarkcodeline`.dwlastupdatedatetime,
  `ecw_factecwremarkcodeline`.sourcesystemcode,
  `ecw_factecwremarkcodeline`.insertedby,
  `ecw_factecwremarkcodeline`.inserteddtm,
  `ecw_factecwremarkcodeline`.modifiedby,
  `ecw_factecwremarkcodeline`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_factecwremarkcodeline`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factecwremarkcodeline`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;