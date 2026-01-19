CREATE OR REPLACE VIEW edwpsc_views.`ecw_factclaimsenttobillingvendor`
AS SELECT
  `ecw_factclaimsenttobillingvendor`.senttobillingvendorkey,
  `ecw_factclaimsenttobillingvendor`.claimkey,
  `ecw_factclaimsenttobillingvendor`.claimnumber,
  `ecw_factclaimsenttobillingvendor`.regionkey,
  `ecw_factclaimsenttobillingvendor`.coid,
  `ecw_factclaimsenttobillingvendor`.iplankey,
  `ecw_factclaimsenttobillingvendor`.senttobillingvendormessage,
  `ecw_factclaimsenttobillingvendor`.senttobillingvendordatekey,
  `ecw_factclaimsenttobillingvendor`.senttobillingvendordatetime,
  `ecw_factclaimsenttobillingvendor`.sourceprimarykeyvalue,
  `ecw_factclaimsenttobillingvendor`.sourcerecordlastupdated,
  `ecw_factclaimsenttobillingvendor`.dwlastupdatedatetime,
  `ecw_factclaimsenttobillingvendor`.sourcesystemcode,
  `ecw_factclaimsenttobillingvendor`.insertedby,
  `ecw_factclaimsenttobillingvendor`.inserteddtm,
  `ecw_factclaimsenttobillingvendor`.modifiedby,
  `ecw_factclaimsenttobillingvendor`.modifieddtm,
  `ecw_factclaimsenttobillingvendor`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_factclaimsenttobillingvendor`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factclaimsenttobillingvendor`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;