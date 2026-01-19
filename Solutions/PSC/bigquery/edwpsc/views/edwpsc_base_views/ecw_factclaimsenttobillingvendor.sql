CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factclaimsenttobillingvendor`
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
    edwpsc.`ecw_factclaimsenttobillingvendor`
;