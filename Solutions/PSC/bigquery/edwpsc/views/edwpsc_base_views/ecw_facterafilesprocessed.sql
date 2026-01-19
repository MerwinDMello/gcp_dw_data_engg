CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_facterafilesprocessed`
AS SELECT
  `ecw_facterafilesprocessed`.erafilesprocessedkey,
  `ecw_facterafilesprocessed`.claimkey,
  `ecw_facterafilesprocessed`.claimnumber,
  `ecw_facterafilesprocessed`.sourcefilename,
  `ecw_facterafilesprocessed`.filename,
  `ecw_facterafilesprocessed`.sourcefilecreateddate,
  `ecw_facterafilesprocessed`.datecreated,
  `ecw_facterafilesprocessed`.transactionnumber,
  `ecw_facterafilesprocessed`.trn03,
  `ecw_facterafilesprocessed`.iet_processed,
  `ecw_facterafilesprocessed`.posteddate,
  `ecw_facterafilesprocessed`.sourceprimarykeyvalue,
  `ecw_facterafilesprocessed`.dwlastupdatedatetime,
  `ecw_facterafilesprocessed`.sourcesystemcode,
  `ecw_facterafilesprocessed`.insertedby,
  `ecw_facterafilesprocessed`.inserteddtm,
  `ecw_facterafilesprocessed`.modifiedby,
  `ecw_facterafilesprocessed`.modifieddtm,
  `ecw_facterafilesprocessed`.deletedflag,
  `ecw_facterafilesprocessed`.fullclaimnumber,
  `ecw_facterafilesprocessed`.regionkey
  FROM
    edwpsc.`ecw_facterafilesprocessed`
;