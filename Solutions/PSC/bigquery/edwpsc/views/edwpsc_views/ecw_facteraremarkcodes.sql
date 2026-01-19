CREATE OR REPLACE VIEW edwpsc_views.`ecw_facteraremarkcodes`
AS SELECT
  `ecw_facteraremarkcodes`.eraremarkcodeskey,
  `ecw_facteraremarkcodes`.claimkey,
  `ecw_facteraremarkcodes`.claimnumber,
  `ecw_facteraremarkcodes`.lq01,
  `ecw_facteraremarkcodes`.lq02,
  `ecw_facteraremarkcodes`.segment,
  `ecw_facteraremarkcodes`.sourcefilename,
  `ecw_facteraremarkcodes`.sourcefilecreateddate,
  `ecw_facteraremarkcodes`.seq,
  `ecw_facteraremarkcodes`.datecreated,
  `ecw_facteraremarkcodes`.sourceprimarykeyvalue,
  `ecw_facteraremarkcodes`.dwlastupdatedatetime,
  `ecw_facteraremarkcodes`.sourcesystemcode,
  `ecw_facteraremarkcodes`.insertedby,
  `ecw_facteraremarkcodes`.inserteddtm,
  `ecw_facteraremarkcodes`.modifiedby,
  `ecw_facteraremarkcodes`.modifieddtm,
  `ecw_facteraremarkcodes`.deletedflag,
  `ecw_facteraremarkcodes`.fullclaimnumber,
  `ecw_facteraremarkcodes`.regionkey
  FROM
    edwpsc_base_views.`ecw_facteraremarkcodes`
;