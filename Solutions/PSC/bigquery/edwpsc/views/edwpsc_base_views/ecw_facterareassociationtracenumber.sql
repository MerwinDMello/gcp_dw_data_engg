CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_facterareassociationtracenumber`
AS SELECT
  `ecw_facterareassociationtracenumber`.erareassociationtracenumberkey,
  `ecw_facterareassociationtracenumber`.claimkey,
  `ecw_facterareassociationtracenumber`.claimnumber,
  `ecw_facterareassociationtracenumber`.trn01,
  `ecw_facterareassociationtracenumber`.trn02,
  `ecw_facterareassociationtracenumber`.trn03,
  `ecw_facterareassociationtracenumber`.trn04,
  `ecw_facterareassociationtracenumber`.segment,
  `ecw_facterareassociationtracenumber`.sourcefilename,
  `ecw_facterareassociationtracenumber`.sourcefilecreateddate,
  `ecw_facterareassociationtracenumber`.seq,
  `ecw_facterareassociationtracenumber`.datecreated,
  `ecw_facterareassociationtracenumber`.sourceprimarykeyvalue,
  `ecw_facterareassociationtracenumber`.dwlastupdatedatetime,
  `ecw_facterareassociationtracenumber`.sourcesystemcode,
  `ecw_facterareassociationtracenumber`.insertedby,
  `ecw_facterareassociationtracenumber`.inserteddtm,
  `ecw_facterareassociationtracenumber`.modifiedby,
  `ecw_facterareassociationtracenumber`.modifieddtm,
  `ecw_facterareassociationtracenumber`.deletedflag,
  `ecw_facterareassociationtracenumber`.fullclaimnumber,
  `ecw_facterareassociationtracenumber`.regionkey
  FROM
    edwpsc.`ecw_facterareassociationtracenumber`
;