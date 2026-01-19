CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_reftypeofservice`
AS SELECT
  `ecw_reftypeofservice`.toskey,
  `ecw_reftypeofservice`.toscode,
  `ecw_reftypeofservice`.tosname,
  `ecw_reftypeofservice`.sourceprimarykeyvalue,
  `ecw_reftypeofservice`.sourcerecordlastupdated,
  `ecw_reftypeofservice`.dwlastupdatedatetime,
  `ecw_reftypeofservice`.sourcesystemcode,
  `ecw_reftypeofservice`.insertedby,
  `ecw_reftypeofservice`.inserteddtm,
  `ecw_reftypeofservice`.modifiedby,
  `ecw_reftypeofservice`.modifieddtm,
  `ecw_reftypeofservice`.deleteflag,
  `ecw_reftypeofservice`.regionkey
  FROM
    edwpsc.`ecw_reftypeofservice`
;