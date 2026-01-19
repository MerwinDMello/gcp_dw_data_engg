CREATE OR REPLACE VIEW edwpsc_views.`ecw_refiplangroupcactus`
AS SELECT
  `ecw_refiplangroupcactus`.iplangroupcactuskey,
  `ecw_refiplangroupcactus`.iplangroupcactusname,
  `ecw_refiplangroupcactus`.sourceprimarykeyvalue,
  `ecw_refiplangroupcactus`.sourcerecordlastupdated,
  `ecw_refiplangroupcactus`.dwlastupdatedatetime,
  `ecw_refiplangroupcactus`.sourcesystemcode,
  `ecw_refiplangroupcactus`.insertedby,
  `ecw_refiplangroupcactus`.inserteddtm,
  `ecw_refiplangroupcactus`.modifiedby,
  `ecw_refiplangroupcactus`.modifieddtm,
  `ecw_refiplangroupcactus`.deleteflag
  FROM
    edwpsc_base_views.`ecw_refiplangroupcactus`
;