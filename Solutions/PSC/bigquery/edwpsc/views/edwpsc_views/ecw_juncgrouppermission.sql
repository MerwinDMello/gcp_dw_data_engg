CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncgrouppermission`
AS SELECT
  `ecw_juncgrouppermission`.ecwgrouppermissionkey,
  `ecw_juncgrouppermission`.regionkey,
  `ecw_juncgrouppermission`.userid,
  `ecw_juncgrouppermission`.userkey,
  `ecw_juncgrouppermission`.groupid,
  `ecw_juncgrouppermission`.permission,
  `ecw_juncgrouppermission`.deleteflag,
  `ecw_juncgrouppermission`.sourceprimarykeyvalue,
  `ecw_juncgrouppermission`.dwlastupdatedatetime,
  `ecw_juncgrouppermission`.sourcesystemcode,
  `ecw_juncgrouppermission`.insertedby,
  `ecw_juncgrouppermission`.inserteddtm,
  `ecw_juncgrouppermission`.modifiedby,
  `ecw_juncgrouppermission`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_juncgrouppermission`
;