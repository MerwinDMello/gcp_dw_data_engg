CREATE OR REPLACE VIEW edwpsc_views.`ecw_refmarket`
AS SELECT
  `ecw_refmarket`.marketkey,
  `ecw_refmarket`.marketname,
  `ecw_refmarket`.divisionkey,
  `ecw_refmarket`.sourceprimarykeyvalue,
  `ecw_refmarket`.sourcerecordlastupdated,
  `ecw_refmarket`.dwlastupdatedatetime,
  `ecw_refmarket`.sourcesystemcode,
  `ecw_refmarket`.insertedby,
  `ecw_refmarket`.inserteddtm,
  `ecw_refmarket`.modifiedby,
  `ecw_refmarket`.modifieddtm,
  `ecw_refmarket`.marketcode,
  `ecw_refmarket`.deleteflag,
  `ecw_refmarket`.coidstatflag,
  `ecw_refmarket`.ppmsflag,
  `ecw_refmarket`.sysstarttime,
  `ecw_refmarket`.sysendtime
  FROM
    edwpsc_base_views.`ecw_refmarket`
;