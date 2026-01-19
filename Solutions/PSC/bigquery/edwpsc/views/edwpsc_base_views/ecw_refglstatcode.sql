CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refglstatcode`
AS SELECT
  `ecw_refglstatcode`.glstatcodekey,
  `ecw_refglstatcode`.statcodedesc,
  `ecw_refglstatcode`.dwlastupdatedatetime,
  `ecw_refglstatcode`.sourcesystemcode,
  `ecw_refglstatcode`.insertedby,
  `ecw_refglstatcode`.inserteddtm,
  `ecw_refglstatcode`.modifiedby,
  `ecw_refglstatcode`.modifieddtm
  FROM
    edwpsc.`ecw_refglstatcode`
;