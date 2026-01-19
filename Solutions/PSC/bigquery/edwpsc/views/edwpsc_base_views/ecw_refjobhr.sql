CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refjobhr`
AS SELECT
  `ecw_refjobhr`.jobhrkey,
  `ecw_refjobhr`.jobcodehr,
  `ecw_refjobhr`.jobdescriptionhr,
  `ecw_refjobhr`.jobclasscodehr,
  `ecw_refjobhr`.jobclassdescriptionhr,
  `ecw_refjobhr`.dwlastupdatedatetime,
  `ecw_refjobhr`.sourcesystemcode,
  `ecw_refjobhr`.insertedby,
  `ecw_refjobhr`.inserteddtm,
  `ecw_refjobhr`.modifiedby,
  `ecw_refjobhr`.modifieddtm
  FROM
    edwpsc.`ecw_refjobhr`
;