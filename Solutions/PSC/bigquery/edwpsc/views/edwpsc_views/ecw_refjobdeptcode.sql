CREATE OR REPLACE VIEW edwpsc_views.`ecw_refjobdeptcode`
AS SELECT
  `ecw_refjobdeptcode`.deptcode,
  `ecw_refjobdeptcode`.deptcodename
  FROM
    edwpsc_base_views.`ecw_refjobdeptcode`
;