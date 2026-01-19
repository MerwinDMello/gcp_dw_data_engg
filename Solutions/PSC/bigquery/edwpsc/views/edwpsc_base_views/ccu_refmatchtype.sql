CREATE OR REPLACE VIEW edwpsc_base_views.`ccu_refmatchtype`
AS SELECT
  `ccu_refmatchtype`.matchtypekey,
  `ccu_refmatchtype`.matchtype,
  `ccu_refmatchtype`.matchflag,
  `ccu_refmatchtype`.matchtypedesc,
  `ccu_refmatchtype`.matchtypelongdesc
  FROM
    edwpsc.`ccu_refmatchtype`
;