CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refsrtgroupvaluedepartment`
AS SELECT
  `ecw_refsrtgroupvaluedepartment`.groupvaluedeptkey,
  `ecw_refsrtgroupvaluedepartment`.lob,
  `ecw_refsrtgroupvaluedepartment`.groupvalue,
  `ecw_refsrtgroupvaluedepartment`.department,
  `ecw_refsrtgroupvaluedepartment`.sourceaprimarykeyvalue,
  `ecw_refsrtgroupvaluedepartment`.deleteflag,
  `ecw_refsrtgroupvaluedepartment`.dwlastupdatedatetime,
  `ecw_refsrtgroupvaluedepartment`.sourcesystemcode,
  `ecw_refsrtgroupvaluedepartment`.insertedby,
  `ecw_refsrtgroupvaluedepartment`.inserteddtm,
  `ecw_refsrtgroupvaluedepartment`.modifiedby,
  `ecw_refsrtgroupvaluedepartment`.modifieddtm
  FROM
    edwpsc.`ecw_refsrtgroupvaluedepartment`
;