CREATE OR REPLACE VIEW edwpsc_views.`ecw_refsrtgroupvaluecontrolnumber`
AS SELECT
  `ecw_refsrtgroupvaluecontrolnumber`.groupvaluecontrolnumberkey,
  `ecw_refsrtgroupvaluecontrolnumber`.primarygroupvalue,
  `ecw_refsrtgroupvaluecontrolnumber`.secondarygroupvalue,
  `ecw_refsrtgroupvaluecontrolnumber`.control1,
  `ecw_refsrtgroupvaluecontrolnumber`.control2,
  `ecw_refsrtgroupvaluecontrolnumber`.sourceaprimarykeyvalue,
  `ecw_refsrtgroupvaluecontrolnumber`.deleteflag,
  `ecw_refsrtgroupvaluecontrolnumber`.dwlastupdatedatetime,
  `ecw_refsrtgroupvaluecontrolnumber`.sourcesystemcode,
  `ecw_refsrtgroupvaluecontrolnumber`.insertedby,
  `ecw_refsrtgroupvaluecontrolnumber`.inserteddtm,
  `ecw_refsrtgroupvaluecontrolnumber`.modifiedby,
  `ecw_refsrtgroupvaluecontrolnumber`.modifieddtm,
  `ecw_refsrtgroupvaluecontrolnumber`.primarygroupsourcesystem,
  `ecw_refsrtgroupvaluecontrolnumber`.secondarygroupsourcesystem
  FROM
    edwpsc_base_views.`ecw_refsrtgroupvaluecontrolnumber`
;