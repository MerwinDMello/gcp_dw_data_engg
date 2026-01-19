CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refmodifierdetails`
AS SELECT
  `ecw_refmodifierdetails`.modifierdetailkey,
  `ecw_refmodifierdetails`.modifiercode,
  `ecw_refmodifierdetails`.modifierdescription,
  `ecw_refmodifierdetails`.anesthesiapercentage,
  `ecw_refmodifierdetails`.anesthesiasuppressflag,
  `ecw_refmodifierdetails`.anesthesiaphysicalstatusunits,
  `ecw_refmodifierdetails`.sourcesystemcode,
  `ecw_refmodifierdetails`.sourceprimarykeyvalue,
  `ecw_refmodifierdetails`.dwlastupdatedatetime,
  `ecw_refmodifierdetails`.insertedby,
  `ecw_refmodifierdetails`.inserteddtm,
  `ecw_refmodifierdetails`.modifiedby,
  `ecw_refmodifierdetails`.modifieddtm
  FROM
    edwpsc.`ecw_refmodifierdetails`
;