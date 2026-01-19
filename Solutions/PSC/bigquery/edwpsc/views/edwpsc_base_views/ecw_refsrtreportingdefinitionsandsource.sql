CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refsrtreportingdefinitionsandsource`
AS SELECT
  `ecw_refsrtreportingdefinitionsandsource`.srtreportingdefinitionsandsoucekey,
  `ecw_refsrtreportingdefinitionsandsource`.metric,
  `ecw_refsrtreportingdefinitionsandsource`.metricdefinition,
  `ecw_refsrtreportingdefinitionsandsource`.criteria,
  `ecw_refsrtreportingdefinitionsandsource`.report,
  `ecw_refsrtreportingdefinitionsandsource`.sort,
  `ecw_refsrtreportingdefinitionsandsource`.sourceaprimarykeyvalue,
  `ecw_refsrtreportingdefinitionsandsource`.deleteflag,
  `ecw_refsrtreportingdefinitionsandsource`.dwlastupdatedatetime,
  `ecw_refsrtreportingdefinitionsandsource`.sourcesystemcode,
  `ecw_refsrtreportingdefinitionsandsource`.insertedby,
  `ecw_refsrtreportingdefinitionsandsource`.inserteddtm,
  `ecw_refsrtreportingdefinitionsandsource`.modifiedby,
  `ecw_refsrtreportingdefinitionsandsource`.modifieddtm
  FROM
    edwpsc.`ecw_refsrtreportingdefinitionsandsource`
;