CREATE OR REPLACE VIEW edwpsc_views.`ecw_refsrtuccfastmedindicator`
AS SELECT
  `ecw_refsrtuccfastmedindicator`.uccfastmedindicatorkey,
  `ecw_refsrtuccfastmedindicator`.id,
  `ecw_refsrtuccfastmedindicator`.practice,
  `ecw_refsrtuccfastmedindicator`.clinic,
  `ecw_refsrtuccfastmedindicator`.fastmedindicator,
  `ecw_refsrtuccfastmedindicator`.effectivedate,
  `ecw_refsrtuccfastmedindicator`.termeddate,
  `ecw_refsrtuccfastmedindicator`.sourceaprimarykeyvalue,
  `ecw_refsrtuccfastmedindicator`.deleteflag,
  `ecw_refsrtuccfastmedindicator`.dwlastupdatedatetime,
  `ecw_refsrtuccfastmedindicator`.sourcesystemcode,
  `ecw_refsrtuccfastmedindicator`.insertedby,
  `ecw_refsrtuccfastmedindicator`.inserteddtm,
  `ecw_refsrtuccfastmedindicator`.modifiedby,
  `ecw_refsrtuccfastmedindicator`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refsrtuccfastmedindicator`
;