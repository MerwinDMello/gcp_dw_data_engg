CREATE OR REPLACE VIEW edwpsc_views.`ecw_refsrtgebbsexistingcoder`
AS SELECT
  `ecw_refsrtgebbsexistingcoder`.gebbsexistingcoderkey,
  `ecw_refsrtgebbsexistingcoder`.user34,
  `ecw_refsrtgebbsexistingcoder`.employeename,
  `ecw_refsrtgebbsexistingcoder`.userrole,
  `ecw_refsrtgebbsexistingcoder`.codertypeforcontractrate,
  `ecw_refsrtgebbsexistingcoder`.sourceaprimarykeyvalue,
  `ecw_refsrtgebbsexistingcoder`.deleteflag,
  `ecw_refsrtgebbsexistingcoder`.dwlastupdatedatetime,
  `ecw_refsrtgebbsexistingcoder`.sourcesystemcode,
  `ecw_refsrtgebbsexistingcoder`.insertedby,
  `ecw_refsrtgebbsexistingcoder`.inserteddtm,
  `ecw_refsrtgebbsexistingcoder`.modifiedby,
  `ecw_refsrtgebbsexistingcoder`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refsrtgebbsexistingcoder`
;