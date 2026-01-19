CREATE OR REPLACE VIEW edwpsc_views.`epic_refprocedurecode`
AS SELECT
  `epic_refprocedurecode`.procedurecodekey,
  `epic_refprocedurecode`.procedurecode,
  `epic_refprocedurecode`.procedurecodename,
  `epic_refprocedurecode`.procedurecodedescription,
  `epic_refprocedurecode`.procedurecodeshortname,
  `epic_refprocedurecode`.procedurecodeactive,
  `epic_refprocedurecode`.procid,
  `epic_refprocedurecode`.regionkey,
  `epic_refprocedurecode`.sourceaprimarykey,
  `epic_refprocedurecode`.dwlastupdatedatetime,
  `epic_refprocedurecode`.sourcesystemcode,
  `epic_refprocedurecode`.insertedby,
  `epic_refprocedurecode`.inserteddtm,
  `epic_refprocedurecode`.modifiedby,
  `epic_refprocedurecode`.modifieddtm
  FROM
    edwpsc_base_views.`epic_refprocedurecode`
;