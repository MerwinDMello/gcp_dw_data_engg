CREATE OR REPLACE VIEW edwpsc_base_views.`ccu_rprtvendorcoidmatrix`
AS SELECT
  `ccu_rprtvendorcoidmatrix`.vendorcoidmatrixkey,
  `ccu_rprtvendorcoidmatrix`.coid,
  `ccu_rprtvendorcoidmatrix`.coidname,
  `ccu_rprtvendorcoidmatrix`.active,
  `ccu_rprtvendorcoidmatrix`.systemvalue,
  `ccu_rprtvendorcoidmatrix`.vendorvalue,
  `ccu_rprtvendorcoidmatrix`.assigneddate,
  `ccu_rprtvendorcoidmatrix`.dwlastupdatedatetime,
  `ccu_rprtvendorcoidmatrix`.insertedby,
  `ccu_rprtvendorcoidmatrix`.inserteddtm,
  `ccu_rprtvendorcoidmatrix`.modifiedby,
  `ccu_rprtvendorcoidmatrix`.modifieddtm
  FROM
    edwpsc.`ccu_rprtvendorcoidmatrix`
;