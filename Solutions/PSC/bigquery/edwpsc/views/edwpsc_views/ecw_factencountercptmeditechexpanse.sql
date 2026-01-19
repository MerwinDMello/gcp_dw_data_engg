CREATE OR REPLACE VIEW edwpsc_views.`ecw_factencountercptmeditechexpanse`
AS SELECT
  `ecw_factencountercptmeditechexpanse`.encountercptmtxkey,
  `ecw_factencountercptmeditechexpanse`.regionkey,
  `ecw_factencountercptmeditechexpanse`.encounterkey,
  `ecw_factencountercptmeditechexpanse`.cptcodekey,
  `ecw_factencountercptmeditechexpanse`.cptcode,
  `ecw_factencountercptmeditechexpanse`.cptunits,
  `ecw_factencountercptmeditechexpanse`.cptorder,
  `ecw_factencountercptmeditechexpanse`.cptmod1,
  `ecw_factencountercptmeditechexpanse`.visitdate,
  `ecw_factencountercptmeditechexpanse`.deleteflag,
  `ecw_factencountercptmeditechexpanse`.chargecode,
  `ecw_factencountercptmeditechexpanse`.sourcelastupdateddate,
  `ecw_factencountercptmeditechexpanse`.dwlastupdatedatetime,
  `ecw_factencountercptmeditechexpanse`.sourcesystemcode,
  `ecw_factencountercptmeditechexpanse`.insertedby,
  `ecw_factencountercptmeditechexpanse`.inserteddtm,
  `ecw_factencountercptmeditechexpanse`.modifiedby,
  `ecw_factencountercptmeditechexpanse`.modifieddtm,
  `ecw_factencountercptmeditechexpanse`.cpttransnum
  FROM
    edwpsc_base_views.`ecw_factencountercptmeditechexpanse`
;