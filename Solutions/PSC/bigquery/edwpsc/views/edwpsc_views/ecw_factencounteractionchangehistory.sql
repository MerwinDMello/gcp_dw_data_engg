CREATE OR REPLACE VIEW edwpsc_views.`ecw_factencounteractionchangehistory`
AS SELECT
  `ecw_factencounteractionchangehistory`.id,
  `ecw_factencounteractionchangehistory`.encid,
  `ecw_factencounteractionchangehistory`.logdetails,
  `ecw_factencounteractionchangehistory`.actiontype,
  `ecw_factencounteractionchangehistory`.userid,
  `ecw_factencounteractionchangehistory`.modifydate,
  `ecw_factencounteractionchangehistory`.modifiedcolumns,
  `ecw_factencounteractionchangehistory`.username,
  `ecw_factencounteractionchangehistory`.regionkey,
  `ecw_factencounteractionchangehistory`.loadkey,
  `ecw_factencounteractionchangehistory`.hashnomatch,
  `ecw_factencounteractionchangehistory`.insertedby,
  `ecw_factencounteractionchangehistory`.inserteddtm,
  `ecw_factencounteractionchangehistory`.modifiedby,
  `ecw_factencounteractionchangehistory`.modifieddtm,
  `ecw_factencounteractionchangehistory`.lastchangedby,
  `ecw_factencounteractionchangehistory`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_factencounteractionchangehistory`
;