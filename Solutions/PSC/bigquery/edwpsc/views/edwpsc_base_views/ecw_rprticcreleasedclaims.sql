CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_rprticcreleasedclaims`
AS SELECT
  `ecw_rprticcreleasedclaims`.snapshotdate,
  `ecw_rprticcreleasedclaims`.coid,
  `ecw_rprticcreleasedclaims`.claimnumber,
  `ecw_rprticcreleasedclaims`.rhclaimhistorysubmissiondatekey,
  `ecw_rprticcreleasedclaims`.rhclaimhistorybridgefilenumber,
  `ecw_rprticcreleasedclaims`.rhclaimhistorypatientcontrolnbr,
  `ecw_rprticcreleasedclaims`.rhclaimhistoryholdcode,
  `ecw_rprticcreleasedclaims`.rhclaimhistoryreleaseddatekey,
  `ecw_rprticcreleasedclaims`.rhclaimhistorytotalclaimamount,
  `ecw_rprticcreleasedclaims`.insertedby,
  `ecw_rprticcreleasedclaims`.inserteddtm,
  `ecw_rprticcreleasedclaims`.modifiedby,
  `ecw_rprticcreleasedclaims`.modifieddtm,
  `ecw_rprticcreleasedclaims`.claimkey,
  `ecw_rprticcreleasedclaims`.regionkey,
  `ecw_rprticcreleasedclaims`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_rprticcreleasedclaims`
;