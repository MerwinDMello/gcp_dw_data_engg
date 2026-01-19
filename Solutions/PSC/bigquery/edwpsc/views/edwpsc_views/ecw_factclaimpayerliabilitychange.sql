CREATE OR REPLACE VIEW edwpsc_views.`ecw_factclaimpayerliabilitychange`
AS SELECT
  `ecw_factclaimpayerliabilitychange`.claimpayerliabilitychangekey,
  `ecw_factclaimpayerliabilitychange`.regionid,
  `ecw_factclaimpayerliabilitychange`.claimnumber,
  `ecw_factclaimpayerliabilitychange`.billedtoid,
  `ecw_factclaimpayerliabilitychange`.billedtoidtype,
  `ecw_factclaimpayerliabilitychange`.transferhistorydate,
  `ecw_factclaimpayerliabilitychange`.transferhistorytime,
  `ecw_factclaimpayerliabilitychange`.userid,
  `ecw_factclaimpayerliabilitychange`.transferhistorymodifieddate,
  `ecw_factclaimpayerliabilitychange`.userkey,
  `ecw_factclaimpayerliabilitychange`.claimkey,
  `ecw_factclaimpayerliabilitychange`.encounterkey,
  `ecw_factclaimpayerliabilitychange`.encounterid,
  `ecw_factclaimpayerliabilitychange`.claimpayerkey,
  `ecw_factclaimpayerliabilitychange`.sourceaprimarykeyvalue,
  `ecw_factclaimpayerliabilitychange`.insertedby,
  `ecw_factclaimpayerliabilitychange`.inserteddtm,
  `ecw_factclaimpayerliabilitychange`.modifiedby,
  `ecw_factclaimpayerliabilitychange`.modifieddtm,
  `ecw_factclaimpayerliabilitychange`.dwlastupdatedatetime,
  `ecw_factclaimpayerliabilitychange`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_factclaimpayerliabilitychange`
;