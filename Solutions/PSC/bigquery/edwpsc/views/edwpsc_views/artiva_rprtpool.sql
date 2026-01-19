CREATE OR REPLACE VIEW edwpsc_views.`artiva_rprtpool`
AS SELECT
  `artiva_rprtpool`.snapshotdate,
  `artiva_rprtpool`.claimkey,
  `artiva_rprtpool`.claimnumber,
  `artiva_rprtpool`.totalbalanceamt,
  `artiva_rprtpool`.artivaliabilitypool,
  `artiva_rprtpool`.poolname,
  `artiva_rprtpool`.artivaliabilitylastrevieweddate,
  `artiva_rprtpool`.artivaliabilitylastworkeddate,
  `artiva_rprtpool`.artivaliabilityfollowupdate,
  `artiva_rprtpool`.insertedby,
  `artiva_rprtpool`.inserteddtm,
  `artiva_rprtpool`.modifiedby,
  `artiva_rprtpool`.modifieddtm,
  `artiva_rprtpool`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`artiva_rprtpool`
;