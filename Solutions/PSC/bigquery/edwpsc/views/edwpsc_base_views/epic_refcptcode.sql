CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refcptcode`
AS SELECT
  `epic_refcptcode`.cptcodekey,
  `epic_refcptcode`.cptcode,
  `epic_refcptcode`.cptname,
  `epic_refcptcode`.deleteflag,
  `epic_refcptcode`.dwlastupdatedatetime,
  `epic_refcptcode`.sourcesystemcode,
  `epic_refcptcode`.insertedby,
  `epic_refcptcode`.inserteddtm,
  `epic_refcptcode`.modifiedby,
  `epic_refcptcode`.modifieddtm,
  `epic_refcptcode`.category1code,
  `epic_refcptcode`.category2code
  FROM
    edwpsc.`epic_refcptcode`
;