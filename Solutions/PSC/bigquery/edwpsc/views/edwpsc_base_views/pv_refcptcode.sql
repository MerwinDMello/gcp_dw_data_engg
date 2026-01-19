CREATE OR REPLACE VIEW edwpsc_base_views.`pv_refcptcode`
AS SELECT
  `pv_refcptcode`.cptcodekey,
  `pv_refcptcode`.cptcode,
  `pv_refcptcode`.cptname,
  `pv_refcptcode`.sourceaprimarykeyvalue,
  `pv_refcptcode`.sourcearecordlastupdated,
  `pv_refcptcode`.sourcebprimarykeyvalue,
  `pv_refcptcode`.sourcebrecordlastupdated,
  `pv_refcptcode`.dwlastupdatedatetime,
  `pv_refcptcode`.sourcesystemcode,
  `pv_refcptcode`.insertedby,
  `pv_refcptcode`.inserteddtm,
  `pv_refcptcode`.modifiedby,
  `pv_refcptcode`.modifieddtm,
  `pv_refcptcode`.deleteflag,
  `pv_refcptcode`.category1code,
  `pv_refcptcode`.category2code
  FROM
    edwpsc.`pv_refcptcode`
;