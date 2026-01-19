CREATE OR REPLACE VIEW edwpsc_base_views.`pv_refvirtualvisit`
AS SELECT
  `pv_refvirtualvisit`.virtualvisittype,
  `pv_refvirtualvisit`.virtualvisitdesc,
  `pv_refvirtualvisit`.dwlastupdatedatetime,
  `pv_refvirtualvisit`.sourcesystemcode,
  `pv_refvirtualvisit`.insertedby,
  `pv_refvirtualvisit`.inserteddtm,
  `pv_refvirtualvisit`.modifiedby,
  `pv_refvirtualvisit`.modifieddtm
  FROM
    edwpsc.`pv_refvirtualvisit`
;