CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refpaymentsource`
AS SELECT
  `epic_refpaymentsource`.paymentsourcekey,
  `epic_refpaymentsource`.paymentsourcename,
  `epic_refpaymentsource`.paymentsourcetitle,
  `epic_refpaymentsource`.paymentsourceabbr,
  `epic_refpaymentsource`.paymentsourceinternalid,
  `epic_refpaymentsource`.paymentsourcec,
  `epic_refpaymentsource`.regionkey,
  `epic_refpaymentsource`.sourceaprimarykey,
  `epic_refpaymentsource`.dwlastupdatedatetime,
  `epic_refpaymentsource`.sourcesystemcode,
  `epic_refpaymentsource`.insertedby,
  `epic_refpaymentsource`.inserteddtm,
  `epic_refpaymentsource`.modifiedby,
  `epic_refpaymentsource`.modifieddtm
  FROM
    edwpsc.`epic_refpaymentsource`
;