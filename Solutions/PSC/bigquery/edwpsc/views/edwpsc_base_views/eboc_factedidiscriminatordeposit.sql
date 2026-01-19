CREATE OR REPLACE VIEW edwpsc_base_views.`eboc_factedidiscriminatordeposit`
AS SELECT
  `eboc_factedidiscriminatordeposit`.ebocedidiscriminatordepositkey,
  `eboc_factedidiscriminatordeposit`.depositid,
  `eboc_factedidiscriminatordeposit`.treasurybatchnumber,
  `eboc_factedidiscriminatordeposit`.creatorloginname,
  `eboc_factedidiscriminatordeposit`.creatorname,
  `eboc_factedidiscriminatordeposit`.sourceprimarykeyvalue,
  `eboc_factedidiscriminatordeposit`.dwlastupdatedatetime,
  `eboc_factedidiscriminatordeposit`.sourcesystemcode,
  `eboc_factedidiscriminatordeposit`.insertedby,
  `eboc_factedidiscriminatordeposit`.inserteddtm,
  `eboc_factedidiscriminatordeposit`.modifiedby,
  `eboc_factedidiscriminatordeposit`.modifieddtm,
  `eboc_factedidiscriminatordeposit`.createddatetime
  FROM
    edwpsc.`eboc_factedidiscriminatordeposit`
;