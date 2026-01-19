CREATE OR REPLACE VIEW edwpsc_base_views.`cmt_factbatchdata`
AS SELECT
  `cmt_factbatchdata`.batchdatakey,
  `cmt_factbatchdata`.batchid,
  `cmt_factbatchdata`.userid,
  `cmt_factbatchdata`.updatedate,
  `cmt_factbatchdata`.historyreasonid,
  `cmt_factbatchdata`.historyreasoncode,
  `cmt_factbatchdata`.batchstate,
  `cmt_factbatchdata`.batchstatename,
  `cmt_factbatchdata`.sourceaprimarykeyvalue,
  `cmt_factbatchdata`.insertedby,
  `cmt_factbatchdata`.inserteddtm,
  `cmt_factbatchdata`.modifiedby,
  `cmt_factbatchdata`.modifieddtm,
  `cmt_factbatchdata`.payername,
  `cmt_factbatchdata`.checknumber,
  `cmt_factbatchdata`.dwlastupdatedatetime
  FROM
    edwpsc.`cmt_factbatchdata`
;