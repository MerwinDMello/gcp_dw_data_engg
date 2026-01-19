CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.offer
(
  offer_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each offer.")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically.")
, valid_to_date DATETIME OPTIONS(description="Date on which the record was invalidated.")
, offer_num NUMERIC OPTIONS(description="A unique number from the source for each offer.")
, submission_sid INT64 OPTIONS(description="A unique identifier for each application.")
, sequence_num INT64 OPTIONS(description="If multiple records are created in Lawson for the  same effective date field will be incremented for each  record for the date.")
, offer_name STRING OPTIONS(description="Describes the name of the offer and whether it is current or not.")
, accept_date DATE OPTIONS(description="The date the offer was accepted on.")
, start_date DATE OPTIONS(description="The proposed start date listed in the offer.")
, extend_date DATE OPTIONS(description="Date the offer was extended to the candidate.")
, last_modified_date DATE OPTIONS(description="Date the offer was last modified.")
, last_modified_time TIME OPTIONS(description="Time the offer was last modified.")
, capture_date DATE OPTIONS(description="The date the offer was first captured.")
, salary_amt BIGNUMERIC(53, 15) OPTIONS(description="The yearly salary amount listed in the offer to the candidate.")
, sign_on_bonus_amt BIGNUMERIC(53, 15) OPTIONS(description="The sign on bonus included in the offer.")
, salary_pay_basis_amt BIGNUMERIC(53, 15) OPTIONS(description="This is the salary broken down by pay period basis.")
, hours_per_day_num NUMERIC OPTIONS(description="The typical hours per day for the position the offer is for.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
 CLUSTER BY offer_sid
OPTIONS(description="Contains details associated with offer extended to a candidate based on their application.");
