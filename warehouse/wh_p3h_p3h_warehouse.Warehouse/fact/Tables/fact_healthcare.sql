CREATE TABLE [fact].[fact_healthcare] (

	[fact_healthcare_sk] bigint NULL, 
	[claim_id] varchar(8000) NULL, 
	[encounter_id] varchar(8000) NULL, 
	[payment_id] varchar(8000) NULL, 
	[member_sk] bigint NULL, 
	[provider_sk] bigint NULL, 
	[plan_sk] bigint NULL, 
	[diagnosis_sk] bigint NULL, 
	[procedure_sk] bigint NULL, 
	[date_key] int NULL, 
	[line_number] int NULL, 
	[amount] float NULL, 
	[transaction_type] varchar(8000) NULL, 
	[load_ts] datetime2(6) NULL
);