CREATE TABLE [dimension].[dim_diagnosis] (

	[diagnosis_sk] bigint NULL, 
	[diagnosis_code] varchar(8000) NULL, 
	[diagnosis_desc] varchar(8000) NULL, 
	[category] varchar(8000) NULL, 
	[severity] varchar(8000) NULL, 
	[load_ts] datetime2(6) NULL
);