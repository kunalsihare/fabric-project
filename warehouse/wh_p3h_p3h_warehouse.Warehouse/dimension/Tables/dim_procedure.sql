CREATE TABLE [dimension].[dim_procedure] (

	[procedure_sk] bigint NULL, 
	[procedure_code] varchar(8000) NULL, 
	[procedure_desc] varchar(8000) NULL, 
	[category] varchar(8000) NULL, 
	[cost_category] varchar(8000) NULL, 
	[load_ts] datetime2(6) NULL
);