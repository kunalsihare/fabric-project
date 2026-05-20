CREATE TABLE [dimension].[dim_provider] (

	[provider_sk] bigint NULL, 
	[provider_id] varchar(8000) NULL, 
	[npi] bigint NULL, 
	[provider_name] varchar(8000) NULL, 
	[specialty] varchar(8000) NULL, 
	[city] varchar(8000) NULL, 
	[state] varchar(8000) NULL, 
	[contract_type] varchar(8000) NULL, 
	[payment_model] varchar(8000) NULL, 
	[status] varchar(8000) NULL, 
	[load_ts] datetime2(6) NULL
);