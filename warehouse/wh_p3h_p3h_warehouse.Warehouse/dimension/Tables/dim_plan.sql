CREATE TABLE [dimension].[dim_plan] (

	[plan_sk] bigint NULL, 
	[plan_id] varchar(8000) NULL, 
	[plan_name] varchar(8000) NULL, 
	[plan_type] varchar(8000) NULL, 
	[coverage_type] varchar(8000) NULL, 
	[effective_date] date NULL, 
	[termination_date] date NULL, 
	[load_ts] datetime2(6) NULL
);