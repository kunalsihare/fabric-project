CREATE TABLE [dimension].[dim_member] (

	[member_sk] bigint NULL, 
	[member_id] varchar(8000) NULL, 
	[member_number] bigint NULL, 
	[first_name] varchar(8000) NULL, 
	[last_name] varchar(8000) NULL, 
	[gender] varchar(8000) NULL, 
	[dob] date NULL, 
	[city] varchar(8000) NULL, 
	[state] varchar(8000) NULL, 
	[plan_id] varchar(8000) NULL, 
	[enrollment_status] varchar(8000) NULL, 
	[effective_date] date NULL, 
	[termination_date] date NULL, 
	[load_ts] datetime2(6) NULL
);