CREATE TABLE [security].[UserSecurityMapping] (

	[ID] bigint IDENTITY NOT NULL, 
	[user_name] varchar(50) NULL, 
	[scope_value] varchar(50) NULL, 
	[scope] varchar(50) NULL
);