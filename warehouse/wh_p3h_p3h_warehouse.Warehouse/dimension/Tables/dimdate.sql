CREATE TABLE [dimension].[dimdate] (

	[datekey] bigint NULL, 
	[Date] date NULL, 
	[year] bigint NULL, 
	[quarter] bigint NULL, 
	[month] bigint NULL, 
	[monthname] varchar(8000) NULL, 
	[weekofyear] bigint NULL, 
	[dayofweek] varchar(8000) NULL, 
	[dayofweeknumber] bigint NULL
);