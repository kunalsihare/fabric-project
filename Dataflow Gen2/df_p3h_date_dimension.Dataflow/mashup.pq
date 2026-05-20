[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared pStartDate = #date(2015, 1, 1) meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type date];
shared pEndDate = #date(2035, 12, 31) meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type date];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DateQuery_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "datekey", DestinationColumnName = "datekey"], [SourceColumnName = "Date", DestinationColumnName = "Date"], [SourceColumnName = "year", DestinationColumnName = "year"], [SourceColumnName = "quarter", DestinationColumnName = "quarter"], [SourceColumnName = "month", DestinationColumnName = "month"], [SourceColumnName = "monthname", DestinationColumnName = "monthname"], [SourceColumnName = "weekofyear", DestinationColumnName = "weekofyear"], [SourceColumnName = "dayofweek", DestinationColumnName = "dayofweek"], [SourceColumnName = "dayofweeknumber", DestinationColumnName = "dayofweeknumber"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DateQuery = let
    StartDate = pStartDate,
    EndDate = pEndDate,

    Dates =
        List.Dates(
            StartDate,
            Duration.Days(EndDate - StartDate) + 1,
            #duration(1,0,0,0)
        ),

    DateTable =
        Table.FromList(
            Dates,
            Splitter.SplitByNothing(),
            {"Date"}
        ),
  #"Changed column type" = Table.TransformColumnTypes(DateTable, {{"Date", type date}}),
  #"Get Year From date" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "year", each Date.Year(Date.From([Date]))), {{"year", Int64.Type}}),
  #"Get Quarter From Date" = Table.TransformColumnTypes(Table.AddColumn(#"Get Year From date", "quarter", each Date.QuarterOfYear(Date.From([Date]))), {{"quarter", Int64.Type}}),
  #"Generate DateKey from Date" = Table.TransformColumnTypes(Table.AddColumn(#"Get Quarter From Date", "datekey", each Number.FromText(Date.ToText(Date.From([Date]), "yyyyMMdd"))), {{"datekey", Int64.Type}}),
  #"Get MonthName From Date" = Table.TransformColumnTypes(Table.AddColumn(#"Generate DateKey from Date", "month", each Date.Month(Date.From([Date]))), {{"month", Int64.Type}}),
  #"Get Month Name from Date" = Table.TransformColumnTypes(Table.AddColumn(#"Get MonthName From Date", "monthname", each Date.ToText(Date.From([Date]), "MMMM")), {{"monthname", type text}}),
  #"Get Week Number from Date" = Table.TransformColumnTypes(Table.AddColumn(#"Get Month Name from Date", "weekofyear", each Date.WeekOfYear(Date.From([Date]))), {{"weekofyear", Int64.Type}}),
  #"Get day of week from date" = Table.TransformColumnTypes(Table.AddColumn(#"Get Week Number from Date", "dayofweek", each Date.DayOfWeekName(Date.From([Date]))), {{"dayofweek", type text}}),
  #"Reordered columns" = Table.ReorderColumns(#"Get day of week from date", {"datekey", "Date", "year", "quarter", "month", "monthname", "weekofyear", "dayofweek"}),
  #"Get day of week number" = Table.TransformColumnTypes(Table.AddColumn(#"Reordered columns", "dayofweeknumber", each Date.DayOfWeek(Date.From([Date])) + 1), {{"dayofweeknumber", Int64.Type}})
in
    #"Get day of week number";
shared DateQuery_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[displayName = "wh_p3h_p3h_warehouse"]}[Data],
  Navigation_3 = Navigation_2{[Schema = "dimension"]}[Data],
  TableNavigation = Navigation_3{[Name = "dimdate"]}?[Data]?
in
  TableNavigation;
