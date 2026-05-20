-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "240a07fd-ac04-8ba2-4596-6d5296d96d65",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "240a07fd-ac04-8ba2-4596-6d5296d96d65",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE OR ALTER FUNCTION security.tvf_securitypredicate_Procedure(@procedure_code AS VARCHAR(50))
    RETURNS TABLE
WITH SCHEMABINDING
AS
    RETURN SELECT 1 AS tvf_securitypredicate_result
    WHERE EXISTS (
        SELECT 1 
        FROM security.UserSecurityMapping
        WHERE user_name = USER_NAME() 
          AND scope_value = @procedure_code
          AND scope = 'PROC'
    );
GO

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- Drop the old policy if it exists
IF EXISTS (SELECT * FROM sys.security_policies WHERE name = 'ProcedureFilter')
    DROP SECURITY POLICY ProcedureFilter;
GO

CREATE SECURITY POLICY ProcedureFilter
ADD FILTER PREDICATE security.tvf_securitypredicate_Procedure(procedure_code)
ON dimension.dim_procedure
WITH (STATE = ON);
GO

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
