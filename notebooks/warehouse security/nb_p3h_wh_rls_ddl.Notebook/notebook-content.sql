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

Create schema security;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE TABLE security.UserSecurityMapping (
    ID BIGINT IDENTITY,
    user_name VARCHAR(50),
    scope_value VARCHAR(50),
    scope VARCHAR(50)
)

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
