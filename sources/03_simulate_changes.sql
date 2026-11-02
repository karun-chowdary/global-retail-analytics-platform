-- Run in SSMS AFTER the first mirroring sync works.
-- Simulates customers moving to a new city so your Gold DIM_CUSTOMER (SCD Type 2) has history.
-- Run it several times with different values. Each run changes @n random customers.

USE RetailSource;
GO

DECLARE @n     INT          = 50;
DECLARE @city  NVARCHAR(100) = N'sao paulo';
DECLARE @state CHAR(2)       = 'SP';

;WITH picked AS (
    SELECT TOP (@n) customer_id
    FROM dbo.customers
    WHERE customer_state <> @state
    ORDER BY NEWID()
)
UPDATE c
SET    c.customer_city  = @city,
       c.customer_state = @state,
       c.updated_at     = SYSUTCDATETIME()
FROM   dbo.customers AS c
JOIN   picked AS p ON p.customer_id = c.customer_id;

SELECT @@ROWCOUNT AS customers_changed;
GO

-- Suggested runs: (N'sao paulo','SP'), (N'rio de janeiro','RJ'), (N'belo horizonte','MG'), (N'curitiba','PR')
