-- Synthetic source deliberately has NO updated_at or deleted_at.
-- Real producer connects only as remote_reader, with SELECT privilege.
CREATE TABLE public.contracts (
 id BIGINT PRIMARY KEY,
 contract_code TEXT NOT NULL UNIQUE,
 amount NUMERIC(14,2) NOT NULL,
 status TEXT NOT NULL,
 description TEXT
);
INSERT INTO public.contracts(id,contract_code,amount,status,description)
SELECT g, 'CON-' || lpad(g::text,7,'0'), (g*17.31)::numeric(14,2),
 CASE WHEN g % 3=0 THEN 'closed' ELSE 'active' END, 'Contract ' || g
FROM generate_series(1,10000) g;
CREATE ROLE remote_reader LOGIN PASSWORD 'lab-reader';
GRANT CONNECT ON DATABASE lab_remote TO remote_reader;
GRANT USAGE ON SCHEMA public TO remote_reader;
GRANT SELECT ON public.contracts TO remote_reader;
-- Fixture administrator only: future lab tables are readable without granting writes.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO remote_reader;
ALTER ROLE remote_reader SET default_transaction_read_only = on;
