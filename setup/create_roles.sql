-- Roles the schema DDL expects to exist. Run this FIRST, once per database.
--
-- setup/create_infor_schema.sql opens with
-- `CREATE SCHEMA infor AUTHORIZATION infor_user`, so the role must already
-- exist or the whole load fails on line 3.
--
-- No password is set here. Set one by hand afterwards and store it in your
-- secret manager -- never in this repository:
--
--   ALTER ROLE infor_user WITH PASSWORD '...';
--
-- To give reporting consumers read access, after the schema is loaded:
--
--   GRANT USAGE ON SCHEMA infor TO reporting_ro;
--   GRANT SELECT ON ALL TABLES IN SCHEMA infor TO reporting_ro;
--   ALTER DEFAULT PRIVILEGES FOR ROLE infor_user IN SCHEMA infor
--       GRANT SELECT ON TABLES TO reporting_ro;

DO $$
BEGIN
    -- Owns the infor schema; the sync connects as this role.
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'infor_user') THEN
        CREATE ROLE infor_user LOGIN;
    END IF;

    -- Optional read-only role for reporting consumers.
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'reporting_ro') THEN
        CREATE ROLE reporting_ro;
    END IF;
END
$$;
