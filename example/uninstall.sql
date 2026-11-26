-- SSDF Example Uninstaller
-- Removes the demo DWH and the example flow registrations.
-- Run BEFORE framework/uninstall.sql:  psql -d demo -f uninstall.sql

\echo Removing example flow points...
DELETE FROM df.flow WHERE process IN ('Collection', 'Main');

\echo Removing example horizons...
DELETE FROM df.horizon WHERE name IN ('Collection', 'Main_Payment');

\echo Dropping schema dwh...
DROP SCHEMA IF EXISTS dwh CASCADE;

\echo SSDF example uninstalled successfully.
