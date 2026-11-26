-- SSDF Example Uninstaller
-- Removes the demo DWH and the example flow registrations.
-- Run BEFORE framework/uninstall.sql:  psql -d demo -f uninstall.sql

\echo Removing example flow points...
DELETE FROM ssdf.flow WHERE process IN ('Collection', 'Main');

\echo Removing example horizons...
DELETE FROM ssdf.horizon WHERE name IN ('Collection', 'Main_Payment');

\echo Dropping schema smpl_dwh...
DROP SCHEMA IF EXISTS smpl_dwh CASCADE;

\echo SSDF example uninstalled successfully.
