CREATE TABLE IF NOT EXISTS ssdf.setting (
    settng varchar(100) PRIMARY KEY,
    value text
);

-- Insert default database name setting
INSERT INTO ssdf.setting (settng, value) VALUES ('default_dbname', 'demo') ON CONFLICT DO NOTHING;