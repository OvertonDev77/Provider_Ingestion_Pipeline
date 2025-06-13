-- rehabs.make_your_own.sql
-- Schema for storing NPI-registered rehab provider information
-- npi_number is the primary key and must be unique

CREATE TABLE IF NOT EXISTS "NPIRehabs" (
    npi_number VARCHAR(20) PRIMARY KEY, -- Unique NPI identifier, required
    organization_name TEXT NOT NULL,    -- Name of the organization, required
    address TEXT,                       -- Mailing address
    city VARCHAR(100),                  -- City
    state VARCHAR(20),                  -- State
    postal_code VARCHAR(20),            -- Postal code
    phone VARCHAR(20),                  -- Phone number
    taxonomy_code VARCHAR(20),          -- Taxonomy code
    last_updated DATE,                  -- Last update date
    CONSTRAINT NPIRehabs_npi_number_key UNIQUE (npi_number)
);

-- Optional: Add a comment for documentation
COMMENT ON TABLE "NPIRehabs" IS 'Stores NPI-registered rehab provider information. npi_number is the primary key and must be unique.';
