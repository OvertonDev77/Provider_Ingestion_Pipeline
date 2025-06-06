from npi_ingestion.config import get_supabase_client

def create_indexes():
    supabase = get_supabase_client()
    sql_statements = [
        'CREATE INDEX IF NOT EXISTS idx_npi_number ON "NPIRehabs" (npi_number);',
        'CREATE INDEX IF NOT EXISTS idx_organization_name ON "NPIRehabs" (organization_name);',
        'CREATE INDEX IF NOT EXISTS idx_state ON "NPIRehabs" (state);'
    ]
    for sql in sql_statements:
        response = supabase.rpc("execute_sql", {"sql": sql}).execute()
        print(f"Executed: {sql}\nResponse: {response}")

if __name__ == "__main__":
    create_indexes() 