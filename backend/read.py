import pandas as pd
from sqlalchemy import create_engine

user = "postgres.kimsluaqkkipyzpnfpxl"
password = "CxzxdPqFKciVppqT"
host = "aws-1-ap-southeast-2.pooler.supabase.com"
port = "5432"
database = "CBS Central"
table_name = "insale"

try:
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/postgres",
        pool_pre_ping=True
    )

    with engine.connect() as conn:
        print("Supabase Engine Connected Successfully")

except Exception as e:
    print(f"Error is this: {e}")


df = pd.read_sql("SELECT * FROM insale", engine)

print(df.head())