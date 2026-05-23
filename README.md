pip install psycopg2-binary

export DATABASE_URL='postgresql://neondb_owner:**********@ep.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require'

python load_leads.py --csv input.csv
# crash? just re-run the same command — it resumes from the last commit
