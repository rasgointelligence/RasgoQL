import rasgoql

creds = rasgoql.BigQueryCredentials.from_env()
print(creds)

rql = rasgoql.connect(creds)
print('Connected!')

tables = rql.list_tables()
print(tables.head())

results = rql.query("Select 'hello cupcake!'")
print(results)

results = rql.query_into_df('SELECT * FROM rasgodb.public.weekly_weather')
print(results.head())

ddl = rql._dw.get_ddl('rasgodb.public.weekly_weather')
print(ddl)

details = rql._dw.get_object_details('rasgodb.public.weekly_weather')
print(details)
