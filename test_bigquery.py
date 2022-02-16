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

ds = rql.dataset('rasgodb.public.weekly_weather')
print(ds)
print(ds.sql())
print(ds.preview().head())

ds2 = ds.cast(casts={"FIPS":"integer"})
print(ds2.sql())
print(ds2.preview().head())

ds3 = ds2.rolling_agg(
    aggregations={"WEEK_AVG_DAILY_HIGH_TEMP": ["MAX", "MIN", "SUM"]},
    order_by="DATE",
    offsets=[-7, 7],
    group_by=["FIPS"],
).drop_columns(exclude_cols=["WEEK_AVG_DAILY_TEMP_VARIATION"])

print(ds3.sql())
print(ds3.preview().head())
