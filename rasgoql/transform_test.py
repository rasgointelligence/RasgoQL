import rasgoql

creds = rasgoql.SnowflakeCredentials.from_env()
rql = rasgoql.connect(creds)
print("rasgoQL connected!")

# Start with an existing Dataset
ds = rql.dataset(fqtn='adventureworks.public.factinternetsales')
print(ds.preview().columns)

# Create a SQL chain by applying Transform(s)
chn = ds.cast(
    casts={
      'duedatekey':'int',
      'customerkey':'STRING'
    }
)
chn

chn = chn.target_encode(column='productkey', target='unitprice')
print(chn.sql())
chn = chn.drop_columns(exclude_cols=['customerphonenumber'])
print(chn.sql())

# Fork existing chains and keep adding to them
# Fork 1
chn1 = chn.concat(concat_list=["currencykey", "'_USD'"])
print('Fork 1:')
print(chn1)

# Fork 2
chn2 = chn.cast(casts = {'salesterritorykey':'string'})
chn2 = chn2.datetrunc(dates = {'orderdate':'month'})
print('Fork 2:')
print(chn2)

