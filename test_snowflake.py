import rasgoql

creds = rasgoql.SnowflakeCredentials.from_env()
print(creds)

rql = rasgoql.connect(creds)
print('Connected!')

transforms = rql.list_transforms()
print(transforms)

transform = rql.define_transform('bin')
print(transform)
