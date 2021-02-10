from context import kauffman

df = kauffman.bfs(['BA_BA', 'BF_SBF8Q'], obs_level=['US', 'AK'])
print(df.head())
print(df.info())

# df = get_data(['BF_DUR4Q', 'BF_DUR8Q', 'BA_BA'], 'state', 2004, annualize=True)
