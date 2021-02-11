from context import kauffman

# df = kauffman.bfs(['BA_BA', 'BF_SBF8Q'], obs_level=['US', 'AK'])
df = kauffman.bds(['FIRM', 'ESTAB'], obs_level='all')

print(df.head())
print(df.tail())
print(df.info())
