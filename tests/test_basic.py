from context import kauffman

from kauffman import bfs, bds, pep

# df = kauffman.bfs(['BA_BA', 'BF_SBF8Q'], obs_level='state')
# df = kauffman.bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level=['AZ'], annualize=True)
# df = kauffman.bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level=['US', 'AK'], march_shift=True)

# df = kauffman.bds(['FIRM', 'ESTAB'], obs_level='all')

# df = kauffman.pep(obs_level='us')
df = kauffman.pep(obs_level='state')


print(df.info())
print(df.head())
print(df.tail())


