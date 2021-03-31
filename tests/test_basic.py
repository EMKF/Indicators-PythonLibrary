from context import kauffman

from kauffman import bfs, bds, pep, bed

df = kauffman.bed('1bf', obs_level=['AL', 'US', 'MO'])

# df = kauffman.bfs(['BA_BA', 'BF_SBF8Q'], obs_level=['AZ'])
# df = kauffman.bfs(['BA_BA', 'BF_SBF8Q'], obs_level='state')
# df = kauffman.bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level=['AZ'], annualize=True)
# df = kauffman.bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level=['US', 'AK'], march_shift=True)

# df = kauffman.bds(['FIRM', 'ESTAB'], obs_level='all')

# df = kauffman.pep(obs_level='us')
# df = kauffman.pep(obs_level='state')

#test = kauffman.bfs(['BA_BA'], obs_level='all', seasonally_adj=True, annualize=False, march_shift=True)
#print(test) should test whether march shift alone anualizes


# print(df)
print(df.info())
print(df.head())
print(df.tail())


