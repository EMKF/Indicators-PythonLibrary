from setuptools import setup, find_packages

setup(
    name='kcr',
    url='https://github.com/EMKF/downwardata',
    author='Travis Howe',
    author_email='thowe@kauffman.org',
    packages=find_packages(),
    # install_requires=[
    #     'numpy', 'pandas', 'requests', 'statsmodels', 'geonamescache', 'xlrd==1.2.0', 'seaborn', 'sklearn'
    # ],
    install_requires=[
        'requests', 'boto3', 'geonamescache', 'joblib', 'matplotlib', 'numpy', 'pandas', 'scikit_learn', 'seaborn',
        'selenium', 'webdriver_manager', 'xlrd'
    ],
    # descartes, geopandas
    version='2.2.9',
    license='MIT',
    description='Modules that pull and transform commonly used administrative data from online sources.',
    long_description=open('README.md', encoding='utf8').read(),
    long_description_content_type="text/markdown"
)
