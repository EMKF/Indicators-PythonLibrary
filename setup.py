from setuptools import setup, find_packages

setup(
    name='kauffman',
    url='https://github.com/EMKF/downwardata',
    maintainer='Katherine Stevens Andersen',
    maintainer_email='KAstev@gmail.com',
    packages=find_packages(),
    install_requires=[
        'requests', 'boto3', 'geonamescache', 'joblib', 'matplotlib', 'numpy',
        'pandas', 'scikit_learn', 'seaborn', 'selenium', 'webdriver_manager', 
        'xlrd'
    ],
    version='2.3.0',
    license='MIT',
    description='Modules that pull and transform commonly used administrative data from online sources.',
    long_description=open('README.md', encoding='utf8').read(),
    long_description_content_type="text/markdown"
)
