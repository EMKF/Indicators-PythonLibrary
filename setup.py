from setuptools import setup, find_packages

setup(
    name='kcr',
    url='https://github.com/EMKF/downwardata',
    author='Travis Howe',
    author_email='thowe@kauffman.org',
    packages=find_packages(),
    install_requires=[
        'numpy', 'pandas', 'requests', 'statsmodels', 'geonamescache', 'xlrd==1.2.0'
    ],
    version='2.1.3',
    license='MIT',
    description='Modules that pull and transform commonly used administrative data from online sources.',
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown"
)
