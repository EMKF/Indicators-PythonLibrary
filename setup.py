from setuptools import setup, find_packages

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name='kcr',
    url='https://github.com/EMKF/downwardata',
    author='Travis Howe',
    author_email='thowe@kauffman.org',
    # Needed to actually package something
    packages=find_packages(),
    # ['kauffman'],
    # Needed for dependencies
    install_requires=[
        'numpy', 'pandas', 'requests', 'statsmodels',
    ],
    # 'xlrd', 'boto3', 'plotly', 'plotly-geo', 'geopandas', 'pyshp',
    #         'shapely', 'psutil', 's3fs', 'webdriver-manager'
    # *strongly* suggested for sharing
    version='2.0.1',
    # The license can be anything you like
    license='MIT',
    description='Modules that pull and transform commonly used administrative data from online sources.',
    long_description=open('README.md').read(),
)