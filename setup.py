from setuptools import setup, find_packages

setup(
    name='kcr',
    url='https://github.com/EMKF/downwardata',
    author='Travis Howe',
    author_email='thowe@kauffman.org',
    packages=find_packages(),
    install_requires=[
        'numpy', 'pandas', 'requests', 'statsmodels', 'geonamescache'
    ],
    version='2.0.8',
    # The license can be anything you like
    license='MIT',
    description='Modules that pull and transform commonly used administrative data from online sources.',
    long_description=open('README.md').read(),
)