from setuptools import setup

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name='kcr',
    url='https://github.com/EMKF/downwardata',
    author='Travis Howe',
    author_email='thowe@kauffman.org',
    # Needed to actually package something
    packages=['kauffman_data'],
    # Needed for dependencies
    install_requires=['numpy', 'pandas', 'requests', 'statsmodels'],
    # *strongly* suggested for sharing
    version='1.0.7',
    # The license can be anything you like
    license='MIT',
    description='Modules that pull and transform commonly used administrative data from online sources.',
    long_description=open('README.md').read(),
)