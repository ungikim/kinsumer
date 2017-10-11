import ast
import os

from setuptools import setup, find_packages


def get_version():
    with open(os.path.join('kinsumer', 'version.py')) as f:
        tree = ast.parse(f.read(), f.name)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Assign) and len(node.targets) == 1):
                continue
            target, = node.targets
            value = node.value
            if not (isinstance(target, ast.Name) and target.id == 'VERSION_INFO' and isinstance(value, ast.Tuple)):
                continue
            elts = value.elts
            if any(not isinstance(elt, ast.Num) for elt in elts):
                continue
            return '.'.join(str(elt.n) for elt in elts)


def readme():
    with open('README.md') as f:
        return f.read()


setup(
    name='kinsumer',
    version=get_version(),
    description='High level Amazon Kinesis Streams consumer',
    long_description=readme(),
    author='Ungi Kim',
    author_email='ungi' '@' 'ungikim.me',
    packages=find_packages(),
    python_requires='>=3.6.0',
    include_package_data=False,
    zip_safe=False,
    install_requires=[
        'click==6.7',
        'boto3==1.4.7',
        'gevent==1.2.2',
        'werkzeug==0.12.2',
        'typeguard==2.1.3',
        'python-daemon==2.1.2',
    ],
    extra_require={
        'dev': [
            'pytest==3.2.3',
            'pytest-mock==1.6.3',
            'tox==2.9.1',
            'flake8==3.4.1',
        ]
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: No Input/Output (Daemon)',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    entry_points="""
        [console_scripts]
        kinsumer=kinsumer.cli:main
    """
)
