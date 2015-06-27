from setuptools import setup
import versioneer

versioneer.VCS = 'git'
versioneer.versionfile_source = '_version.py'
versioneer.versionfile_build = '_version.py'
versioneer.tag_prefix = ''
versioneer.parentdir_prefix = 'exif2timestream-'

desc = """
exif2timestream: Rename image files into a timestream from exif data.
"""

install_requires = [
    "ExifRead==1.4.2",
    "docopt==0.6.1",
    "voluptuous==0.8.4",
    "pexif==0.15",
    "numpy==1.9.2", 
    "scipy==0.15.1", 
    "scikit-image==0.11.3",  
]

test_requires = [
    "coverage==3.7.1",
    "nose==1.3.0",
    "pep8==1.4.6",
    "pylint==1.0.0",
]

setup(
    name="exif2timestream",
    py_modules=['exif2timestream', ],
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    install_requires=install_requires,
    tests_require=test_requires,
    description=desc,
    author="Kevin Murray",
    author_email="spam@kdmurray.id.au",
    url="https://github.com/borevitzlab/traitcapture-bin",
    keywords=["timestream", "timelapse", "photography", "video"],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "License :: OSI Approved :: GNU General Public License v3 or later " +
            "(GPLv3+)",
    ],
    test_suite="test",
)
