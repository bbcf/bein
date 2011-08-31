from distutils.core import setup

setup(
        name            =   'bein',
        version         =   '1.1.0',
        description     =   'Miniature LIMS and workflow manager for bioinformatics',
        long_description=   open('README.md').read(),
        license         =   'GNU General Public License 3.0',
        url             =   'http://bbcf.epfl.ch/bein',
        author          =   'EPFL BBCF',
        author_email    =   'webmaster.bbcf@epfl.ch',
        classifiers     =   ['Topic :: Scientific/Engineering :: Bio-Informatics',
                             'Topic :: System :: Shells',],
        packages        =   ['bein'],
    )
