"""
:mod:`bein` -- LIMS and workflow manager for bioinformatics
===========================================================

.. module:: bein
   :platform: Unix
   :synopsis: Workflow and provenance manager for bioinformatics
.. moduleauthor:: BBCF <webmaster.bbcf@epfl.ch>

Bein contains a miniature LIMS (Laboratory Information Management
System) and a workflow manager.  It was written for the Bioinformatics
and Biostatistics Core Facility of the Ecole Polytechnique Federale de
Lausanne.  It is aimed at processes just complicated enough where the
Unix shell becomes problematic, but not so large as to justify all the
machinery of big workflow managers like KNIME or Galaxy.

This module contains all the core logic and functionality of bein.

There are three core classes you need to understand:

execution
    The actual class is Execution, but it is generally created with the
    execution contextmanager.  An execution tracks all the information
    about a run of a given set of programs.  It corresponds roughly to a
    script in shell.

MiniLIMS
    MiniLIMS represents a database and a directory of files.  The database
    stores metainformation about the files and records all executions run
    with this MiniLIMS.  You can go back and examine the return code, stdout,
    stderr, imported files, etc. from any execution.

program
    The @program decorator provides a very simple way to bind external
    programs into bein for use in executions.
"""

# Built-in modules #
import random, os, string

# Internal modules #
from bein.exe import execution
from bein.lims import MiniLIMS
from bein.prog import program

# Special varaibles #
__version__ = '1.1.0'

################################################################################
class ProgramOutput(object):
    """Object passed to return_value functions when binding programs.

    Programs bound with ``@program`` can call a function when they are
    finished to create a return value from their output.  The output
    is passed as a ``ProgramObject``, containing all the information
    available to bein about that program.
    """
    def __init__(self, return_code, pid, arguments, stdout, stderr):
        self.return_code = return_code
        self.pid = pid
        self.arguments = arguments
        self.stdout = stdout
        self.stderr = stderr

################################################################################
class ProgramFailed(Exception):
    """Thrown when a program bound by ``@program`` exits with a value other than 0."""
    def __init__(self, output):
        self.output = output
    def __str__(self):
        message = "Running '%s' failed with " % " ".join(self.output.arguments)
        if self.output.stdout: message += "stdout:\n%s" % "".join(self.output.stdout)
        if self.output.stderr: message += "stderr:\n%s" % "".join(self.output.stderr)
        return message

################################################################################
def unique_filename_in(path=None):
    """Return a random filename unique in the given path.

    The filename returned is twenty alphanumeric characters which are
    not already serving as a filename in *path*.  If *path* is
    omitted, it defaults to the current working directory.
    """
    if path == None:
        path = os.getcwd()
    def random_string():
        return "".join([random.choice(string.letters + string.digits)
                        for x in range(20)])
    while True:
        filename = random_string()
        files = [f for f in os.listdir(path) if f.startswith(filename)]
        if files == []:
            break
    return filename

################################################################################
def task(f):
    """Wrap the function *f* in an execution.

    The @task decorator wraps a function in an execution and handles
    producing a sensible return value.  The function must expect its
    first argument to be an execution.  The function produced by @task
    instead expects a MiniLIMS (or ``None``) in its place.
    You can also pass a ``description`` keyword argument, which will
    be used to set the description of the execution.  For example,::

        @task
        def f(ex, filename):
            touch(ex, filename)
            ex.add(filename, "New file")
            return {'created': filename}

    will be wrapped into a function that is called as::

        f(M, "boris", description="An execution")

    where ``M`` is a MiniLIMS.  It could also be called with ``None``
    in place of ``M``::

        f(None, "boris")

    which is the same as creation an execution without attaching it to
    a MiniLIMS.  In this case it will fail, since ``f`` tries to add a
    file to the MiniLIMS.

    The return value is a dictionary with three keys:

        * ``value`` is the value returned by the function which @task
          wrapped.

        * ``files`` is a dictionary of all files the execution added
          to the MiniLIMS, with their descriptions as keys and their
          IDs in the MiniLIMS as values.

        * ``execution`` is the execution ID.

    In the call to ``f`` above, the return value would be (with some
    other value for ``'execution'`` in practice)::

        {'value': {'created': 'boris'},
         'files': {'New file': 'boris'},
         'execution': 33}
    """
    def wrapper(lims, *args, **kwargs):
        # If there is a description given, pull it out to use for the
        # execution.
        try:
            description = kwargs.pop('description')
        except KeyError:
            description = ""

        # Wrap the function to run in an execution.
        with execution(lims, description=description) as ex:
            v = f(ex, *args, **kwargs)

        # Pull together the return value.
        ex_id = ex.id
        if isinstance(lims, MiniLIMS):
            file_ids = lims.search_files(source=('execution', ex_id))
            files = dict([(lims.fetch_file(i)['description'],i) for i in file_ids])
        else:
            files = {}
        return {'value': v, 'files': files, 'execution': ex_id}

    wrapper.__doc__ = f.__doc__
    wrapper.__name__ = f.__name__
    return wrapper
