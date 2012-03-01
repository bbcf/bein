# Built-in modules #
import os, sys, time, shutil, traceback
from contextlib import contextmanager

# Internal modules #
from bein import unique_filename_in

################################################################################
class Execution(object):
    """``Execution`` objects hold the state of a current running execution.

    You should generally use the execution function below to create an
    Execution, since it sets up the working directory properly.

    Executions are run against a particular MiniLIMS object where it
    records all the information onf programs that were run during it,
    fetches files from it, and writes files back to it.

    The important methods for the user to know are ``add`` and ``use``.
    Everything else is used internally by bein.  ``add`` puts a file
    into the LIMS repository from the execution's working directory.
    ``use`` fetches a file from the LIMS repository into the working
    directory.
    """
    def __init__(self, lims, working_directory):
        self.lims = lims
        self.working_directory = working_directory
        self.programs = []
        self.files = []
        self.used_files = []
        self.started_at = int(time.time())
        self.finished_at = None
        self.id = None

    def path_to_file(self, id_or_alias):
        """Fetch the path to *id_or_alias* in the attached LIMS."""
        if self.lims == None:
            raise ValueError("Cannot use path_to_file; no attached LIMS.")
        else:
            return self.lims.path_to_file(id_or_alias)

    def report(self, program):
        """Add a ProgramOutput object to the execution.

        When the Execution finishes, all programs added to the
        Execution with 'report', in the order the were added, are
        written into the MiniLIMS repository.
        """
        self.programs.append(program)

    def add(self, filename, description="", associate_to_id=None,
            associate_to_filename=None, template=None, alias=None):
        """Add a file to the MiniLIMS object from this execution.

        filename is the name of the file in the execution's working
        directory to import.  description is an optional argument to
        assign a string or a dictionary to describe that file in the MiniLIMS
        repository.

        Note that the file is not actually added to the repository
        until the execution finishes.
        """
        if isinstance(description,dict): description=str(description)
        if filename == None:
            if description == "":
                raise(IOError("Tried to add None to repository."))
            else:
                raise(IOError("Tried to add None to repository, with description '" + description +"' ."))
        elif not(os.path.exists(filename)):
            raise IOError("No such file or directory: '"+filename+"'")
        else:
            self.files.append((filename,description,associate_to_id,
                               associate_to_filename,template,alias))
    def finish(self):
        """Set the time when the execution finished."""
        self.finished_at = int(time.time())

    def use(self, file_or_alias):
        """Fetch a file from the MiniLIMS repository.

        fileid should be an integer assigned to a file in the MiniLIMS
        repository, or a string giving a file alias in the MiniLIMS
        repository.  The file is copied into the execution's working
        directory with a unique filename.  'use' returns the unique
        filename it copied the file into.
        """
        fileid = self.lims.resolve_alias(file_or_alias)
        try:
            filename = [x for (x,) in
                        self.lims.db.execute("select exportfile(?,?)",
                                             (fileid, self.working_directory))][0]
            for (f,t) in self.lims.associated_files_of(fileid):
                self.lims.db.execute("select exportfile(?,?)",
                                     (f, os.path.join(self.working_directory,t % filename)))
            self.used_files.append(fileid)
            return filename
        except ValueError:
            raise ValueError("Tried to use a nonexistent file id " + str(fileid))

################################################################################
@contextmanager
def execution(lims = None, description="", remote_working_directory=None):
    """Create an ``Execution`` connected to the given MiniLIMS object.

    ``execution`` is a ``contextmanager``, so it can be used in a ``with``
    statement, as in::

        with execution(mylims) as ex:
            touch('boris')

    It creates a temporary directory where the execution will work,
    sets up the ``Execution`` object, then runs the body of the
    ``with`` statement.  After the body finished, or if it fails and
    throws an exception, ``execution`` writes the ``Execution`` to the
    MiniLIMS repository and deletes the temporary directory after all
    is finished.

    The ``Execution`` has field ``id`` set to ``None`` during the
    ``with`` block, but afterwards ``id`` is set to the execution ID
    it ran as.  For example::

        with execution(mylims) as ex:
            pass

        print ex.id

    will print the execution ID the ``with`` block ran as.

    On some clusters, such as VITAL-IT in Lausanne, the path to the
    current directory is different on worker nodes where batch jobs
    run than on the nodes from which jobs are submitted.  For
    instance, if you are working in /scratch/abc on your local node,
    the worker nodes might mount the same directory as
    /nfs/boris/scratch/abc.  In this case, running programs via LSF
    would not work correctly.

    If this is the case, you can pass the equivalent directory on
    worker nodes as *remote_working_directory*.  In the example above,
    an execution may create a directory lK4321fdr21 in /scratch/abc.
    On the worker node, it would be /nfs/boris/scratch/abc/lK4321fd21,
    so you pass /nfs/boris/scratch/abc as *remote_working_directory*.
    """
    execution_dir = unique_filename_in(os.getcwd())
    os.mkdir(os.path.join(os.getcwd(), execution_dir))
    ex = Execution(lims,os.path.join(os.getcwd(), execution_dir))
    if remote_working_directory == None:
        ex.remote_working_directory = ex.working_directory
    else:
        ex.remote_working_directory = os.path.join(remote_working_directory,
                                                   execution_dir)
    os.chdir(os.path.join(os.getcwd(), execution_dir))
    exception_string = None
    try:
        yield ex
    except:
        (exc_type, exc_value, exc_traceback) = sys.exc_info()
        exception_string = ''.join(traceback.format_exception(exc_type, exc_value,
                                                            exc_traceback))
        raise
    finally:
        ex.finish()
        try:
            if lims != None:
                ex.id = lims.write(ex, description, exception_string)
        finally:
            os.chdir("..")
            shutil.rmtree(ex.working_directory, ignore_errors=True)
            cleaned_up = True
        assert(cleaned_up)
