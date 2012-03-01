# Built-in modules #
import os, time, subprocess, threading

# Internal modules #
from bein import unique_filename_in
from bein import ProgramOutput, ProgramFailed
from bein.exe import Execution

################################################################################
class program(object):
    """Decorator to wrap external programs for use by bein.

    Bein depends on external programs to do most of its work.  In this
    sense, it's a strange version of a shell.  The ``@program`` decorator
    makes bindings to external programs only a couple lines long.

    To wrap a program, write a function that takes whatever arguments
    you will need to vary in calling the program (for instance, the
    filename for touch or the number of seconds to sleep for sleep).
    This function should return a dictionary containing two keys,
    ``'arguments'`` and ``'return_value'``.  ``'arguments'`` should
    point to a list of strings which is the actual command and
    arguments to be executed (``["touch",filename]`` for touch, for instance).
    ``'return_value'`` should point to a value to return, or a callable
    object which takes a ProgramOutput object and returns the value
    that will be passed back to the user when this program is run.

    For example, to wrap touch, we write a one argument function that
    takes the filename of the file to touch, and apply the ``@program``
    decorator to it::

        @program
        def touch(filename):
            return {"arguments": ["touch",filename],
                    "return_value": filename}

    Once we have such a function, how do we call it?  We can call it
    directly, but ``@program`` inserts an additional argument at the
    beginning of the argument list to take the execution the program
    is run in.  Typically it will be run like::

        with execution(lims) as ex:
            touch(ex, "myfile")

    where ``lims`` is a MiniLIMs object.  The ProgramOutput of touch
    is automatically recorded to the execution ``ex`` and stored in the
    MiniLIMS.  The value returned by touch is ``"myfile"``, the name of
    the touched file.

    Often you want to call a function, but not block when it returns
    so you can run several in parallel.  ``@program`` also creates a
    method ``nonblocking`` which does this.  The return value is a
    Future object with a single method: ``wait()``.  When you call
    ``wait()``, it blocks until the program finishes, then returns the
    same value that you would get from calling the function directly.
    So to touch two files, and not block until both commands have
    started, you would write::

        with execution(lims) as ex:
            a = touch.nonblocking(ex, "myfile1")
            b = touch.nonblocking(ex, "myfile2")
            a.wait()
            b.wait()

    By default, ``nonblocking`` runs local processes, but you can
    control how it runs its processes with the ``via`` keyword
    argument.  For example, on systems using the LSF batch submission
    system,s you can run commands via batch submission by passing the
    ``via`` argument the value ``"lsf"``::

        with execution(lims) as ex:
            a = touch.nonblocking(ex, "myfile1", via="lsf")
            a.wait()

    You can force local execution with ``via="local"``.

    Some programs do not accept an output file as an argument and only
    write to ``stdout``.  Alternately, you might need to capture
    ``stderr`` to a file.  All the methods of ``@program`` accept
    keyword arguments ``stdout`` and ``stderr`` to specify files to
    write these streams to.  If they are omitted, then both streams
    are captured and returned in the ``ProgramOutput`` object.
    """

    def __init__(self, gen_args):
        self.gen_args = gen_args
        self.__doc__ = gen_args.__doc__
        self.__name__ = gen_args.__name__

    def __call__(self, ex, *args, **kwargs):
        """Run a program locally, and block until it completes.

        This form takes one argument before those to the decorated
        function, an execution the program should be run as part of.
        The return_code, pid, stdout, stderr, and command arguments of
        the program are recorded to that execution, and thus to the
        MiniLIMS object.
        """
        if not(isinstance(ex,Execution)):
            raise ValueError("First argument to program " + self.gen_args.__name__ + " must be an Execution.")
        elif ex.id != None:
            raise SyntaxError("Program being called on an execution that has already terminated.")

        if kwargs.has_key('stdout'):
            stdout = open(kwargs['stdout'],'w')
            kwargs.pop('stdout')
        else:
            stdout = subprocess.PIPE

        if kwargs.has_key('stderr'):
            stderr = open(kwargs['stderr'],'w')
            kwargs.pop('stderr')
        else:
            stderr = subprocess.PIPE

        d = self.gen_args(*args, **kwargs)

        try:
            sp = subprocess.Popen(d["arguments"], bufsize=-1, stdout=stdout,
                                  stderr=stderr,
                                  cwd = ex.working_directory)
        except OSError:
            raise ValueError("Program %s does not seem to exist in your $PATH." % d['arguments'][0])

        return_code = sp.wait()
        if isinstance(stdout,file):
            stdout_value = None
        else:
            stdout_value = sp.stdout.readlines()

        if isinstance(stderr,file):
            stderr_value = None
        else:
            stderr_value = sp.stderr.readlines()

        po = ProgramOutput(return_code, sp.pid,
                           d["arguments"],
                           stdout_value, stderr_value)
        ex.report(po)
        if return_code == 0:
            z = d["return_value"]
            if callable(z):
                return z(po)
            else:
                return z
        else:
            raise ProgramFailed(po)

    def nonblocking(self, ex, *args, **kwargs):
        """Run a program, but return a Future object instead of blocking.

        Like __call__, nonblocking takes an Execution as an extra,
        initial argument before the arguments to the decorated
        function.  However, instead of blocking, it starts the program
        in a separate thread, and returns an object which lets the
        user choose when to wait for the program by calling its wait()
        method.  When wait() is called, the thread blocks, and the
        program is recorded in the execution and its value returned as
        if the use had called __call__ directory.  Thus,

        with execution(lims) as ex:
            f = touch("boris")

        is exactly equivalent to

        with execution(lims) as ex:
            a = touch.nonblocking("boris")
            f = a.wait()

        All the methods are named as _method, with the same arguments
        as ``nonblocking``.  That is, the ``via="local"`` method is
        implemented by ``_local``, the ``via="lsf"`` method by
        ``_lsf``, etc.  When writing a new method, name it in the same
        way, and add a condition to the ``if`` statement in
        ``nonblocking``.

        If you need to pass a keyword argument ``via`` to your
        program, you will need to call one of the hidden methods
        (``_local`` or ``_lsf``) directly.
        """
        if not(isinstance(ex,Execution)):
            raise ValueError("First argument to a program must be an Execution.")
        elif ex.id != None:
            raise SyntaxError("Program being called on an execution that has already terminated.")

        if kwargs.has_key('via'):
            via = kwargs['via']
            kwargs.pop('via')
        else:
            via = 'local'

        if via == 'local':
            return self._local(ex, *args, **kwargs)
        elif via == 'lsf':
            return self._lsf(ex, *args, **kwargs)

    def _local(self, ex, *args, **kwargs):
        """Method called by ``nonblocking`` for running locally.

        If you need to pass a ``via`` keyword argument to your
        function, you will have to call this method directly.
        """
        if kwargs.has_key('stdout'):
            stdout = open(kwargs['stdout'],'w')
            kwargs.pop('stdout')
        else:
            stdout = subprocess.PIPE

        if kwargs.has_key('stderr'):
            stderr = open(kwargs['stderr'],'w')
            kwargs.pop('stderr')
        else:
            stderr = subprocess.PIPE

        d = self.gen_args(*args, **kwargs)

        class Future(object):
            def __init__(self):
                self.program_output = None
                self.return_value = None
            def wait(self):
                v.wait()
                ex.report(self.program_output)
                if isinstance(f.return_value, Exception):
                    raise self.return_value
                else:
                    return self.return_value
        f = Future()
        v = threading.Event()
        def g():
            try:
                try:
                    sp = subprocess.Popen(d["arguments"], bufsize=-1,
                                          stdout=stdout,
                                          stderr=stderr,
                                          cwd = ex.working_directory)
                except OSError:
                    raise ValueError("Program %s does not seem to exist in your $PATH." % d['arguments'][0])

                return_code = sp.wait()
                if isinstance(stdout,file):
                    stdout_value = None
                else:
                    stdout_value = sp.stdout.readlines()

                if isinstance(stderr,file):
                    stderr_value = None
                else:
                    stderr_value = sp.stderr.readlines()

                f.program_output = ProgramOutput(return_code, sp.pid,
                                                 d["arguments"],
                                                 stdout_value,
                                                 stderr_value)
                if return_code == 0:
                    z = d["return_value"]
                    if callable(z):
                        f.return_value = z(f.program_output)
                    else:
                        f.return_value = z
                v.set()
            except Exception, e:
                f.return_value = e
                v.set()
        a = threading.Thread(target=g)
        a.start()
        return f

    def lsf(self, ex, *args, **kwargs):
        """Deprecated.  Use nonblocking(via="lsf") instead."""
        raise DeprecationWarning("Use nonblocking(via='lsf') instead.")
        return self._lsf(ex, *args, **kwargs)

    def _lsf(self, ex, *args, **kwargs):
        """Method called by ``nonblocking`` to run via LSF."""
        if not(isinstance(ex,Execution)):
            raise ValueError("First argument to a program must be an Execution.")

        if kwargs.has_key('stdout'):
            stdout = kwargs['stdout']
            kwargs.pop('stdout')
            load_stdout = False
        else:
            stdout = unique_filename_in(ex.working_directory)
            load_stdout = True

        if kwargs.has_key('stderr'):
            stderr = kwargs['stderr']
            kwargs.pop('stderr')
            load_stderr = False
        else:
            stderr = unique_filename_in(ex.working_directory)
            load_stderr = True

        d = self.gen_args(*args, **kwargs)

        # Jacques Rougemont figured out the following syntax that works in
        # both bash and tcsh.
        remote_cmd = " ".join(d["arguments"])
        remote_cmd += " > "+stdout
        remote_cmd = " ( "+remote_cmd+" ) >& "+stderr
        cmds = ["bsub","-cwd",ex.remote_working_directory,"-o","/dev/null",
                "-e","/dev/null","-K","-r",remote_cmd]
        class Future(object):
            def __init__(self):
                self.program_output = None
                self.return_value = None
            def wait(self):
                v.wait()
                ex.report(self.program_output)
                return self.return_value
        f = Future()
        v = threading.Event()
        def g():
            try:
                nullout = open(os.path.devnull, 'w')
                sp = subprocess.Popen(cmds, bufsize=-1, stdout=nullout,
                                      stderr=nullout)
                return_code = sp.wait()
                while not(os.path.exists(os.path.join(ex.working_directory,
                                                      stdout))):
                    time.sleep(1) # We need to wait until the files actually show up
                if load_stdout:
                    with open(os.path.join(ex.working_directory,stdout), 'r') as fo:
                        stdout_value = fo.readlines()
                else:
                    stdout_value = None

                while not(os.path.exists(os.path.join(ex.working_directory,stderr))):
                    time.sleep(1) # We need to wait until the files actually show up
                if load_stderr:
                    with open(os.path.join(ex.working_directory,stderr), 'r') as fe:
                        stderr_value = fe.readlines()
                else:
                    stderr_value = None

                f.program_output = ProgramOutput(return_code, sp.pid,
                                                 cmds, stdout_value, stderr_value)
                if return_code == 0:
                    z = d["return_value"]
                    if callable(z):
                        f.return_value = z(f.program_output)
                    else:
                        f.return_value = z
                v.set()
            except:
                f.return_value = None
                v.set()
                raise
        a = threading.Thread(target=g)
        a.start()
        return(f)

