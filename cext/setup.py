from distutils.core import setup, Extension

threadmodule = Extension('thread', sources=['threadmodule.c'])

setup(name='pthread_table',
      version='0.1',
      description='Parallel access table implemented with POSIX threads',
      ext_modules=[threadmodule])
