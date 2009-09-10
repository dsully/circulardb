#!/usr/bin/python

from distutils.core import setup, Extension

setup(
  name        = "CircularDB",
  version     = "1.0",
  ext_modules = [ Extension("circulardb", ["circulardb.c"],
    define_macros = [],
    libraries     = ["circulardb"], # @GSL_LIBS@
    include_dirs  = ["../../include"],
    library_dirs  = ["../../src/.libs"],
  ) ]
)
