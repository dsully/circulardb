dnl - The second argument of AC_INIT is the package version number.
dnl - It should be specified as major.minor.revision [1.3.0]
dnl - The third argument, should be replaced with the package 
dnl - maintainers email address.

AC_INIT([circulardb], [0.2.3], [daniel-github@electricrain.com])

AC_CONFIG_AUX_DIR([build-aux])

dnl - Turn off Fortran crap.
m4_defun([_LT_AC_LANG_F77_CONFIG], [:])

AC_CANONICAL_SYSTEM

AM_INIT_AUTOMAKE
AM_MAINTAINER_MODE

AC_PROG_CC
AM_CONFIG_HEADER(config.h)

AM_PROG_LIBTOOL
AC_PROG_MAKE_SET

dnl
dnl - check for Darwin and MacOS X
dnl

AC_MSG_CHECKING([for Darwin (Mac OS X)])
if test "`(uname) 2>/dev/null`" = Darwin; then

  AC_CACHE_CHECK([for CoreFoundation.framework], ax_cv_lib_corefoundation, [
    hold_libs=$LIBS; 
    LIBS="$LIBS -framework CoreFoundation"
    AC_TRY_LINK([#include <CoreFoundation/CoreFoundation.h>],
                [CFBundleRef b = CFBundleGetMainBundle();],
                ax_cv_lib_corefoundation=yes, ax_cv_lib_corefoundation=no)
    LIBS=$hold_libs;])
  if test $ax_cv_lib_corefoundation = yes; then
    LIBS="$LIBS -framework CoreFoundation"
    AC_DEFINE(HAVE_COREFOUNDATION, 1,
              [Do we have access to Darwin CoreFoundation.framework?])
  else
    ax_no_corefoundation=1
  fi

  CPPFLAGS="$CPPFLAGS -no-cpp-precomp"
else
  AC_MSG_RESULT(no)
fi

dnl
dnl - disable optimizations and make things easier on gdb
dnl

AC_ARG_ENABLE(debug,
  AC_HELP_STRING([--enable-debug],
                 [disable optimizations and inlining [default=no]]),
              [case "${enableval}" in
                yes) ENABLE_DEBUG=1 ;;
                no) ENABLE_DEBUG=0 ;;
                *) AC_MSG_ERROR(bad value ${enableval} for --enable-debug) ;;
              esac],
              [ENABLE_DEBUG=2])

if test "x$ENABLE_DEBUG" = "x1"; then
  AC_MSG_WARN(enable debug)
  CPPFLAGS="$CPPFLAGS -ggdb -fno-inline"
  CFLAGS=`echo "$CFLAGS -O0" | sed 's/-O[[1-9]]//g'`
  CXXFLAGS=`echo "$CXXFLAGS -O0" | sed 's/-O[[1-9]]//g'`
fi

AC_SUBST(ENABLE_DEBUG)


dnl
dnl - default to strong warning enforcement for C/C++
dnl 

AC_ARG_ENABLE(hardcore,
  AC_HELP_STRING([--enable-hardcore],
                 [turn on strong warning enforcement [default=yes]]),
              [case "${enableval}" in
                yes) ENABLE_HARDCORE=1 ;;
                no) ENABLE_HARDCORE=0 ;;
                *) AC_MSG_ERROR(bad value ${enableval} for --enable-hardcore) ;;
              esac],
              [ENABLE_HARDCORE=2])

if test $ENABLE_HARDCORE -ne 0; then
  CPPFLAGS="$CPPFLAGS -Wall -Werror -Wpointer-arith -Wwrite-strings"
fi

AC_SUBST(ENABLE_HARDCORE)

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([fcntl.h string.h values.h])

# Checks for typedefs, structures, and compiler characteristics.
# autoconf 2.59 doesn't define these. 2.60 does.
AC_TYPE_OFF_T
AC_TYPE_INT64_T
AC_TYPE_UINT64_T

AC_CHECK_SIZEOF(double)
AC_CHECK_SIZEOF(long double)

if test $ac_cv_sizeof_long_double = 0; then
    AC_MSG_ERROR([long double is not a supported type with your compiler.])
fi
if test $ac_cv_sizeof_long_double = $ac_cv_sizeof_double; then
    AC_MSG_ERROR([long double is the same size as double. Precision may be lost.])
fi

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_MMAP
AC_FUNC_STRFTIME
AC_CHECK_FUNCS([strerror fdatasync fsync])

# Check for GSL
AM_PATH_GSL

AC_DEFINE(_GNU_SOURCE, 1, [GNU headers])

AC_CONFIG_FILES([
    Makefile
    src/Makefile
    tests/Makefile
    include/Makefile
])

AC_OUTPUT
