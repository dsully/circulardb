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
AC_PROG_CPP
AC_PROG_CXX
AC_PROG_LIBTOOL
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MKDIR_P

AM_CONFIG_HEADER(config.h)
AC_PROG_MAKE_SET

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

AC_MSG_CHECKING([for Darwin (Mac OS X)])
if test "`(uname) 2>/dev/null`" = Darwin; then

  ENVARGS="ARCHFLAGS=\"-arch `/usr/bin/arch`\""
else
  AC_MSG_RESULT(no)
fi
AC_SUBST(ENVARGS)

AC_DEFUN([AX_CDB_BINDING], [
    AC_ARG_WITH($1,
        AC_HELP_STRING([--with-$1], [build the $2 library @<:@default=$3@:>@]),
            [with_$1="$withval"],
            [with_$1=$3]
        )
    ])

# Build bindings for different languages.
AX_CDB_BINDING(py, [Python], yes)
if test "$with_py" = "yes";  then
  AM_PATH_PYTHON(2.4,, :)
fi
AM_CONDITIONAL(WITH_PYTHON, [test -n "$PYTHON" -a "$PYTHON" != ":"])

AX_CDB_BINDING(perl, [Perl], yes)
if test "$with_perl" = "yes"; then
  AC_PATH_PROG([PERL], [perl])
fi
AM_CONDITIONAL(WITH_PERL, [test -n "$PERL"])

AX_CDB_BINDING(ruby, [Ruby], yes)
if test "$with_ruby" = "yes"; then
  AC_PATH_PROG([RUBY], [ruby])
fi
AM_CONDITIONAL(WITH_RUBY, [test -n "$RUBY"])

# Checks for header files.
AC_HEADER_STDC
AC_HEADER_STDBOOL
AC_HEADER_TIME
AC_CHECK_HEADERS([fcntl.h string.h values.h])

# Checks for typedefs, structures, and compiler characteristics.
# autoconf 2.59 doesn't define these. 2.60 does.

AC_TYPE_OFF_T
AC_TYPE_INT64_T
AC_TYPE_UINT64_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T

AC_CHECK_SIZEOF(double)

if test $ac_cv_sizeof_double = 0; then
    AC_MSG_ERROR([double is not a supported type with your compiler.])
fi

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_ALLOCA
AC_FUNC_STAT
AC_FUNC_MMAP
AC_FUNC_STRFTIME
AC_CHECK_FUNCS([strerror pread pwrite])

AC_DEFINE(_GNU_SOURCE, 1, [GNU headers])

# Check for GSL
AM_PATH_GSL

AC_CONFIG_FILES([
    Makefile
    bindings/Makefile
    bindings/perl/Makefile
    bindings/perl/Makefile.PL
    bindings/python/Makefile
    bindings/ruby/Makefile
    bindings/ruby/ext/extconf.rb
    src/Makefile
    tests/Makefile
    include/Makefile
])

AC_OUTPUT
