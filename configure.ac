dnl - The second argument of AC_INIT is the package version number.
dnl - It should be specified as major.minor.revision [1.3.0]
dnl - The third argument, should be replaced with the package 
dnl - maintainers email address.

AC_INIT([circulardb], [0.1.0], [dan@powerset.com])
SHORT_DESCRIPTION="Circular database for time series data storage"

dnl - The following prerequisites line can be used to specify
dnl - the exact, or minimum, versions of packages on which this
dnl - package will depend (for build _and_ runtime)

#RUNTIME_PREREQUISITES="wordnet-2 >= 2.1.0"

dnl - The following prerequisites line can be used to specify
dnl - the exact, or minimum, versions of packages on which this
dnl - package will depend (for build only)

#BUILD_PREREQUISITES="boost"

dnl -- [ end basic configuration ]



AC_CONFIG_AUX_DIR([bones-common])

AC_CANONICAL_SYSTEM
AC_PREFIX_DEFAULT([/p])

AX_INIT

AM_INIT_AUTOMAKE
AM_MAINTAINER_MODE

AC_PROG_CC
AM_CONFIG_HEADER(config.h)

AM_PROG_LIBTOOL
AC_PROG_MAKE_SET

AC_SUBST(SHORT_DESCRIPTION)



dnl
dnl - check for common utilities
dnl

AC_PROG_AWK
AC_PROG_EGREP

AC_PATH_PROG(TEE, tee)

AC_CHECK_PROG(HAVE_PERL, perl, yes, 
              [AC_MSG_ERROR([the Powerset build environment requires perl])])


dnl
dnl - determine whether or not to use pkg-config
dnl

PKG_PROG_PKG_CONFIG

AC_ARG_ENABLE([pkgconfig],
  AC_HELP_STRING([--enable-pkgconfig],
                 [check build deps with pkg-config (default: auto)]),
                 [dopkgconfig="$enableval"], [dopkgconfig="maybe"])

case "$dopkgconfig" in
    yes) if test -n "$PKG_CONFIG"; then
           using_pkgconfig=yes
         else
           AC_MSG_ERROR([pkg-config not found])
         fi
         ;;
  maybe) if test -n "$PKG_CONFIG"; then
           using_pkgconfig=yes
         fi
         ;;
esac
    
AM_CONDITIONAL([HAVE_PKGCONFIG], [test -n "$PKG_CONFIG"])

dnl
dnl - extract build pkgconfig and non-pkgconfig prereqs
dnl

AX_PACKAGE_LIST([BUILD], [$BUILD_PREREQUISITES])

if test -n "$using_pkgconfig"; then
  AX_SPLIT_PREREQUISITES([BUILD_PREREQUISITES], [$BUILD_PREREQUISITES])
  if test -n "$BUILD_PREREQUISITES_PKGCONFIG"; then
    PKG_CHECK_MODULES([BUILD_PREREQUISITES], [$BUILD_PREREQUISITES_PKGCONFIG])
  fi
fi

AC_SUBST(BUILD_PREREQUISITES_NONPKGCONFIG)
AC_SUBST(BUILD_PREREQUISITES_PKGCONFIG)

dnl
dnl - extract runtime pkgconfig and non-pkgconfig prereqs
dnl

AX_PACKAGE_LIST([RUNTIME], [$RUNTIME_PREREQUISITES])

if test -n "$using_pkgconfig"; then
  AX_SPLIT_PREREQUISITES([RUNTIME_PREREQUISITES], [$RUNTIME_PREREQUISITES])
  if test -n "$RUNTIME_PREREQUISITES_PKGCONFIG"; then
    PKG_CHECK_MODULES([RUNTIME_PREREQUISITES], [$RUNTIME_PREREQUISITES_PKGCONFIG])
  fi
fi

AC_SUBST(RUNTIME_PREREQUISITES_NONPKGCONFIG)
AC_SUBST(RUNTIME_PREREQUISITES_PKGCONFIG)

dnl
dnl - establish single cflag and library var for automake
dnl

AX_CHECK_LD_AS_NEEDED

if test -n "$BUILD_PREREQUISITES_LIBS" -a x"$ax_cv_prog_ld_as_needed" = xyes; then
  BUILD_PREREQUISITES_LIBS="-Wl,--as-needed $BUILD_PREREQUISITES_LIBS -Wl,--no-as-needed"

  # Force the build packages to be installed if the linker isn't 
  # smart enough to exclude dependencies with no symbols in use
  RUNTIME_PACKAGES="$RUNTIME_PACKAGES $BUILD_PACKAGES"
  unset BUILD_PACKAGES
fi

PKGCONFIG_PREREQUISITES_CFLAGS="$BUILD_PREREQUISITES_CFLAGS $RUNTIME_PREREQUISITES_CFLAGS"
PKGCONFIG_PREREQUISITES_LIBS="$BUILD_PREREQUISITES_LIBS $RUNTIME_PREREQUISITES_LIBS"

AC_SUBST(PKGCONFIG_PREREQUISITES_CFLAGS)
AC_SUBST(PKGCONFIG_PREREQUISITES_LIBS)


dnl
dnl - see if we can create packages
dnl

AC_CHECK_PROG(HAVE_PP_CREATE, pp_create.rb, yes, no)
AM_CONDITIONAL(HAVE_PP_CREATE, test $HAVE_PP_CREATE = yes)

dnl
dnl - Allow override of auto-detect package architecture
dnl

AC_ARG_WITH([package-target],
            AC_HELP_STRING([--with-package-target=ARG],
                           [override the detected rpm architecture]),
            [cv_force_arch="$withval"],
            [cv_force_arch="$PACKAGE_TARGET"])

AM_CONDITIONAL(FORCE_PKG_ARCH, test -n "$cv_force_arch")
AC_SUBST(cv_force_arch)


dnl
dnl - internal use only
dnl

AC_ARG_WITH([internal-release],
            AC_HELP_STRING([--with-internal-release=ARG],
                           [internal use only]),
            [PACKAGE_RELEASE=$withval],
            [PACKAGE_RELEASE=0])

AC_SUBST(PACKAGE_RELEASE)

dnl
dnl - build the PKG_DEPENDS list for the yaml
dnl

AX_INIT_DEPENDENCY_SOLVER
                       
AX_GENERATE_PKG_DEPENDS([RUNTIME_DEPENDS_LIST],
                        [$RUNTIME_PACKAGES],
                        [$pkg_name: {min: '$pkg_version', max: '$pkg_version_max'}])

AX_GENERATE_PKG_DEPENDS([BUILD_DEPENDS_LIST],
                        [$RUNTIME_PACKAGES $BUILD_PACKAGES],
                        [$pkg_name: {eq: '$pkg_version-$pkg_release'}],
                        [want-closure])

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
dnl - Leave coverage and hardcore tests for last since
dnl - they mutate flags which could alter other test results
dnl

AC_ARG_ENABLE(coverage,
  AC_HELP_STRING([--enable-coverage],
                 [turn on -ftest-coverage -fprofile-arcs [default=no]]),
              [case "${enableval}" in
                yes) ENABLE_COVERAGE=1 ;;
                no) ENABLE_COVERAGE=0 ;;
                *) AC_MSG_ERROR(bad value ${enableval} for --enable-coverage) ;;
              esac],
              [ENABLE_COVERAGE=2])

if test "x$ENABLE_COVERAGE" = "x1"; then
  AC_MSG_WARN(enable coverage)
  CFLAGS=`echo "$CFLAGS -ftest-coverage -fprofile-arcs" | sed 's/-O[[[:digit:]]]//g'`
  CXXFLAGS=`echo "$CXXFLAGS -ftest-coverage -fprofile-arcs" | sed 's/-O[[[:digit:]]]//g'`
fi

AC_SUBST(ENABLE_COVERAGE)

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
  CXXFLAGS=`echo "$CXXFLAGS -O0" | sed 's/-O[[1-9]]]//g'`
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




dnl - Uncomment these if you need (f)lex/yacc(bison) support
#AM_PROG_LEX
#AC_PROG_YACC

dnl - Uncomment if you need sicstus support
#AX_PATH_SICSTUS

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([fcntl.h inttypes.h regex.h stdint.h stdlib.h string.h unistd.h values.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_INT64_T
AC_TYPE_OFF_T
AC_TYPE_UINT64_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_STRFTIME
AC_CHECK_FUNCS([regcomp strerror])

AC_CONFIG_FILES([
    Makefile
    src/Makefile
    tests/Makefile
    include/Makefile
    build-yaml-template.yaml
    yaml-template.yaml
    pkgconfig-template.pc
])

AC_OUTPUT
AX_FINI
