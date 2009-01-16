# See also:
# http://www.scons.org/doc/1.1.0/HTML/scons-user/x4032.html
# http://www.scons.org/wiki/SconsRecipes

env = Environment()
env.Tool('packaging') 

env.MergeFlags('-std=c99 -I. -Iinclude -Wall -Werror -Wpointer-arith -Wwrite-strings -pg')
env.Append(
    RPATH = '/p/lib',
    CPPDEFINES = ['_GNU_SOURCE', '_REENTRANT']
)

conf = Configure(env, config_h = "config.h")
conf.CheckLib('m')

# http://www.scons.org/wiki/GSoC2007/MaciejPasternacki/ModelPrograms/FetchmailSConstruct
for header in ['ctype.h', 'errno.h', 'fcntl.h', 'float.h', 
               'inttypes.h', 'math.h', 'stdbool.h', 'stdio.h',
               'stdint.h', 'stdlib.h', 'string.h', 'sys/types.h', 
               'sys/stat.h', 'time.h', 'unistd.h', 'values.h']:
    conf.CheckHeader(header)

for func in ['mmap', 'strftime', 'strerror', 'fdatasync', 'fsync', 'isnormal']:
    conf.CheckFunc(func)

# This must come first, since lgsl requires it. And once we find it, it's appended to the config.
if not conf.CheckLib('gslcblas'):
    print 'CircularDB needs the GSL library and headers.'
    Exit(1)

if not conf.CheckLibWithHeader('gsl', 'gsl/gsl_sys.h', 'C'):
    print 'CircularDB needs the GSL library and headers.'
    Exit(1)

conf.CheckType('off_t', '#include <sys/types.h>\n')

env = conf.Finish()

shared = env.SharedLibrary('circulardb', ['src/circulardb.c'])
static = env.StaticLibrary('circulardb', ['src/circulardb.c'])

files = env.Install('/p/lib', [shared, static])

#rpm = env.Package(NAME           = 'circulardb',
#                  VERSION        = '1.0.0',
#                  PACKAGEVERSION = 0,
#                  PACKAGETYPE    = 'rpm',
#                  LICENSE        = 'private',
#                  SUMMARY        = 'foo',
#                  X_RPM_GROUP    = 'System/Foo',
#                  DESCRIPTION    = 'circulardb rpm',
#                  source         = [files])
#
#Default(rpm)

# vim: syntax=python
