/*
 * testcirculardb
 * (C) 2006 Powerset, Inc.
 */

#ifndef LINT
static const char svnid[] __attribute__ ((unused)) = "$Id$";
#endif

#include <assert.h>

#include <circulardb.h>

int
main (int argc, char **argv)
{
  assert(0 == circulardb_run ());
  return 0;
}

/* -*- Mode: C; tab-width: 2 -*- */
/* vim: set tabstop=2 expandtab shiftwidth=2: */
