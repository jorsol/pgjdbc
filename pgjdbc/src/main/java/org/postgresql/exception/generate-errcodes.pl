#!/usr/bin/perl
#
# Generate the PgSqlState.java class from errcodes.txt
# Copyright (c) 2000-2020, PostgreSQL Global Development Group

use warnings;
use strict;

print "/*\n";
print " * Copyright (c) 2020, PostgreSQL Global Development Group\n";
print " * See the LICENSE file in the project root for more information.\n";
print " */\n";

print "// autogenerated from src/backend/utils/errcodes.txt, do not edit\n\n";

print "package org.postgresql.exception;\n\n";

print "/**\n";
print " * This class is used for holding SQLState constants codes specific of PostgreSQL.\n";
print " *\n";
print " * \@see \"src/backend/utils/errcodes.txt\"\n";
print " */\n";
print "public final class PgSqlState {\n\n";

print "  private PgSqlState() {\n";
print "    throw new IllegalStateException();\n";
print "  }\n";

open my $errcodes, '<', 'errcodes.txt' or die;

while (<$errcodes>)
{
  chomp;

  # Skip comments
  next if /^#/;
  next if /^\s*$/;

  # Emit a comment for each section header
  if (/^Section:(.*)/)
  {
    my $header = $1;
    $header =~ s/^\s+//;
    print "\n  /*\n   * $header\n   */\n";
    next;
  }

  die "unable to parse errcodes.txt"
    unless /^([^\s]{5})\s+[EWS]\s+([^\s]+)/;

  my $sqlstate = $1;
  my $macro_name =  substr $2, 8;

  print "  public static final String $macro_name = \"$sqlstate\";\n";
}

print "}\n";

close $errcodes;
