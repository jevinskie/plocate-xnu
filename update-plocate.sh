#! /bin/bash

set -e

if [ @PROCESSED_BY_MESON@ = 1 ]; then
	SBINDIR=@sbindir@
	LOCATEGROUP=@groupname@
	DBFILE=@dbfile@
else
	SBINDIR=/usr/local/sbin
	LOCATEGROUP=plocate
	DBFILE=/var/lib/plocate/plocate.db
fi

$SBINDIR/plocate-build /var/lib/mlocate/mlocate.db $DBFILE.new
chgrp $LOCATEGROUP $DBFILE.new
mv $DBFILE.new $DBFILE
