#! /bin/bash

set -e

if [ @PROCESSED_BY_MESON@ = 1 ]; then
	SBINDIR=@sbindir@
	LOCATEGROUP=@locategroup@
else
	SBINDIR=/usr/local/sbin
	LOCATEGROUP=mlocate
fi

$SBINDIR/plocate-build /var/lib/mlocate/mlocate.db /var/lib/mlocate/plocate.db.new
chgrp $LOCATEGROUP /var/lib/mlocate/plocate.db.new
mv /var/lib/mlocate/plocate.db.new /var/lib/mlocate/plocate.db
