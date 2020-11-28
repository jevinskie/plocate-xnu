#! /bin/bash

set -e

if [ @PROCESSED_BY_MESON@ = 1 ]; then
	SBINDIR=@sbindir@
	LOCATEGROUP=@locategroup@
else
	SBINDIR=/usr/local/sbin
	LOCATEGROUP=plocate
fi

$SBINDIR/plocate-build /var/lib/mlocate/mlocate.db /var/lib/plocate/plocate.db.new
chgrp $LOCATEGROUP /var/lib/plocate/plocate.db.new
mv /var/lib/plocate/plocate.db.new /var/lib/plocate/plocate.db
