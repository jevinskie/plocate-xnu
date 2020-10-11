#! /bin/bash

set -e

SBINDIR=@sbindir@
[ -d $SBINDIR ] || SBINDIR=/usr/local/sbin  # Default if not processed by Meson.

$SBINDIR/plocate-build /var/lib/mlocate/mlocate.db /var/lib/mlocate/plocate.db.new
chgrp mlocate /var/lib/mlocate/plocate.db.new
mv /var/lib/mlocate/plocate.db.new /var/lib/mlocate/plocate.db
