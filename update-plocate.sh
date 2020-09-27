#! /bin/bash

set -e

/usr/local/sbin/plocate-build /var/lib/mlocate/mlocate.db /var/lib/mlocate/plocate.db.new
chgrp mlocate /var/lib/mlocate/plocate.db.new
mv /var/lib/mlocate/plocate.db.new /var/lib/mlocate/plocate.db
