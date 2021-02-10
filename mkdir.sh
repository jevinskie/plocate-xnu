#! /bin/sh
mkdir -p ${DESTDIR}/"$1"

cat <<EOF >${DESTDIR}/"$1"/CACHEDIR.TAG
Signature: 8a477f597d28d172789f06886806bc55
# This file is a cache directory tag created by plocate.
# For information about cache directory tags, see:
#	https://bford.info/cachedir/
EOF
