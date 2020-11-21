/* updatedb configuration.

Copyright (C) 2005, 2007, 2008 Red Hat, Inc. All rights reserved.
This copyrighted material is made available to anyone wishing to use, modify,
copy, or redistribute it subject to the terms and conditions of the GNU General
Public License v.2.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 51 Franklin
Street, Fifth Floor, Boston, MA 02110-1301, USA.

Author: Miloslav Trmac <mitr@redhat.com>

plocate modifications: Copyright (C) 2020 Steinar H. Gunderson.
plocate parts and modifications are licensed under the GPLv2 or, at your option,
any later version.
*/

#ifndef CONF_H__
#define CONF_H__

#include <stddef.h>
#include <string>
#include <vector>

/* true if locate(1) should check whether files are visible before reporting
   them */
extern bool conf_check_visibility;

/* Filesystems to skip, converted to uppercase and sorted by name */
extern std::vector<std::string> conf_prunefs;

/* Directory names to skip, sorted by name */
extern std::vector<std::string> conf_prunenames;

/* Paths to skip, sorted by name using dir_path_cmp () */
extern std::vector<std::string> conf_prunepaths;

/* true if bind mounts should be skipped */
extern bool conf_prune_bind_mounts;

/* true if pruning debug output was requested */
extern bool conf_debug_pruning;

/* Root of the directory tree to store in the database (canonical) */
extern char *conf_scan_root;

/* Absolute (not necessarily canonical) path to the database */
extern std::string conf_output;

/* true if file names should be written to stdout as they are found */
extern bool conf_verbose;

/* Configuration representation for the database configuration block */
extern std::string conf_block;

/* Parse /etc/updatedb.conf and command-line arguments ARGC, ARGV.
   Exit on error or --help, --version. */
extern void conf_prepare(int argc, char *argv[]);

extern int conf_block_size;
extern bool use_debug;

#endif
