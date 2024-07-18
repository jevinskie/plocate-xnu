/* Common functions.

Copyright (C) 2005, 2007 Red Hat, Inc. All rights reserved.
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

#ifndef LIB_H__
#define LIB_H__

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include <sys/types.h>
#include <vector>

/* Compare two path names using the database directory order. This is not
   exactly strcmp () order: "a" < "a.b", so "a/z" < "a.b". */
extern int dir_path_cmp(const std::string &a, const std::string &b);

/* Sort LIST using dir_path_cmp () */
extern void string_list_dir_path_sort(std::vector<std::string> *list);

/* Is PATH included in LIST?  Update *IDX to move within LIST.

   LIST is assumed to be sorted using dir_path_cmp (), successive calls to this
   function are assumed to use PATH values increasing in dir_path_cmp (). */
extern bool string_list_contains_dir_path(const std::vector<std::string> *list,
                                          size_t *idx, const std::string &path);

#endif
