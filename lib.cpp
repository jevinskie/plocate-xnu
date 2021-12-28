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

#include "lib.h"

#include "db.h"

#include <algorithm>
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

using namespace std;

/* Compare two path names using the database directory order. This is not
   exactly strcmp () order: "a" < "a.b", so "a/z" < "a.b". */
int dir_path_cmp(const string &a, const string &b)
{
	auto [ai, bi] = mismatch(a.begin(), a.end(), b.begin(), b.end());
	if (ai == a.end() && bi == b.end()) {
		return 0;
	}
	if (ai == a.end()) {
		return -1;
	}
	if (bi == b.end()) {
		return 1;
	}
	if (*ai == *bi) {
		return 0;
	}
	if (*ai == '/') {
		return -1;
	}
	if (*bi == '/') {
		return 1;
	}
	return int((unsigned char)*ai) - int((unsigned char)*bi);
}

/* Sort LIST using dir_path_cmp () */
void string_list_dir_path_sort(vector<string> *list)
{
	sort(list->begin(), list->end(), [](const string &a, const string &b) {
		return dir_path_cmp(a, b) < 0;
	});
}

/* Is PATH included in LIST?  Update *IDX to move within LIST.

   LIST is assumed to be sorted using dir_path_cmp (), successive calls to this
   function are assumed to use PATH values increasing in dir_path_cmp (). */
bool string_list_contains_dir_path(const vector<string> *list, size_t *idx,
                                   const string &path)
{
	int cmp = 0;
	while (*idx < list->size() && (cmp = dir_path_cmp((*list)[*idx], path)) < 0) {
		(*idx)++;
	}
	if (*idx < list->size() && cmp == 0) {
		(*idx)++;
		return true;
	}
	return false;
}
