/* Bind mount detection.  Note: if you change this, change tmpwatch as well.

Copyright (C) 2005, 2007, 2008, 2012 Red Hat, Inc. All rights reserved.
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

#include "bind-mount.h"

#include "conf.h"
#include "lib.h"

#include <atomic>
#include <fcntl.h>
#include <limits.h>
#include <map>
#include <poll.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <sys/time.h>
#include <thread>

using namespace std;

/* mountinfo handling */

/* A single mountinfo entry */
struct mount {
	int id, parent_id;
	unsigned dev_major, dev_minor;
	string root;
	string mount_point;
	string fs_type;
	string source;
};

/* Path to mountinfo */
static const char *mountinfo_path;
atomic<bool> mountinfo_updated{ false };

multimap<pair<int, int>, mount> mount_entries;  // Keyed by device major/minor.

/* Read a line from F.
   Return a string, or empty string on error. */
static string read_mount_line(FILE *f)
{
	string line;

	for (;;) {
		char buf[LINE_MAX];

		if (fgets(buf, sizeof(buf), f) == nullptr) {
			if (feof(f))
				break;
			return "";
		}
		size_t chunk_length = strlen(buf);
		if (chunk_length > 0 && buf[chunk_length - 1] == '\n') {
			line.append(buf, chunk_length - 1);
			break;
		}
		line.append(buf, chunk_length);
	}
	return line;
}

/* Parse a space-delimited entry in STR, decode octal escapes, write it to
   DEST if it is not nullptr.  Return 0 if OK, -1 on error. */
static int parse_mount_string(string *dest, const char **str)
{
	const char *src = *str;
	while (*src == ' ' || *src == '\t') {
		src++;
	}
	if (*src == 0) {
		return -1;
	}
	string mount_string;
	for (;;) {
		char c = *src;

		switch (c) {
		case 0:
		case ' ':
		case '\t':
			goto done;

		case '\\':
			if (src[1] >= '0' && src[1] <= '7' && src[2] >= '0' && src[2] <= '7' && src[3] >= '0' && src[3] <= '7') {
				unsigned v;

				v = ((src[1] - '0') << 6) | ((src[2] - '0') << 3) | (src[3] - '0');
				if (v <= UCHAR_MAX) {
					mount_string.push_back(v);
					src += 4;
					break;
				}
			}
			/* Else fall through */

		default:
			mount_string.push_back(c);
			src++;
		}
	}

done:
	*str = src;
	if (dest != nullptr) {
		*dest = move(mount_string);
	}
	return 0;
}

/* Read a single entry from F. Return true if succesful. */
static bool read_mount_entry(FILE *f, mount *me)
{
	string line = read_mount_line(f);
	if (line.empty()) {
		return false;
	}
	size_t offset;
	if (sscanf(line.c_str(), "%d %d %u:%u%zn", &me->id, &me->parent_id, &me->dev_major,
	           &me->dev_minor, &offset) != 4) {
		return false;
	}
	const char *ptr = line.c_str() + offset;
	if (parse_mount_string(&me->root, &ptr) != 0 ||
	    parse_mount_string(&me->mount_point, &ptr) != 0 ||
	    parse_mount_string(nullptr, &ptr) != 0) {
		return false;
	}
	bool separator_found;
	do {
		string option;
		if (parse_mount_string(&option, &ptr) != 0) {
			return false;
		}
		separator_found = strcmp(option.c_str(), "-") == 0;
	} while (!separator_found);

	if (parse_mount_string(&me->fs_type, &ptr) != 0 ||
	    parse_mount_string(&me->source, &ptr) != 0 ||
	    parse_mount_string(nullptr, &ptr) != 0) {
		return false;
	}
	return true;
}

/* Read mount information from mountinfo_path, update mount_entries and
   num_mount_entries.
   Return 0 if OK, -1 on error. */
static int read_mount_entries(void)
{
	FILE *f = fopen(mountinfo_path, "r");
	if (f == nullptr) {
		return -1;
	}

	mount_entries.clear();

	mount me;
	while (read_mount_entry(f, &me)) {
		if (conf_debug_pruning) {
			fprintf(stderr,
			        " `%s' (%d on %d) is `%s' of `%s' (%u:%u), type `%s'\n",
			        me.mount_point.c_str(), me.id, me.parent_id, me.root.c_str(), me.source.c_str(),
			        me.dev_major, me.dev_minor, me.fs_type.c_str());
		}
		mount_entries.emplace(make_pair(me.dev_major, me.dev_minor), me);
	}
	fclose(f);
	return 0;
}

/* Bind mount path list maintenace and top-level interface. */

/* mountinfo_path file descriptor, or -1 */
static int mountinfo_fd;

/* Known bind mount paths */
static struct vector<string> bind_mount_paths; /* = { 0, }; */

/* Next bind_mount_paths entry */
static size_t bind_mount_paths_index; /* = 0; */

/* Rebuild bind_mount_paths */
static void rebuild_bind_mount_paths(void)
{
	if (conf_debug_pruning) {
		fprintf(stderr, "Rebuilding bind_mount_paths:\n");
	}
	if (read_mount_entries() != 0) {
		return;
	}
	if (conf_debug_pruning) {
		fprintf(stderr, "Matching bind_mount_paths:\n");
	}

	bind_mount_paths.clear();

	for (const auto &[dev_id, me] : mount_entries) {
		const auto &[first, second] = mount_entries.equal_range(make_pair(me.dev_major, me.dev_minor));
		for (auto it = first; it != second; ++it) {
			const mount &other = it->second;
			if (other.id == me.id) {
				// Don't compare an element to itself.
				continue;
			}
			// We have two mounts from the same device. Is one a prefix of the other?
			// If there are two that are equal, prefer the one with lowest ID.
			if (me.root.size() > other.root.size() && me.root.find(other.root) == 0) {
				if (conf_debug_pruning) {
					fprintf(stderr, " => adding `%s' (root `%s' is a child of `%s', mounted on `%s')\n",
					        me.mount_point.c_str(), me.root.c_str(), other.root.c_str(), other.mount_point.c_str());
				}
				bind_mount_paths.push_back(me.mount_point);
				break;
			}
			if (me.root == other.root && me.id > other.id) {
				if (conf_debug_pruning) {
					fprintf(stderr, " => adding `%s' (duplicate of mount point `%s')\n",
					        me.mount_point.c_str(), other.mount_point.c_str());
				}
				bind_mount_paths.push_back(me.mount_point);
				break;
			}
		}
	}
	if (conf_debug_pruning) {
		fprintf(stderr, "...done\n");
	}
	string_list_dir_path_sort(&bind_mount_paths);
}

/* Return true if PATH is a destination of a bind mount.
   (Bind mounts "to self" are ignored.) */
bool is_bind_mount(const char *path)
{
	if (mountinfo_updated.exchange(false)) {  // Atomic test-and-clear.
		rebuild_bind_mount_paths();
		bind_mount_paths_index = 0;
	}
	return string_list_contains_dir_path(&bind_mount_paths,
	                                     &bind_mount_paths_index, path);
}

/* Initialize state for is_bind_mount(), to read data from MOUNTINFO. */
void bind_mount_init(const char *mountinfo)
{
	mountinfo_path = mountinfo;
	mountinfo_fd = open(mountinfo_path, O_RDONLY);
	if (mountinfo_fd == -1)
		return;
	rebuild_bind_mount_paths();

	// mlocate re-polls this for each and every directory it wants to check,
	// for unclear reasons; it's possible that it's worried about a new recursive
	// bind mount being made while updatedb is running, causing an infinite loop?
	// Since it's probably for some good reason, we do the same, but we don't
	// want the barrage of syscalls. It's not synchronous, but the poll signal
	// isn't either; there's a slight race condition, but one that could only
	// be exploited by root.
	//
	// The thread is forcibly terminated on exit(), so we just let it loop forever.
	thread poll_thread([&] {
		for (;;) {
			struct pollfd pfd;
			/* Unfortunately (mount --bind $path $path/subdir) would leave st_dev
			   unchanged between $path and $path/subdir, so we must keep reparsing
			   mountinfo_path each time it changes. */
			pfd.fd = mountinfo_fd;
			pfd.events = POLLPRI;
			if (poll(&pfd, 1, /*timeout=*/-1) == -1) {
				perror("poll()");
				exit(1);
			}
			if ((pfd.revents & POLLPRI) != 0) {
				mountinfo_updated = true;
			}
		}
	});
	poll_thread.detach();
}
