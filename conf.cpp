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

#include "conf.h"

#include "lib.h"

#include <algorithm>
#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef __APPLE__
const char *program_invocation_name;
__attribute__((constructor)) static void init_program_invocation_name(void)
{
	program_invocation_name = getprogname();
}
#endif

using namespace std;

/* true if locate(1) should check whether files are visible before reporting
   them */
bool conf_check_visibility = true;

/* Filesystems to skip, converted to uppercase and sorted by name */
vector<string> conf_prunefs;

/* Directory names to skip, sorted by name */
vector<string> conf_prunenames;

/* Paths to skip, sorted by name using dir_path_cmp () */
vector<string> conf_prunepaths;

/* true if bind mounts should be skipped */
bool conf_prune_bind_mounts; /* = false; */

/* true if pruning debug output was requested */
bool conf_debug_pruning; /* = false; */

/* Root of the directory tree to store in the database (canonical) */
char *conf_scan_root; /* = NULL; */

/* Absolute (not necessarily canonical) path to the database */
string conf_output;

/* 1 if file names should be written to stdout as they are found */
bool conf_verbose; /* = false; */

/* Configuration representation for the database configuration block */
string conf_block;

int conf_block_size = 32;
bool use_debug = false;

/* Parse a STR, store the parsed boolean value to DEST;
   return 0 if OK, -1 on error. */
static int
parse_bool(bool *dest, const char *str)
{
	if (strcmp(str, "0") == 0 || strcmp(str, "no") == 0) {
		*dest = false;
		return 0;
	}
	if (strcmp(str, "1") == 0 || strcmp(str, "yes") == 0) {
		*dest = true;
		return 0;
	}
	return -1;
}

/* String list handling */

/* Add values from space-separated VAL to VAR and LIST */
static void
var_add_values(vector<string> *list, const char *val)
{
	for (;;) {
		const char *start;

		while (isspace((unsigned char)*val))
			val++;
		if (*val == 0)
			break;
		start = val;
		do
			val++;
		while (*val != 0 && !isspace((unsigned char)*val));
		list->emplace_back(start, val - start);
	}
}

/* Finish variable LIST, sort its contents, remove duplicates */
static void
var_finish(vector<string> *list)
{
	sort(list->begin(), list->end());
	auto new_end = unique(list->begin(), list->end());
	list->erase(new_end, list->end());
}

/* UPDATEDB_CONF parsing */

/* UPDATEDB_CONF (locked) */
static FILE *uc_file;
/* Line number at token start; type matches error_at_line () */
static unsigned uc_line;
/* Current line number; type matches error_at_line () */
static unsigned uc_current_line;
/* Last string returned by uc_lex */
static string uc_lex_buf;

/* Token types */
enum {
	UCT_EOF,
	UCT_EOL,
	UCT_IDENTIFIER,
	UCT_EQUAL,
	UCT_QUOTED,
	UCT_OTHER,
	UCT_PRUNE_BIND_MOUNTS,
	UCT_PRUNEFS,
	UCT_PRUNENAMES,
	UCT_PRUNEPATHS
};

/* Return next token from uc_file; for UCT_IDENTIFIER, UCT_QUOTED or keywords,
   store the data to uc_lex_buf (valid until next call). */
static int
uc_lex(void)
{
	int c;

	uc_lex_buf.clear();
	uc_line = uc_current_line;
	do {
		c = getc_unlocked(uc_file);
		if (c == EOF)
			return UCT_EOF;
	} while (c != '\n' && isspace((unsigned char)c));
	switch (c) {
	case '#':
		do {
			c = getc_unlocked(uc_file);
			if (c == EOF)
				return UCT_EOF;
		} while (c != '\n');
		/* Fall through */
	case '\n':
		uc_current_line++;
		return UCT_EOL;

	case '=':
		return UCT_EQUAL;

	case '"': {
		while ((c = getc_unlocked(uc_file)) != '"') {
			if (c == EOF || c == '\n') {
				fprintf(stderr, "%s:%u: missing closing `\"'\n",
				        UPDATEDB_CONF, uc_line);
				exit(EXIT_FAILURE);
			}
			uc_lex_buf.push_back(c);
		}
		return UCT_QUOTED;
	}

	default: {
		if (!isalpha((unsigned char)c) && c != '_')
			return UCT_OTHER;
		do {
			uc_lex_buf.push_back(c);
			c = getc_unlocked(uc_file);
		} while (c != EOF && (isalnum((unsigned char)c) || c == '_'));
		ungetc(c, uc_file);
		if (uc_lex_buf == "PRUNE_BIND_MOUNTS")
			return UCT_PRUNE_BIND_MOUNTS;
		if (uc_lex_buf == "PRUNEFS")
			return UCT_PRUNEFS;
		if (uc_lex_buf == "PRUNENAMES")
			return UCT_PRUNENAMES;
		if (uc_lex_buf == "PRUNEPATHS")
			return UCT_PRUNEPATHS;
		return UCT_IDENTIFIER;
	}
	}
}

/* Parse /etc/updatedb.conf.  Exit on I/O or syntax error. */
static void
parse_updatedb_conf(void)
{
	bool had_prune_bind_mounts, had_prunefs, had_prunenames, had_prunepaths;

	uc_file = fopen(UPDATEDB_CONF, "r");
	if (uc_file == NULL) {
		if (errno != ENOENT) {
			perror(UPDATEDB_CONF);
			exit(EXIT_FAILURE);
		}
		return;
	}
	flockfile(uc_file);
	uc_current_line = 1;
	had_prune_bind_mounts = false;
	had_prunefs = false;
	had_prunenames = false;
	had_prunepaths = false;
	for (;;) {
		bool *had_var;
		int var_token, token;

		token = uc_lex();
		switch (token) {
		case UCT_EOF:
			goto eof;

		case UCT_EOL:
			continue;

		case UCT_PRUNE_BIND_MOUNTS:
			had_var = &had_prune_bind_mounts;
			break;

		case UCT_PRUNEFS:
			had_var = &had_prunefs;
			break;

		case UCT_PRUNENAMES:
			had_var = &had_prunenames;
			break;

		case UCT_PRUNEPATHS:
			had_var = &had_prunepaths;
			break;

		case UCT_IDENTIFIER:
			fprintf(stderr, "%s:%u: unknown variable: `%s'\n",
			        UPDATEDB_CONF, uc_line, uc_lex_buf.c_str());
			exit(EXIT_FAILURE);

		default:
			fprintf(stderr, "%s:%u: variable name expected\n",
			        UPDATEDB_CONF, uc_line);
			exit(EXIT_FAILURE);
		}
		if (*had_var != false) {
			fprintf(stderr, "%s:%u: variable `%s' was already defined\n",
			        UPDATEDB_CONF, uc_line, uc_lex_buf.c_str());
			exit(EXIT_FAILURE);
		}
		*had_var = true;
		var_token = token;
		token = uc_lex();
		if (token != UCT_EQUAL) {
			fprintf(stderr, "%s:%u: `=' expected after variable name\n",
			        UPDATEDB_CONF, uc_line);
			exit(EXIT_FAILURE);
		}
		token = uc_lex();
		if (token != UCT_QUOTED) {
			fprintf(stderr, "%s:%u: value in quotes expected after `='\n",
			        UPDATEDB_CONF, uc_line);
			exit(EXIT_FAILURE);
		}
		if (var_token == UCT_PRUNE_BIND_MOUNTS) {
			if (parse_bool(&conf_prune_bind_mounts, uc_lex_buf.c_str()) != 0) {
				fprintf(stderr, "%s:%u: invalid value `%s' of PRUNE_BIND_MOUNTS\n",
				        UPDATEDB_CONF, uc_line, uc_lex_buf.c_str());
				exit(EXIT_FAILURE);
			}
		} else if (var_token == UCT_PRUNEFS)
			var_add_values(&conf_prunefs, uc_lex_buf.c_str());
		else if (var_token == UCT_PRUNENAMES)
			var_add_values(&conf_prunenames, uc_lex_buf.c_str());
		else if (var_token == UCT_PRUNEPATHS)
			var_add_values(&conf_prunepaths, uc_lex_buf.c_str());
		else
			abort();
		token = uc_lex();
		if (token != UCT_EOL && token != UCT_EOF) {
			fprintf(stderr, "%s:%u: unexpected data after variable value\n",
			        UPDATEDB_CONF, uc_line);
			exit(EXIT_FAILURE);
		}
		/* Fall through */
		while (token != UCT_EOL) {
			if (token == UCT_EOF)
				goto eof;
			token = uc_lex();
		}
	}
eof:
	if (ferror(uc_file)) {
		perror(UPDATEDB_CONF);
		exit(EXIT_FAILURE);
	}
	funlockfile(uc_file);
	fclose(uc_file);
}

/* Command-line argument parsing */

/* Output --help text */
static void
help(void)
{
	printf("Usage: updatedb [OPTION]...\n"
	       "Update a plocate database.\n"
	       "\n"
	       "  -f, --add-prunefs FS           omit also FS (space-separated)\n"
	       "  -n, --add-prunenames NAMES     omit also NAMES (space-separated)\n"
	       "  -e, --add-prunepaths PATHS     omit also PATHS (space-separated)\n"
	       "      --add-single-prunepath PATH  omit also PATH\n"
	       "  -U, --database-root PATH       the subtree to store in "
	       "database (default \"/\")\n"
	       "  -h, --help                     print this help\n"
	       "  -o, --output FILE              database to update (default\n"
	       "                                 `%s')\n"
	       "  -b, --block-size SIZE          number of filenames to store\n"
	       "                                 in each block (default 32)\n"
	       "      --prune-bind-mounts FLAG   omit bind mounts (default "
	       "\"no\")\n"
	       "      --prunefs FS               filesystems to omit from "
	       "database\n"
	       "      --prunenames NAMES         directory names to omit from "
	       "database\n"
	       "      --prunepaths PATHS         paths to omit from database\n"
	       "  -l, --require-visibility FLAG  check visibility before "
	       "reporting files\n"
	       "                                 (default \"yes\")\n"
	       "  -v, --verbose                  print paths of files as they "
	       "are found\n"
	       "  -V, --version                  print version information\n"
	       "\n"
	       "The configuration defaults to values read from\n"
	       "`%s'.\n",
	       DBFILE, UPDATEDB_CONF);
	printf("\n"
	       "Report bugs to %s.\n",
	       PACKAGE_BUGREPORT);
}

/* Prepend current working directory to PATH;
   return resulting path */
static string
prepend_cwd(const string &path)
{
	const char *res;
	string buf;
	buf.resize(BUFSIZ); /* Not PATH_MAX because it is not defined on some platforms. */
	do
		buf.resize(buf.size() * 1.5);
	while ((res = getcwd(buf.data(), buf.size())) == NULL && errno == ERANGE);
	if (res == NULL) {
		fprintf(stderr, "%s: %s: can not get current working directory\n",
		        program_invocation_name, strerror(errno));
		exit(EXIT_FAILURE);
	}
	buf.resize(strlen(buf.data()));
	return buf + '/' + path;
}

/* Parse ARGC, ARGV.  Exit on error or --help, --version. */
static void
parse_arguments(int argc, char *argv[])
{
	enum { OPT_DEBUG_PRUNING = CHAR_MAX + 1,
	       OPT_ADD_SINGLE_PRUNEPATH = CHAR_MAX + 2 };

	static const struct option options[] = {
		{ "add-prunefs", required_argument, NULL, 'f' },
		{ "add-prunenames", required_argument, NULL, 'n' },
		{ "add-prunepaths", required_argument, NULL, 'e' },
		{ "add-single-prunepath", required_argument, NULL, OPT_ADD_SINGLE_PRUNEPATH },
		{ "database-root", required_argument, NULL, 'U' },
		{ "debug-pruning", no_argument, NULL, OPT_DEBUG_PRUNING },
		{ "help", no_argument, NULL, 'h' },
		{ "output", required_argument, NULL, 'o' },
		{ "prune-bind-mounts", required_argument, NULL, 'B' },
		{ "prunefs", required_argument, NULL, 'F' },
		{ "prunenames", required_argument, NULL, 'N' },
		{ "prunepaths", required_argument, NULL, 'P' },
		{ "require-visibility", required_argument, NULL, 'l' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "version", no_argument, NULL, 'V' },
		{ "block-size", required_argument, 0, 'b' },
		{ "debug", no_argument, 0, 'D' },  // Not documented.
		{ NULL, 0, NULL, 0 }
	};

	bool prunefs_changed, prunenames_changed, prunepaths_changed;
	bool got_prune_bind_mounts, got_visibility;

	prunefs_changed = false;
	prunenames_changed = false;
	prunepaths_changed = false;
	got_prune_bind_mounts = false;
	got_visibility = false;
	for (;;) {
		int opt, idx;

		opt = getopt_long(argc, argv, "U:Ve:f:hl:n:o:vb:D", options, &idx);
		switch (opt) {
		case -1:
			goto options_done;

		case '?':
			exit(EXIT_FAILURE);

		case 'B':
			if (got_prune_bind_mounts != false) {
				fprintf(stderr, "%s: --%s would override earlier command-line argument\n",
				        program_invocation_name, "prune-bind-mounts");
				exit(EXIT_FAILURE);
			}
			got_prune_bind_mounts = true;
			if (parse_bool(&conf_prune_bind_mounts, optarg) != 0) {
				fprintf(stderr, "%s: invalid value %s of --%s\n",
				        program_invocation_name, optarg, "prune-bind-mounts");
				exit(EXIT_FAILURE);
			}
			break;

		case 'F':
			if (prunefs_changed != false) {
				fprintf(stderr, "%s: --%s would override earlier command-line argument\n",
				        program_invocation_name, "prunefs");
				exit(EXIT_FAILURE);
			}
			prunefs_changed = true;
			conf_prunefs.clear();
			var_add_values(&conf_prunefs, optarg);
			break;

		case 'N':
			if (prunenames_changed != false) {
				fprintf(stderr, "%s: --%s would override earlier command-line argument\n",
				        program_invocation_name, "prunenames");
				exit(EXIT_FAILURE);
			}
			prunenames_changed = true;
			conf_prunenames.clear();
			var_add_values(&conf_prunenames, optarg);
			break;

		case 'P':
			if (prunepaths_changed != false) {
				fprintf(stderr, "%s: --%s would override earlier command-line argument\n",
				        program_invocation_name, "prunepaths");
				exit(EXIT_FAILURE);
			}
			prunepaths_changed = true;
			conf_prunepaths.clear();
			var_add_values(&conf_prunepaths, optarg);
			break;

		case 'U':
			if (conf_scan_root != NULL) {
				fprintf(stderr, "%s: --%s specified twice\n",
				        program_invocation_name, "database-root");
				exit(EXIT_FAILURE);
			}
			conf_scan_root = realpath(optarg, nullptr);
			if (conf_scan_root == NULL) {
				fprintf(stderr, "%s: %s: invalid value `%s' of --%s\n",
				        program_invocation_name, strerror(errno), optarg, "database-root");
				exit(EXIT_FAILURE);
			}
			break;

		case 'V':
			puts("updatedb (" PACKAGE_NAME ") " PACKAGE_VERSION);
			puts("Copyright (C) 2007 Red Hat, Inc. All rights reserved.\n"
			     "This software is distributed under the GPL v.2.\n"
			     "\n"
			     "This program is provided with NO WARRANTY, to the extent "
			     "permitted by law.");
			exit(EXIT_SUCCESS);

		case 'e':
			prunepaths_changed = true;
			var_add_values(&conf_prunepaths, optarg);
			break;

		case OPT_ADD_SINGLE_PRUNEPATH:
			prunepaths_changed = true;
			conf_prunepaths.push_back(optarg);
			break;

		case 'f':
			prunefs_changed = true;
			var_add_values(&conf_prunefs, optarg);
			break;

		case 'h':
			help();
			exit(EXIT_SUCCESS);

		case 'l':
			if (got_visibility != false) {
				fprintf(stderr, "%s: --%s specified twice\n",
				        program_invocation_name, "require-visibility");
				exit(EXIT_FAILURE);
			}

			got_visibility = true;
			if (parse_bool(&conf_check_visibility, optarg) != 0) {
				fprintf(stderr, "%s: invalid value `%s' of --%s",
				        program_invocation_name, optarg, "require-visibility");
				exit(EXIT_FAILURE);
			}
			break;

		case 'n':
			prunenames_changed = true;
			var_add_values(&conf_prunenames, optarg);
			break;

		case 'o':
			if (!conf_output.empty()) {
				fprintf(stderr, "%s: --%s specified twice",
				        program_invocation_name, "output");
				exit(EXIT_FAILURE);
			}
			conf_output = optarg;
			break;

		case 'v':
			conf_verbose = true;
			break;

		case 'b':
			conf_block_size = atoi(optarg);
			break;

		case 'D':
			use_debug = true;
			break;

		case OPT_DEBUG_PRUNING:
			conf_debug_pruning = true;
			break;

		default:
			abort();
		}
	}
options_done:
	if (optind != argc) {
		fprintf(stderr, "%s: unexpected operand on command line",
		        program_invocation_name);
		exit(EXIT_FAILURE);
	}
	if (conf_scan_root == NULL) {
		static char root[] = "/";

		conf_scan_root = root;
	}
	if (conf_output.empty())
		conf_output = DBFILE;
	if (conf_output[0] != '/')
		conf_output = prepend_cwd(conf_output);
}

/* Conversion of configuration for main code */

/* Store a string list to OBSTACK */
static void
gen_conf_block_string_list(string *obstack,
                           const vector<string> *strings)
{
	for (const string &str : *strings) {
		*obstack += str;
		*obstack += '\0';
	}
	*obstack += '\0';
}

/* Generate conf_block */
static void
gen_conf_block(void)
{
	conf_block.clear();

#define CONST(S) conf_block.append(S, sizeof(S))
	/* conf_check_visibility value is stored in the header */
	CONST("prune_bind_mounts");
	/* Add two NUL bytes after the value */
	conf_block.append(conf_prune_bind_mounts != false ? "1\0" : "0\0", 3);
	CONST("prunefs");
	gen_conf_block_string_list(&conf_block, &conf_prunefs);
	CONST("prunenames");
	gen_conf_block_string_list(&conf_block, &conf_prunenames);
	CONST("prunepaths");
	gen_conf_block_string_list(&conf_block, &conf_prunepaths);
	/* scan_root is contained directly in the header */
	/* conf_output, conf_verbose are not relevant */
#undef CONST
}

/* Parse /etc/updatedb.conf and command-line arguments ARGC, ARGV.
   Exit on error or --help, --version. */
void conf_prepare(int argc, char *argv[])
{
	parse_updatedb_conf();
	parse_arguments(argc, argv);
	for (string &str : conf_prunefs) {
		/* Assuming filesystem names are ASCII-only */
		for (char &c : str)
			c = toupper(c);
	}
	/* Finish the variable only after converting filesystem names to upper case
	   to avoid keeping duplicates that originally differed in case and to sort
	   them correctly. */
	var_finish(&conf_prunefs);
	var_finish(&conf_prunenames);
	var_finish(&conf_prunepaths);
	gen_conf_block();
	string_list_dir_path_sort(&conf_prunepaths);

	if (conf_debug_pruning) {
		fprintf(stderr, "conf_block:\n");
		for (char c : conf_block) {
			if (isascii((unsigned char)c) && isprint((unsigned char)c) && c != '\\')
				putc(c, stderr);
			else {
				fprintf(stderr, "\\%03o", (unsigned)(unsigned char)c);
				if (c == 0)
					putc('\n', stderr);
			}
		}
		fprintf(stderr, "\n-----------------------\n");
	}
}
