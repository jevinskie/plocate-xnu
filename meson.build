project('plocate', 'cpp', default_options: ['buildtype=debugoptimized','cpp_std=c++17'], version: '1.0.0-pre')

cxx = meson.get_compiler('cpp')
uringdep = dependency('liburing', required: false)
zstddep = dependency('libzstd')

if not uringdep.found()
	add_project_arguments('-DWITHOUT_URING', language: 'cpp')
endif

executable('plocate', ['plocate.cpp', 'io_uring_engine.cpp', 'turbopfor.cpp', 'parse_trigrams.cpp'],
	dependencies: [uringdep, zstddep],
	install: true,
	install_mode: ['rwxr-sr-x', 'root', 'mlocate'])
executable('plocate-build', 'plocate-build.cpp',
	dependencies: [zstddep],
	install: true,
	install_dir: get_option('sbindir'))

install_data('update-plocate.sh',
	install_dir: '/etc/cron.daily',
	rename: 'plocate')

# Requires having TurboPFor checked out, so not built by default.
# Unless you have a recent Meson, there's no apparently good way
# of calling into that directory to run make, and we're not
# replicating their build system, so it has to be manual.
pfordir = meson.source_root() + '/TurboPFor-Integer-Compression'
if run_command('[', '-r', pfordir + '/libic.a', ']').returncode() == 0
	turbopfordep = declare_dependency(
		include_directories: include_directories('TurboPFor-Integer-Compression'),
		dependencies: meson.get_compiler('cpp').find_library('ic', dirs: pfordir))
	executable('bench', ['bench.cpp', 'io_uring_engine.cpp'],
		dependencies: [uringdep, turbopfordep],
		build_by_default: false,
		install: false)
endif
