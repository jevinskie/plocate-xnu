project('plocate', 'cpp', default_options: ['buildtype=debugoptimized','cpp_std=c++17'], version: '1.0.4-pre')

# Make the version available as a #define.
add_project_arguments('-DPLOCATE_VERSION="' + meson.project_version() + '"', language: 'cpp')

cxx = meson.get_compiler('cpp')
uringdep = dependency('liburing', required: false)
zstddep = dependency('libzstd')
threaddep = dependency('threads')

if not uringdep.found()
	add_project_arguments('-DWITHOUT_URING', language: 'cpp')
endif

executable('plocate', ['plocate.cpp', 'io_uring_engine.cpp', 'turbopfor.cpp', 'parse_trigrams.cpp', 'serializer.cpp', 'access_rx_cache.cpp', 'needle.cpp'],
	dependencies: [uringdep, zstddep, threaddep],
	install: true,
	install_mode: ['rwxr-sr-x', 'root', 'mlocate'])
executable('plocate-build', 'plocate-build.cpp',
	dependencies: [zstddep],
	install: true,
	install_dir: get_option('sbindir'))

conf_data = configuration_data()
conf_data.set('PROCESSED_BY_MESON', '1')
conf_data.set('sbindir', get_option('prefix') + '/' + get_option('sbindir'))
conf_data.set('locategroup', get_option('locategroup'))
update_script = configure_file(input: 'update-plocate.sh',
               output: 'update-plocate.sh',
               configuration: conf_data)

if get_option('install_cron')
	install_data(update_script,
		install_dir: '/etc/cron.daily',
		rename: 'plocate')
endif
install_man('plocate.1')
install_man('plocate-build.8')

# Requires having TurboPFor checked out, so not built by default.
# Unless you have a recent Meson, there's no apparently good way
# of calling into that directory to run make, and we're not
# replicating their build system, so it has to be manual.
pfordir = meson.source_root() + '/TurboPFor-Integer-Compression'
if run_command('[', '-r', pfordir + '/libic.a', ']').returncode() == 0
	turbopfordep = declare_dependency(
		include_directories: include_directories('TurboPFor-Integer-Compression'),
		dependencies: meson.get_compiler('cpp').find_library('ic', dirs: pfordir))
	executable('bench', ['bench.cpp', 'io_uring_engine.cpp', 'turbopfor.cpp'],
		dependencies: [uringdep, turbopfordep],
		build_by_default: false,
		install: false)
endif
