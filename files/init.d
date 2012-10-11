#!/sbin/runscript

# This is auto generated file.
# If you need to change it, please edit pgherd configuration file at: {}
# Then run pgherd init_script

depend() {
    use net
    use postgres
    provide pgherd
}

checkconfig() {

}

start() {

    checkconfig || return 1

    ebegin "Starting pgherd"

    start-stop-daemon --start --user {} --group {} --pid-file {}

    eend $retval
}