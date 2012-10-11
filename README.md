pgerd
==============

This is full automated PostgreSQL 9.0+ streaming replication cluster manager.

Status: not ready

### Functionality:
* auto discovery cluster configuration
* command line cluster manager
* cluster nodes automatic configuration with master and slave support
* cluster status monitoring with automatic failover

### How it works
On every PostgreSQL node you need to run small pgherd daemon which will be responsible for monitoring local PostgreSQL
node and communication with other pgherd daemons.

pgherd deamons will communicate each other in two ways:
* udp broadcast messages which are used for automatic cluster configuration
* tcp/ip socket connection which is used for local node status propagation and failover detection, it is also used for
new master negotiation when current master goes down.

### Installation

Currently only simple bash installation script is provided, which will install pgherd and all dependencies in python
virtualenv. Whole installation is done via git so the virtualenv and git tools are required.

To install simply call:
   wget https://raw.github.com/mprzytulski/pgherd/master/files/install.sh
   bash ./install.sh <installation_directory>

This script will:
* create <installation_directory>
* create virtualenv <installation_directory>/env
* install all required dependencies into virtualenv
* clone current version of pgherd from github repository into a <installation_directory>/pgherd directory
* create symbolic links of <installation_directory>/pgherd/pgherdd and <installation_directory>/pgherd/pgherd into /usr/sbin/
* copy sample pgherd.conf into /etc/pgherd.conf
* create /var/log/pgherd and /var/run/pgherd directories

By default pgherd will be run as postgres user, it is possible to change it, but it is needed to use account which will
have right to:
* read and write permissions to pg_data directory and all tablespaces if there are any
* read and write permissions to postgresql.conf and pg_hba.conf file
* write permissions to trigger_file directory

### On roadmap
* sentry monitoring integration
* pgpool2 integration
* web gui manager
