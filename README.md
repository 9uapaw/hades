
# Table of Contents

1.  [Hades](#org25e1fb4)
    1.  [Overview](#orgaf19a24)
    2.  [Cluster types](#orgfa9ccca)
    3.  [Nomenclature](#org1a516b9)
    4.  [Getting started](#orgb86ca52)
    5.  [Init command](#org2fb1dab)
    6.  [Commands](#org57a8ed1)
    7.  [Selector](#org2b6e692)
        1.  [Type selector format](#org4ad6591)
        2.  [Complex selector format](#orgfdb587d)


<a id="org25e1fb4"></a>

# Hades


<a id="orgaf19a24"></a>

## Overview

Hades is a CLI tool, that shares a common interface between various Hadoop distributions. It is a collection of commands most frequently used by developers of Hadoop components.


<a id="orgfa9ccca"></a>

## Cluster types

Hades supports [Hadock](https://github.com/9uapaw/docker-hadoop-dev) and [CDP](https://www.cloudera.com/products/cloudera-data-platform.html) clusters out of the box, but custom distributions might be supported in the future.


<a id="org1a516b9"></a>

## Nomenclature

-   Service: Hadoop component inside a cluster (HDFS, Yarn etc..)
-   Role: A role type of a service instance (Resource Manager, Name Node, Data Node etc..)


<a id="orgb86ca52"></a>

## Getting started

1.  Clone the repository
2.  Install dependencies via
    
        pipenv install
3.  Or install dependencies globally
4.  Run init command
    
        ./cli.py init


<a id="org2fb1dab"></a>

## Init command

The init command generates the boilerplate of a Hades config file. It is possible to generate the cluster config depending on the cluster type (see &#x2013;help for more information on this)


<a id="org57a8ed1"></a>

## Commands
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Command                                                    | Description                                                             | Options                                                                       |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli usage [OPTIONS]                                 |   Prints the aggregated usage of Hades                                  |   -n, --no-wrap  Turns off the wrapping                                       |
|                                                            |                                                                         |   --help         Show this message and exit.                                  |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli compile [OPTIONS]                               |   Compiles hadoop modules                                               |   -c, --changed                   compiles only the changed modules           |
|                                                            |                                                                         |   -d, --deploy                    deploy the changed modules to cluster       |
|                                                            |                                                                         |   -n, --no-copy                   do not copy the compiled modules jar to     |
|                                                            |                                                                         |                                   hadoop jar path                             |
|                                                            |                                                                         |   -m, --module TEXT               adds a module to the aggregated compilation |
|                                                            |                                                                         |   -s, --single [YARN_UI2|HADOOP_DIST|RESOURCEMANAGER]                         |
|                                                            |                                                                         |                                   only compiles a single module               |
|                                                            |                                                                         |   --help                          Show this message and exit.                 |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli init [OPTIONS]                                  |   Initializes an empty config                                           |   --help  Show this message and exit.                                         |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli discover [OPTIONS]                              |   Discovers a cluster manifest file                                     |   -c, --cluster-type [ClouderaManager|Hadock|Standard|]                       |
|                                                            |                                                                         |                                   Sets the type of the cluster                |
|                                                            |                                                                         |   -h, --host TEXT                 set the Cloudera Manager host               |
|                                                            |                                                                         |   -u, --username TEXT             sets the username credential when           |
|                                                            |                                                                         |                                   communicating with Cloudera Manager         |
|                                                            |                                                                         |   -p, --password TEXT             sets the password credential when           |
|                                                            |                                                                         |                                   communicating with Cloudera Manager         |
|                                                            |                                                                         |   -d, --hadock-path TEXT          sets the Hadock repository path             |
|                                                            |                                                                         |   --help                          Show this message and exit.                 |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli log [OPTIONS] SELECTOR                          |   Read the logs of Hadoop roles                                         |   -f, --follow     whether to follow the logs file instead of just reading it |
|                                                            |                                                                         |   -t, --tail TEXT  only read the last N lines                                 |
|                                                            |                                                                         |   -g, --grep TEXT  only read the lines that have this substring               |
|                                                            |                                                                         |   --help           Show this message and exit.                                |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli distribute [OPTIONS] [SELECTOR]                 |   Distributes files to selected roles                                   |   -s, --source TEXT               path of local file to distribute to role    |
|                                                            |                                                                         |                                   hosts                                       |
|                                                            |                                                                         |   -d, --dest TEXT                 path of remote destination path on role     |
|                                                            |                                                                         |                                   hosts                                       |
|                                                            |                                                                         |   -m, --module [YARN_UI2|HADOOP_DIST|RESOURCEMANAGER]                         |
|                                                            |                                                                         |                                   name of hadoop module to replace            |
|                                                            |                                                                         |   --help                          Show this message and exit.                 |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli status [OPTIONS]                                |   Prints the status of cluster                                          |   --help  Show this message and exit.                                         |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli run-app [OPTIONS] [DISTRIBUTED_SHELL|MAPREDUCE] |   Runs an application on the defined cluster                            |   -c, --cmd TEXT    defines the command to run                                |
|                                                            |                                                                         |   -q, --queue TEXT  defines the queue to which the application will be        |
|                                                            |                                                                         |                     submitted                                                 |
|                                                            |                                                                         |   --help            Show this message and exit.                               |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli run-script [OPTIONS] SCRIPT                     |   Runs the selected Hades script file in script/ directory              |   --help  Show this message and exit.                                         |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli update-config [OPTIONS] [SELECTOR]              |   Update properties on a config file for selected roles                 |   -f, --file                                                                  |
|                                                            |                                                                         |                                   which config file to update                 |
|                                                            |                                                                         |   -p, --property TEXT             property name                               |
|                                                            |                                                                         |   -v, --value TEXT                property value                              |
|                                                            |                                                                         |   -s, --source TEXT               update the config from a local file         |
|                                                            |                                                                         |   -n, --no-backup                 do not create a backup file before making   |
|                                                            |                                                                         |                                   any change to the config file               |
|                                                            |                                                                         |   --help                          Show this message and exit.                 |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli get-config [OPTIONS] [SELECTOR]                 |   Prints the selected configuration file for selected roles             |   -f, --file                                                                  |
|                                                            |                                                                         |                                   which config file to read                   |
|                                                            |                                                                         |   --help                          Show this message and exit.                 |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli restart-role [OPTIONS] [SELECTOR]               |   Restarts a role                                                       |   --help  Show this message and exit.                                         |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli yarn [OPTIONS] COMMAND [ARGS]...                |   Yarn specific commands                                                |   --help  Show this message and exit.                                         |
|                                                            |                                                                         | Commands:                                                                     |
|                                                            |                                                                         |   info           Prints YARN scheduler info                                   |
|                                                            |                                                                         |   mutate-config  Mutates YARN queue configuration at runtime through YARN...  |
|                                                            |                                                                         |   queue          Prints YARN queues                                           |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli yarn queue [OPTIONS]                            |   Prints YARN queues                                                    |   --help  Show this message and exit.                                         |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli yarn info [OPTIONS]                             |   Prints YARN scheduler info                                            |   --help  Show this message and exit.                                         |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
| Usage: cli yarn mutate-config [OPTIONS]                    |   Mutates YARN queue configuration at runtime through YARN mutation API |   -p, --property TEXT  property name                                          |
|                                                            |                                                                         |   -q, --queue TEXT     queue name                                             |
|                                                            |                                                                         |   -v, --value TEXT     property value                                         |
|                                                            |                                                                         |   --help               Show this message and exit.                            |
|------------------------------------------------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------|
<a id="org2b6e692"></a>

## Selector

Many commands could be limited to ran on specified roles. The **selector** argument is &ldquo;&rdquo; by default, which means that all roles will be considered inside every services. The format of the selector expression:


<a id="org4ad6591"></a>

### Type selector format

**<service\_type>/<role\_type>**
Examples:

-   Yarn/ResourceManager
-   Yarn/NodeManager


<a id="orgfdb587d"></a>

### Complex selector format

**type=<service\_type>&name=<service\_name>/type=<role\_type>&name=<role\_name>**
Examples:

-   type=Yarn/name=ResourceManager-ACTIVE

