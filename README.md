
# Table of Contents

1.  [Hades](#orgcbd5ffc)
    1.  [Overview](#org4e0c21b)
    2.  [Cluster types](#orgbd98613)
    3.  [Terminology](#orgd09644a)
    4.  [Getting started](#orgc98591d)
    5.  [Usage](#orgd95d8b1)
    6.  [Init](#org60f14c5)
    7.  [Discover cluster definition](#org55ec266)
    8.  [Commands](#orgd9a8367)
    9.  [Selector](#orgfdefe4b)
        1.  [Type selector format](#org01de2b7)
        2.  [Complex selector format](#org3198744)
    10. [Cheat sheet](#org4d75791)
    11. [Hades Script](#org5b0f6c5)


<a id="orgcbd5ffc"></a>

# Hades


<a id="org4e0c21b"></a>

## Overview

Hades is a CLI tool, that shares a common interface between various Hadoop distributions. It is a collection of commands most frequently used by developers of Hadoop components.


<a id="orgbd98613"></a>

## Cluster types

Hades supports [Hadock](https://github.com/9uapaw/docker-hadoop-dev), [CDP](https://www.cloudera.com/products/cloudera-data-platform.html) and standard upstream distribution.


<a id="orgd09644a"></a>

## Terminology

-   Service: Hadoop component inside a cluster (HDFS, Yarn etc..)
-   Role: A role type of a service instance (Resource Manager, Name Node, Data Node etc..)


<a id="orgc98591d"></a>

## Getting started

1.  Clone the repository
2.  Install dependencies via
    
        pipenv install
3.  Or install dependencies globally
4.  Run init command
    
        ./cli.py init


<a id="orgd95d8b1"></a>

## Usage

    Usage: cli.py [OPTIONS] COMMAND [ARGS]...
    
    Options:
      -c, --config TEXT  path to config file
      --cluster TEXT     path to cluster manifest file
      -d, --debug        turn on DEBUG level logging
      --help             Show this message and exit.
    
    Commands:
      compile        Compiles hadoop modules
      discover       Discovers a cluster manifest file
      distribute     Distributes files to selected roles
      get-config     Prints the selected configuration file for selected roles
      init           Initializes an empty config
      log            Read the logs of Hadoop roles
      restart-role   Restarts a role
      run-app        Runs an application on the defined cluster
      run-script     Runs the selected Hades script file in script/ directory
      status         Prints the status of cluster
      update-config  Update properties on a config file for selected roles
      usage          Prints the aggregated usage of Hades
      yarn           Yarn specific commands


<a id="org60f14c5"></a>

## Init

The init command generates a default configuration file for Hades. The config consists of:

    {
        "defaultModules": [
            "hadoop-common",
            "hadoop-yarn-server-common",
            "hadoop-yarn-api"
        ],
        "hadoopJarPath": "",
        "compileCmd": "mvn package -Pdist -Dtar -Dmaven.javadoc.skip=true -DskipTests -fail-at-end",
        "cmdPrefix": "",
        "hadoopPath": ""
    }

-   defaultModules: when compiling specific module, always add these modules as well (this is necessary in order to successfully compile single modules)
-   hadoopJarPath: points to the location of built hadoop jars (usually in $HADOOP\_SOURCE\_ROOT/hadoop-dist/target/hadoop-$HADOOP\_VERSION/share)
-   compileCmd: the mvn command used for building hadoop from source
-   cmdPrefix: prefix of every command issued on the cluster (eg. sudo -u hdfs)
-   hadoopPath: Hadoop source path


<a id="org55ec266"></a>

## Discover cluster definition

Discover is used to generate the cluster definition file. An example Hadock cluster definition is:

    {
        "clusterType": "Hadock",
        "clusterName": "",
        "context":{
            "Hdfs": {
                "name": "HDFS-1",
                "roles": {
                    "HDFS-DATANODE-1": {
                        "type": "DataNode",
                        "host": "datanode"
                    }
                }
            },
            "Yarn": {
                "name": "YARN-1",
                "roles": {
                    "RESOURCEMANAGER-1": {
                        "type": "ResourceManager",
                        "host": "resourcemanager"
                    }
                }
            }
        }
    }

-   clusterType: Hadock or ClouderaManager
-   context: grouped by service type (currently supported Hdfs and Yarn)
-   roles: grouped by role name


<a id="orgd9a8367"></a>

## Commands

```
┌──────────────────────────────────────────────────────────┬──────────────────────────────────────────────────────────┬──────────────────────────────────────────────────────────┐
│ Command                                                  │ Description                                              │ Options                                                  │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli usage [OPTIONS]                               │   Prints the aggregated usage of Hades                   │   -n, --no-wrap  Turns off the wrapping                  │
│                                                          │                                                          │   --help         Show this message and exit.             │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli compile [OPTIONS]                             │   Compiles hadoop modules                                │   -c, --changed                   compiles only the      │
│                                                          │                                                          │ changed modules                                          │
│                                                          │                                                          │   -d, --deploy                    deploy the changed     │
│                                                          │                                                          │ modules to cluster                                       │
│                                                          │                                                          │   -n, --no-copy                   do not copy the        │
│                                                          │                                                          │ compiled modules jar to                                  │
│                                                          │                                                          │                                   hadoop jar path        │
│                                                          │                                                          │   -m, --module TEXT               adds a module to the   │
│                                                          │                                                          │ aggregated compilation                                   │
│                                                          │                                                          │   -s, --single                                           │
│                                                          │                                                          │ [YARN_UI2|HADOOP_DIST|RESOURCEMANAGER|YARN_COMMON]       │
│                                                          │                                                          │                                   only compiles a single │
│                                                          │                                                          │ module                                                   │
│                                                          │                                                          │   --help                          Show this message and  │
│                                                          │                                                          │ exit.                                                    │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli init [OPTIONS]                                │   Initializes an empty config                            │   --help  Show this message and exit.                    │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli discover [OPTIONS]                            │   Discovers a cluster manifest file                      │   -c, --cluster-type [ClouderaManager|Hadock|Standard|]  │
│                                                          │                                                          │                                   Sets the type of the   │
│                                                          │                                                          │ cluster                                                  │
│                                                          │                                                          │   -h, --host TEXT                 set the Cloudera       │
│                                                          │                                                          │ Manager host                                             │
│                                                          │                                                          │   -u, --username TEXT             sets the username      │
│                                                          │                                                          │ credential when                                          │
│                                                          │                                                          │                                   communicating with     │
│                                                          │                                                          │ Cloudera Manager                                         │
│                                                          │                                                          │   -p, --password TEXT             sets the password      │
│                                                          │                                                          │ credential when                                          │
│                                                          │                                                          │                                   communicating with     │
│                                                          │                                                          │ Cloudera Manager                                         │
│                                                          │                                                          │   -v, --version TEXT              sets the CM API        │
│                                                          │                                                          │ version                                                  │
│                                                          │                                                          │   -d, --hadock-path TEXT          sets the Hadock        │
│                                                          │                                                          │ repository path                                          │
│                                                          │                                                          │   --help                          Show this message and  │
│                                                          │                                                          │ exit.                                                    │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli log [OPTIONS] SELECTOR                        │   Read the logs of Hadoop roles                          │   -f, --follow     whether to follow the logs file       │
│                                                          │                                                          │ instead of just reading it                               │
│                                                          │                                                          │   -t, --tail TEXT  only read the last N lines            │
│                                                          │                                                          │   -g, --grep TEXT  only read the lines that have this    │
│                                                          │                                                          │ substring                                                │
│                                                          │                                                          │   -d, --download   download log to current directory     │
│                                                          │                                                          │   --help           Show this message and exit.           │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli distribute [OPTIONS] [SELECTOR]               │   Distributes files to selected roles                    │   -s, --source TEXT               path of local file to  │
│                                                          │                                                          │ distribute to role                                       │
│                                                          │                                                          │                                   hosts                  │
│                                                          │                                                          │   -d, --dest TEXT                 path of remote         │
│                                                          │                                                          │ destination path on role                                 │
│                                                          │                                                          │                                   hosts                  │
│                                                          │                                                          │   -m, --module                                           │
│                                                          │                                                          │ [YARN_UI2|HADOOP_DIST|RESOURCEMANAGER|YARN_COMMON]       │
│                                                          │                                                          │                                   name of hadoop module  │
│                                                          │                                                          │ to replace                                               │
│                                                          │                                                          │   --help                          Show this message and  │
│                                                          │                                                          │ exit.                                                    │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli status [OPTIONS]                              │   Prints the status of cluster                           │   --help  Show this message and exit.                    │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli run-app [OPTIONS]                             │   Runs an application on the defined cluster             │   -c, --cmd TEXT    defines the command to run           │
│ {DISTRIBUTED_SHELL|MAPREDUCE}                            │                                                          │   -q, --queue TEXT  defines the queue to which the       │
│                                                          │                                                          │ application will be                                      │
│                                                          │                                                          │                     submitted                            │
│                                                          │                                                          │   --help            Show this message and exit.          │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli run-script [OPTIONS] SCRIPT                   │   Runs the selected Hades script file in script/         │   --help  Show this message and exit.                    │
│                                                          │ directory                                                │                                                          │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli update-config [OPTIONS] [SELECTOR]            │   Update properties on a config file for selected roles  │   -f, --file                                             │
│                                                          │                                                          │                                   which config file to   │
│                                                          │                                                          │ update                                                   │
│                                                          │                                                          │   -p, --property TEXT             property name          │
│                                                          │                                                          │   -v, --value TEXT                property value         │
│                                                          │                                                          │   -s, --source TEXT               update the config from │
│                                                          │                                                          │ a local file                                             │
│                                                          │                                                          │   -n, --no-backup                 do not create a backup │
│                                                          │                                                          │ file before making                                       │
│                                                          │                                                          │                                   any change to the      │
│                                                          │                                                          │ config file                                              │
│                                                          │                                                          │   --help                          Show this message and  │
│                                                          │                                                          │ exit.                                                    │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli get-config [OPTIONS] [SELECTOR]               │   Prints the selected configuration file for selected    │   -f, --file                                             │
│                                                          │ roles                                                    │                                   which config file to   │
│                                                          │                                                          │ read                                                     │
│                                                          │                                                          │   --help                          Show this message and  │
│                                                          │                                                          │ exit.                                                    │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli restart-role [OPTIONS] [SELECTOR]             │   Restarts a role                                        │   --help  Show this message and exit.                    │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli yarn [OPTIONS] COMMAND [ARGS]...              │   Yarn specific commands                                 │   --help  Show this message and exit.                    │
│                                                          │                                                          │ Commands:                                                │
│                                                          │                                                          │   add-queue       Adds a YARN queue configuration at     │
│                                                          │                                                          │ runtime through YARN...                                  │
│                                                          │                                                          │   global-updates  Mutates YARN global configuration at   │
│                                                          │                                                          │ runtime through...                                       │
│                                                          │                                                          │   info            Prints YARN scheduler info             │
│                                                          │                                                          │   queue           Prints YARN queues                     │
│                                                          │                                                          │   raw-mutate      Mutates YARN configuration at runtime  │
│                                                          │                                                          │ through YARN...                                          │
│                                                          │                                                          │   remove-queue    Remove YARN queue at runtime through   │
│                                                          │                                                          │ YARN mutation API                                        │
│                                                          │                                                          │   update-queue    Mutates YARN queue configuration at    │
│                                                          │                                                          │ runtime through...                                       │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli yarn queue [OPTIONS]                          │   Prints YARN queues                                     │   --help  Show this message and exit.                    │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli yarn info [OPTIONS]                           │   Prints YARN scheduler info                             │   --help  Show this message and exit.                    │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli yarn update-queue [OPTIONS]                   │   Mutates YARN queue configuration at runtime through    │   -p, --property TEXT  property name                     │
│                                                          │ YARN mutation API                                        │   -q, --queue TEXT     queue name                        │
│                                                          │                                                          │   -v, --value TEXT     property value                    │
│                                                          │                                                          │   -d, --dry            dry run                           │
│                                                          │                                                          │   --help               Show this message and exit.       │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli yarn add-queue [OPTIONS]                      │   Adds a YARN queue configuration at runtime through     │   -p, --property TEXT  property name                     │
│                                                          │ YARN mutation API                                        │   -q, --queue TEXT     queue name                        │
│                                                          │                                                          │   -v, --value TEXT     property value                    │
│                                                          │                                                          │   -d, --dry            dry run                           │
│                                                          │                                                          │   --help               Show this message and exit.       │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli yarn remove-queue [OPTIONS]                   │   Remove YARN queue at runtime through YARN mutation API │   -q, --queue TEXT  queue name                           │
│                                                          │                                                          │   -d, --dry         dry run                              │
│                                                          │                                                          │   --help            Show this message and exit.          │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli yarn global-updates [OPTIONS]                 │   Mutates YARN global configuration at runtime through   │   -k, --key TEXT    key                                  │
│                                                          │ YARN mutation API                                        │   -v, --value TEXT  value                                │
│                                                          │                                                          │   -d, --dry         dry run                              │
│                                                          │                                                          │   --help            Show this message and exit.          │
├──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ Usage: cli yarn raw-mutate [OPTIONS]                     │   Mutates YARN configuration at runtime through YARN     │   -x, --xml TEXT  xml                                    │
│                                                          │ mutation API                                             │   --help          Show this message and exit.            │
└──────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────┘
```

<a id="orgfdefe4b"></a>

## Selector

Many commands could be limited to ran on specified roles. The **selector** argument is &ldquo;&rdquo; by default, which means that all roles will be considered inside every services. The format of the selector expression:


<a id="org01de2b7"></a>

### Type selector format

> **<service\_type>/<role\_type>**

Examples:

-   Yarn/ResourceManager
-   Yarn/NodeManager


<a id="org3198744"></a>

### Complex selector format

> **type=<service\_type>&name=<service\_name>/type=<role\_type>&name=<role\_name>**

Examples:

-   type=Yarn/name=ResourceManager-ACTIVE


<a id="org4d75791"></a>

## Cheat sheet

    hades init # init Hades config file
    hades --debug discover -c ClouderaManager -h http://CM_HOST:7180 -u USER -p PASSWORD # create a ClouderaManager cluster definition file
    hades --debug discover -c Hadock --hadock-path ~/Programming/docker/docker-hadoop-dev # create a Hadock cluster definition file based on local Github repository path
    
    hades compile -c -d # build only the changed modules and replace the jars on every hosts in the cluster
    hades -s RESOURCEMANAGER -d # build the hadoop-yarn-server-resourcemanager module and replace the jar on every hosts in the cluster
    
    hades distribute Yarn -s ./example_file -d /etc/example_file # copy the local example_file to every hosts that have a Yarn role running on
    hades distribute Yarn/ResourceManager -m RESOURCEMANAGER # copy the hadoop-yarn-server-resourcemanager jar file to every hosts that have a ResourceManager role running on
    
    hades log Yarn/NodeManager -f # tail follows every NodeManager logs
    hades restart-role Yarn # restart all Yarn roles
    
    hades run-app DISTRIBUTED_SHELL -c "-shell_command \"sleep 100\"" # runs a DistributedShell command
    hades update-config Yarn -f capacity-scheduler.xml -s capacity-scheduler-resourcemanager-1612514771.xml # update the capacity-scheduler.xml of all Yarn roles from a local file
    
    hades yarn queue # prints Yarn queues
    hades yarn info # prints the formatted scheduler information

    hades yarn update-queue -q "root.parent2.queue1" -p "state" -v "RUNNING"
    hades yarn remove-queue -q "root.parent2.queue1"
    hades yarn add-queue -q "root.parent2.queue1" -p "capacity" -v "100"
    hades yarn global-updates -k "yarn.scheduler.capacity.queue-mappings" -v "u:%user:parent1.%user"


The *--dry* can be used to generate xml(s) then the *raw-mutate* command can be used with the combined xml, example:
```
 hades yarn add-queue -q "root.a" -p "capacity" -v "10" -d
 hades yarn update-queue -q "root.default" -p "capacity" -v "90" -d
 hades yarn raw-mutate -x "$(cat <<-END
<sched-conf>
  <add-queue>
    <queue-name>root.a</queue-name>
    <params>
      <entry>
        <key>capacity</key>
        <value>10</value>
      </entry>
    </params>
  </add-queue>
  <update-queue>
    <queue-name>root.default</queue-name>
    <params>
      <entry>
        <key>capacity</key>
        <value>90</value>
      </entry>
    </params>
  </update-queue>
</sched-conf>
END
)"
```

<a id="org5b0f6c5"></a>

## Hades Script

All the functionalities could be accessed from a custom Python script. A Hades script needs to be adhere to the following:

-   Located inside script/ directory
-   Inherits HadesScriptBase

An example script script/test.py, that prints the cluster status:

    class TestScript(HadesScriptBase):
    
        def run(self):
            print(self.cluster.get_status())

which could be run as:

    hades run-script test

## Set up Hades on a cluster and run the Netty script
### 1. Install Python 3.9

Commands are copied from: https://computingforgeeks.com/install-latest-python-on-centos-linux/

```
sudo yum -y groupinstall "Development Tools"
sudo yum -y install openssl-devel bzip2-devel libffi-devel xz-devel
gcc --version
sudo yum -y install wget
wget https://www.python.org/ftp/python/3.9.13/Python-3.9.13.tgz
tar xvf Python-3.9.13.tgz
cd Python-3.9*/
./configure --enable-optimizations
sudo make altinstall
```

### 2. Make sure you have python and pip installed
```
ls -la /usr/local/bin/python3.9
python3.9 --version
pip3.9 --version
```

### 3. Upgrade pip
```
/usr/local/bin/python3.9 -m pip install --upgrade pip
```

### 4. Install pipenv
```
pip3.9 install --user pipenv
```

### 5. Install git
```
sudo yum install -y git 
```

### 6. Clone Hades
```
git clone https://github.com/szilard-nemeth/hades.git
```

### 7. Checkout branch (if required) 
```
git checkout netty4-finish
```

### 8. Install dependencies of Hades
```
cd ~/hades
pipenv install
```

### 9. Launch a pipenv shell
```
cd ~/hades
pipenv shell
```

# 10 . Create Hades working dir
```
mkdir ~/hades_working_dir
```

# 11. Generate initial Hades configuration
```
cd ~/hades_working_dir
python ~/hades/cli.py init
```

### 12. Discover cluster
```
CLUSTERHOST1=ccycloud-1.snemeth-netty.root.hwx.site
python ~/hades/cli.py discover -h $CLUSTERHOST1 -c Standard
```

### 13. Make sure cluster.json and config.json files are generated correctly
```
find ~/hades_working_dir -maxdepth 1 -iname "*.json"
```

### 14. Make sure that the json config files are indented / formatted
```
python -m json.tool ~/hades_working_dir/config.json > /tmp/config.json && cp /tmp/config.json ~/hades_working_dir/config.json
python -m json.tool ~/hades_working_dir/cluster.json > /tmp/cluster.json && cp /tmp/cluster.json ~/hades_working_dir/cluster.json
```

### 15. Clone Hadoop repo
```
cd ~
git clone https://github.com/apache/hadoop.git
```

### 16. Edit config.json to point to the Hadoop repo
```
jq -c '.hadoopPath = $newHadoopPath' --arg newHadoopPath '/home/systest/hadoop' ~/hades_working_dir/cluster.json > /tmp/config.json && mv /tmp/config.json ~/hades_working_dir/config.json
```


### 17. Install tmux and create new tmux session
```
sudo yum install tmux -y
tmux
```

### 18. Start pipenv shell again
```
cd ~/hades && pipenv shell
```

NOTE: The remaining steps are specific to the netty4.py script

### 19. scp the patch file or create patch file link (from your local machine)
```
scp `readlink ~/netty4patch.patch` ccycloud.snemeth-testing.root.hwx.site:netty4patch.patch
```

### 20. Make sure that the cluster can communicate with the testing cluster (for all machines)
```
ssh root@ccycloud-1.snemeth-netty.root.hwx.site
exit
ssh root@ccycloud-2.snemeth-netty.root.hwx.site
exit
ssh root@ccycloud-3.snemeth-netty.root.hwx.site
exit
ssh root@ccycloud-4.snemeth-netty.root.hwx.site
exit

scp root@ccycloud-1.snemeth-netty.root.hwx.site:/opt/hadoop/etc/hadoop/yarn-site.xml /tmp/yarn-site1.xml
scp root@ccycloud-2.snemeth-netty.root.hwx.site:/opt/hadoop/etc/hadoop/yarn-site.xml /tmp/yarn-site2.xml
scp root@ccycloud-3.snemeth-netty.root.hwx.site:/opt/hadoop/etc/hadoop/yarn-site.xml /tmp/yarn-site3.xml
scp root@ccycloud-4.snemeth-netty.root.hwx.site:/opt/hadoop/etc/hadoop/yarn-site.xml /tmp/yarn-site4.xml
```

### 21. Run the Netty script
```
cd ~/hades_working_dir/
python ~/hades/cli.py -d run-script -s netty4
```
