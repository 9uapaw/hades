
# Table of Contents

1.  [Hades](#orga61df08)
    1.  [Overview](#orgedbb972)
    2.  [Cluster types](#org0784c9c)
    3.  [Nomenclature](#orgb83803a)
    4.  [Getting started](#org98fcab1)
    5.  [Init command](#org10afd48)
    6.  [Commands](#org9e4842c)
    7.  [Selector](#org617e946)
        1.  [Type selector format](#orgc8e95a4)
        2.  [Complex selector format](#org9b75e90)


<a id="orga61df08"></a>

# Hades


<a id="orgedbb972"></a>

## Overview

Hades is a CLI tool, that shares a common interface between various Hadoop distributions. It is a collection of commands most frequently used by developers of Hadoop components.


<a id="org0784c9c"></a>

## Cluster types

Hades supports [Hadock](https://github.com/9uapaw/docker-hadoop-dev) and [CDP](https://www.cloudera.com/products/cloudera-data-platform.html) clusters out of the box, but custom distributions might be supported in the future.


<a id="orgb83803a"></a>

## Nomenclature

-   Service: Hadoop component inside a cluster (HDFS, Yarn etc..)
-   Role: A role type of a service instance (Resource Manager, Name Node, Data Node etc..)


<a id="org98fcab1"></a>

## Getting started

1.  Clone the repository
2.  Install dependencies via
    
        pipenv install
3.  Or install dependencies globally
4.  Run init command
    
        ./cli.py init


<a id="org10afd48"></a>

## Init command

The init command generates the boilerplate of a Hades config file. It is possible to generate the cluster config depending on the cluster type (see &#x2013;help for more information on this)


<a id="org9e4842c"></a>

## Commands

<table border="2" cellspacing="0" cellpadding="6" rules="groups" frame="hsides">


<colgroup>
<col  class="org-left" />

<col  class="org-left" />

<col  class="org-left" />

<col  class="org-left" />
</colgroup>
<thead>
<tr>
<th scope="col" class="org-left">Command</th>
<th scope="col" class="org-left">Description</th>
<th scope="col" class="org-left">Hadock</th>
<th scope="col" class="org-left">CDP</th>
</tr>
</thead>

<tbody>
<tr>
<td class="org-left">compile</td>
<td class="org-left">Compiles hadoop modules</td>
<td class="org-left">IMPLEMENTED</td>
<td class="org-left">IMPLEMENTED</td>
</tr>


<tr>
<td class="org-left">distribute</td>
<td class="org-left">Distributes files to selected roles</td>
<td class="org-left">NOT IMPLEMENTED</td>
<td class="org-left">NOT IMPLEMENTED</td>
</tr>


<tr>
<td class="org-left">init</td>
<td class="org-left">Initializes config file</td>
<td class="org-left">IMPLEMENTED</td>
<td class="org-left">NOT IMPLEMENTED</td>
</tr>


<tr>
<td class="org-left">log</td>
<td class="org-left">Read the logs of Hadoop roles</td>
<td class="org-left">IMPLEMENTED</td>
<td class="org-left">NOT IMPLEMENTED</td>
</tr>


<tr>
<td class="org-left">restart-role</td>
<td class="org-left">Restart a role</td>
<td class="org-left">IMPLEMENTED</td>
<td class="org-left">NOT IMPLEMENTED</td>
</tr>


<tr>
<td class="org-left">run-app</td>
<td class="org-left">Runs an application on the defined cluster</td>
<td class="org-left">IMPLEMENTED</td>
<td class="org-left">NOT IMPLEMENTED</td>
</tr>


<tr>
<td class="org-left">run-script</td>
<td class="org-left">Runs the selected Hades script file in  script/ directory</td>
<td class="org-left">IMPLEMENTED</td>
<td class="org-left">IMPLEMENTED</td>
</tr>


<tr>
<td class="org-left">status</td>
<td class="org-left">Prints the status of cluster</td>
<td class="org-left">IMPLEMENTED</td>
<td class="org-left">NOT IMPLEMENTED</td>
</tr>


<tr>
<td class="org-left">update-config</td>
<td class="org-left">Update properties on a config file for  selected roles</td>
<td class="org-left">IMPLEMENTED</td>
<td class="org-left">NOT IMPLEMENTED</td>
</tr>


<tr>
<td class="org-left">yarn</td>
<td class="org-left">Yarn specific commands</td>
<td class="org-left">IMPLEMENTED</td>
<td class="org-left">NOT IMPLEMENTED</td>
</tr>
</tbody>
</table>


<a id="org617e946"></a>

## Selector

Many commands could be limited to ran on specified roles. The **selector** argument is "" by default, which means that all roles will be considered inside every services. The format of the selector expression:


<a id="orgc8e95a4"></a>

### Type selector format

**<service\_type>/<role\_type>**
Examples:

-   Yarn/ResourceManager
-   Yarn/NodeManager


<a id="org9b75e90"></a>

### Complex selector format

**type=<service\_type>&name=<service\_name>/type=<role\_type>&name=<role\_name>**
Examples:

-   type=Yarn/name=ResourceManager-ACTIVE

