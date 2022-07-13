#!/usr/bin/env python3

import logging
import time
from os import path
from typing import List, Tuple

import click
from rich import print as rich_print, box
from rich.table import Table
from rich.tree import Tree

from core.config import Config, ClusterConfig
from hadoop.action import RoleAction
from hadoop.app.example import Application
from hadoop.cluster_type import ClusterType
from core.context import HadesContext
from core.error import HadesException, ConfigSetupException, CliArgException
from core.handler import MainCommandHandler
from hadoop.xml_config import HadoopConfigFile
from hadoop.yarn.rm_api import RmApi
from hadoop.yarn.yarn_mutation import MutationRequest
from hadoop_dir.module import HadoopModule

logger = logging.getLogger(__name__)


@click.group()
@click.option('-c', '--config', default='config.json', help='path to config file')
@click.option('--cluster', default='cluster.json', help='path to cluster manifest file')
@click.option('-d', '--debug', is_flag=True, help='turn on DEBUG level logging')
@click.option('-p', '--prefix', help='overwrite command prefix')
@click.pass_context
def cli(ctx, config: str, cluster: str, debug: bool, prefix: str):
    if ctx.invoked_subcommand == "usage":
        return

    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=level)
    sh_log = logging.getLogger("sh")
    sh_log.setLevel(logging.CRITICAL)
    ctx.ensure_object(dict)

    logger.info("Invoked command {}".format(ctx.invoked_subcommand))

    if ctx.invoked_subcommand == "init":
        hades_ctx = HadesContext(config_path=config, cluster_config_path=cluster)
        ctx.obj['handler'] = MainCommandHandler(hades_ctx)
        return

    if not path.exists(config):
        raise ConfigSetupException("Config file does not exist. Create config with 'init' subcommand.")

    with open(config) as file:
        json_str = file.read()
        config_file = Config.from_json(json_str)

    if path.exists(cluster):
        with open(cluster) as file:
            json_str = file.read()
            cluster_file = ClusterConfig.from_json(json_str)
    else:
        cluster_file = None

    if prefix:
        config_file.cmd_prefix = prefix

    context = HadesContext(config=config_file, cluster_config=cluster_file, config_path=config, cluster_config_path=cluster)
    ctx.obj['handler'] = MainCommandHandler(context)


@cli.command()
@click.option('-n', '--no-wrap', is_flag=True, help='Turns off the wrapping')
def usage(no_wrap: bool = False):
    """
    Prints the aggregated usage of Hades
    """
    table = Table(title="Hades CLI", show_lines=True, box=box.SQUARE)
    table.add_column("Command")
    table.add_column("Description")
    table.add_column("Options", no_wrap=no_wrap)

    def recursive_help(cmd, parent=None, is_root: bool = False):
        ctx = click.core.Context(cmd, info_name=cmd.name, parent=parent)
        commands = getattr(cmd, 'commands', {})
        help = list(filter(bool, cmd.get_help(ctx).split("\n")))
        if is_root:
            command = help[0]
            cmd_id = help.index("Commands:")
            desc = "\n".join(help[2:cmd_id])
            options = "\n".join(help[cmd_id + 1:])
        else:
            command = help[0]
            desc = help[1]
            options = "\n".join(help[3:])
            table.add_row(command, desc, options)

        for sub in commands.values():
            recursive_help(sub, ctx)

    recursive_help(cli, is_root=True)
    rich_print(table)


@cli.command()
@click.pass_context
@click.option('-c', '--changed', is_flag=True, help='compiles only the changed modules')
@click.option('-d', '--deploy', is_flag=True, help='deploy the changed modules to cluster')
@click.option('-n', '--no-copy', default=False, is_flag=True, help='do not copy the compiled modules jar to hadoop jar path')
@click.option('-m', '--module', multiple=True, help='adds a module to the aggregated compilation')
@click.option('-s', '--single', type=click.Choice([m.name for m in HadoopModule]), help='only compiles a single module')
def compile(ctx, changed: bool, deploy: bool, module: List[str], no_copy: bool, single: str):
    """
    Compiles hadoop modules
    """
    handler: MainCommandHandler = ctx.obj['handler']
    all_modules = []
    single_module = HadoopModule[single] if single else None
    if module:
        all_modules.extend(handler.ctx.config.default_modules)
        all_modules.extend(module)

    handler.compile(changed, deploy, modules=all_modules, no_copy=no_copy, single=single_module)


@cli.command()
@click.pass_context
def init(ctx):
    """
    Initializes an empty config
    """
    handler: MainCommandHandler = ctx.obj['handler']
    config_path = handler.ctx.config_path
    if not path.exists(config_path):
        with open(config_path, 'w') as f:
            f.write(Config().to_json())

        logger.info("Initialized config file {}".format(config_path))
    else:
        logger.info("Config already exists")


@cli.command()
@click.pass_context
@click.option('-c', '--cluster-type', type=click.Choice([n.value for n in ClusterType], case_sensitive=False), help='Sets the type of the cluster', required=True)
@click.option('-h', '--host', help='set the Cloudera Manager/ResourceManager API host')
@click.option('-u', '--username', default="admin", help='sets the username credential')
@click.option('-p', '--password', default="admin", help='sets the password credential')
@click.option('-v', '--version', default="v40", help='sets the CM API version')
@click.option('-d', '--hadock-path', help='sets the Hadock repository path')
def discover(ctx, cluster_type: str or None, host: str or None, username: str or None, password: str or None, hadock_path: str or None, version: str):
    """
    Discovers a cluster manifest file
    """
    ctx: HadesContext = ctx.obj['handler'].ctx
    ctx.cluster_config = ClusterConfig()
    if cluster_type == ClusterType.CM.value:
        ctx.cluster_config.specific_context['username'] = username
        ctx.cluster_config.specific_context['host'] = host
        ctx.cluster_config.specific_context['password'] = password
        ctx.cluster_config.specific_context['version'] = version
    elif cluster_type == ClusterType.HADOCK.value:
        ctx.cluster_config.specific_context['hadock_path'] = hadock_path
    elif cluster_type == ClusterType.STANDARD.value:
        ctx.cluster_config.specific_context['host'] = host

    ctx.cluster_config.cluster_type = cluster_type
    handler: MainCommandHandler = MainCommandHandler(ctx)
    handler.discover()


@cli.command()
@click.pass_context
@click.argument('selector')
@click.option('-f', '--follow', is_flag=True, help='whether to follow the logs file instead of just reading it')
@click.option('-t', '--tail', default=None, help='only read the last N lines')
@click.option('-g', '--grep', default=None, help='only read the lines that have this substring')
@click.option('-d', '--download', is_flag=True, default=None, help='download log to current directory')
def log(ctx, selector: str, follow: bool, tail: int or None, grep: str or None, download: bool):
    """
    Read the logs of Hadoop roles
    """
    handler: MainCommandHandler = ctx.obj['handler']
    handler.log(selector, follow, tail, grep, download)


@cli.command()
@click.argument("selector", default="")
@click.option('-s', '--source', multiple=True, help='path of local file to distribute to role hosts')
@click.option('-d', '--dest', multiple=True, help='path of remote destination path on role hosts')
@click.option('-m', '--module', type=click.Choice([m.name for m in HadoopModule]),
              multiple=True, help='name of hadoop module to replace')
@click.pass_context
def distribute(ctx, selector: str, source: Tuple[str], dest: Tuple[str], module: Tuple[str]):
    """
    Distributes files to selected roles
    """
    handler: MainCommandHandler = ctx.obj['handler']
    files = {k: v for k, v in zip(source, dest)}
    module = [HadoopModule[m].value for m in module]
    handler.distribute(selector, files, module)


@cli.command()
@click.pass_context
def status(ctx):
    """
    Prints the status of cluster
    """
    handler: MainCommandHandler = ctx.obj['handler']
    handler.print_status()
    handler.print_cluster_metrics()


@cli.command()
@click.pass_context
@click.argument('app', type=click.Choice([n.name for n in Application], case_sensitive=False))
@click.option('-c', '--cmd', help='defines the command to run')
@click.option('-q', '--queue', help='defines the queue to which the application will be submitted')
def run_app(ctx, app: str, cmd: str = None, queue: str = None):
    """
    Runs an application on the defined cluster
    """
    handler: MainCommandHandler = ctx.obj['handler']
    handler.run_app(app, cmd, queue)


@cli.command()
@click.pass_context
@click.argument('script')
def run_script(ctx, script: str):
    """
    Runs the selected Hades script file in script/ directory
    """
    handler: MainCommandHandler = ctx.obj['handler']
    handler.run_script(script)


@cli.command()
@click.pass_context
@click.argument('selector', default="")
@click.option('-f', '--file', type=click.Choice([n.value for n in HadoopConfigFile]), required=True, help='which config file to update')
@click.option('-p', '--property', multiple=True, help='property name')
@click.option('-v', '--value', multiple=True, help='property value')
@click.option('-s', '--source', help='update the config from a local file')
@click.option('-n', '--no-backup', is_flag=True, help='do not create a backup file before making any change to the config file')
def update_config(ctx, selector: str, file: str, property: Tuple[str], value: Tuple[str], no_backup: bool = False,
                  source: str = None):
    """
    Update properties on a config file for selected roles
    """
    if len(property) != len(value):
        raise CliArgException("All property must map to a value. Properties: {} Values: {}".format(len(property), len(value)))

    handler: MainCommandHandler = ctx.obj['handler']
    file = HadoopConfigFile(file)
    handler.update_config(selector, file, list(property), list(value), no_backup, source)


@cli.command()
@click.pass_context
@click.argument('selector', default="")
@click.option('-f', '--file', type=click.Choice([n.value for n in HadoopConfigFile]), required=True, help='which config file to read')
def get_config(ctx, selector: str, file: str):
    """
    Prints the selected configuration file for selected roles
    """
    handler: MainCommandHandler = ctx.obj['handler']
    handler.print_config(selector, HadoopConfigFile(file))


@cli.command()
@click.pass_context
@click.argument('selector', default="")
def restart_role(ctx, selector: str):
    """
    Restarts a role
    """
    handler: MainCommandHandler = ctx.obj['handler']
    handler.role_action(selector, RoleAction.RESTART)


@cli.group()
@click.pass_context
def yarn(ctx):
    """
    Yarn specific commands
    """
    pass


@yarn.command()
@click.pass_context
def queue(ctx):
    """
    Prints YARN queues
    """
    handler: MainCommandHandler = ctx.obj['handler']
    handler.print_queues()


@yarn.command()
@click.pass_context
def info(ctx):
    """
    Prints YARN scheduler info
    """
    handler: MainCommandHandler = ctx.obj['handler']
    handler.print_scheduler_info()


def run_mutation(ctx, mutation: MutationRequest, dry: bool):
    handler: MainCommandHandler = ctx.obj['handler']
    if dry:
        print(mutation.dump_xml(pretty=True))
    else:
        handler.mutate_yarn_config(mutation.dump_xml())


@yarn.command()
@click.pass_context
@click.option('-p', '--property', multiple=True, help='property name')
@click.option('-q', '--queue', help='queue name')
@click.option('-v', '--value', multiple=True, help='property value')
@click.option('-d', '--dry', is_flag=True, default=False, help='dry run')
def update_queue(ctx, property: Tuple[str], value: Tuple[str], queue: str, dry: bool):
    """
    Mutates YARN queue configuration at runtime through YARN mutation API
    """
    mutation = MutationRequest()
    mutation.update_queue(queue, **{k: v for k, v in zip(property, value)})
    run_mutation(ctx, mutation, dry)


@yarn.command()
@click.pass_context
@click.option('-p', '--property', multiple=True, help='property name')
@click.option('-q', '--queue', help='queue name')
@click.option('-v', '--value', multiple=True, help='property value')
@click.option('-d', '--dry', is_flag=True, default=False, help='dry run')
def add_queue(ctx, property: Tuple[str], value: Tuple[str], queue: str, dry: bool):
    """
    Adds a YARN queue configuration at runtime through YARN mutation API
    """
    mutation = MutationRequest()
    mutation.add_queue(queue, **{k: v for k, v in zip(property, value)})
    run_mutation(ctx, mutation, dry)

@yarn.command()
@click.pass_context
@click.option('-q', '--queue', help='queue name')
@click.option('-d', '--dry', is_flag=True, default=False, help='dry run')
def remove_queue(ctx, queue: str, dry: bool):
    """
    Remove YARN queue at runtime through YARN mutation API
    """
    mutation = MutationRequest()
    mutation.remove_queue(queue)
    run_mutation(ctx, mutation, dry)


@yarn.command()
@click.pass_context
@click.option('-k', '--key', help='key')
@click.option('-v', '--value', help='value')
@click.option('-d', '--dry', is_flag=True, default=False, help='dry run')
def global_updates(ctx, key: str, value: str, dry: bool):
    """
    Mutates YARN global configuration at runtime through YARN mutation API
    """
    mutation = MutationRequest()
    mutation.global_update(key, value)
    run_mutation(ctx, mutation, dry)


@yarn.command()
@click.pass_context
@click.option('-x', '--xml', help='xml')
def raw_mutate(ctx, xml: str):
    """
    Mutates YARN configuration at runtime through YARN mutation API
    """
    handler: MainCommandHandler = ctx.obj['handler']
    handler.mutate_yarn_config(xml)

if __name__ == "__main__":
    logger.info("Started application")
    before = time.time()
    try:
        cli()
        after = time.time()
        logger.info("Executed successfully after {}s".format(int(after - before)))
    except HadesException as e:
        logger.error(str(e))
        after = time.time()
        logger.info("Error during execution after {}s".format(int(after - before)))
