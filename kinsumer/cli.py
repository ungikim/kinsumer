""":mod:`kinsumer.cli` --- Daemon based command line runner
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
import os
import os.path
import signal
import sys
import traceback
from typing import Optional

import click
import lockfile
from daemon import DaemonContext
from daemon.pidfile import TimeoutPIDLockFile
from typeguard import typechecked

from . import __version__


class NoConsumerException(click.UsageError):
    pass


def read_pidfile(path):
    try:
        with open(path, 'r') as f:
            return int(f.read())
    except Exception as e:
        raise ValueError(e.message)


def prepare_import(path):
    path = os.path.realpath(path)
    if os.path.splitext(path)[1] == '.py':
        path = os.path.splitext(path)[0]
    if os.path.basename(path) == '__init__':
        path = os.path.dirname(path)
    module_name = []
    while True:
        path, name = os.path.split(path)
        module_name.append(name)
        if not os.path.exists(os.path.join(path, '__init__.py')):
            break
    if sys.path[0] != path:
        sys.path.insert(0, path)
    return '.'.join(module_name[::-1])


def find_best_consumer(consumer_module):
    from . import Consumer
    for attr_name in ('consumer', 'app', 'application'):
        consumer = getattr(consumer_module, attr_name, None)
        if isinstance(consumer, Consumer):
            return consumer

    matches = [
        v for k, v in iter(consumer_module.__dict__) if isinstance(v, Consumer)
    ]

    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        raise NoConsumerException(
            'Auto detected consumer in module "{module}".'.format(
                module=consumer_module.__name__))
    raise NoConsumerException(
        'Failed to find consumer in module "{module}"'.format(
            module=consumer_module.__name__))


def locate_consumer(module_name, raise_if_not_found=True):
    # __traceback_hide__ = True

    try:
        __import__(module_name)
    except ImportError:
        if sys.exc_info()[-1].tb_next:
            raise NoConsumerException(
                'While importing "{name}", an ImportError was raised:'
                '\n\n{tb}'.format(name=module_name, tb=traceback.format_exc())
            )
        elif raise_if_not_found:
            raise NoConsumerException(
                'Could not import "{name}"."'.format(name=module_name)
            )
        else:
            return

    imported_module = sys.modules[module_name]
    return find_best_consumer(imported_module)


@click.command(help="""\
This shell command acts as daemon for Kinesis Consumer

Example usage:

\b
    kinsumer example.py
""")
@click.argument('consumer', type=click.Path(exists=True, resolve_path=True))
@click.option('--foreground', '-f', is_flag=True, help="""\
Run in the foreground  (Default if no pid file is specified)\
""")
@click.option('--debug', '-d', is_flag=True, help="""\
Run a consumer in debug mode\
""")
@click.option('--pidfile', '-p', type=click.Path(exists=False), help="""\
Use a PID file\
""")
@click.option('--stop', is_flag=True, help="""\
Sends SIGTERM to the running daemon (needs --pidfile)\
""")
@click.version_option(__version__)
@click.pass_context
@typechecked
def cli(ctx, consumer: str, foreground: bool, debug: bool,
        pidfile: Optional[str], stop: bool):
    if stop:
        if not pidfile or not os.path.exists(pidfile):
            click.echo('PID file not specified or not found', color=ctx.color)
            ctx.exit(1)
        try:
            pid = read_pidfile(pidfile)
        except ValueError as e:
            click.echo('Could not read PID file: {0}'.format(e),
                       color=ctx.color)
            ctx.exit(1)
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError as e:
            click.echo('Could not send SIGTERM: {0}'.format(e),
                       color=ctx.color)
            ctx.exit(1)
        ctx.exit(0)
    if not pidfile and not foreground:
        click.echo('No PID file specified; running in foreground')
        foreground = True
    daemon_kwargs = {}
    if foreground:
        daemon_kwargs.update({'detach_process': False,
                              'stdin': sys.stdin,
                              'stdout': sys.stdout,
                              'stderr': sys.stderr})
    if pidfile:
        pidfile = os.path.abspath(pidfile) \
            if not os.path.isabs(pidfile) else pidfile
        if os.path.exists(pidfile):
            pid = read_pidfile(pidfile)
            if not os.path.exists(os.path.join('/proc', str(pid))):
                click.echo("""\
Deleting obsolete PID file (process {0} does not exist)
""".format(pid))
                os.unlink(pidfile)
        daemon_kwargs['pidfile'] = TimeoutPIDLockFile(pidfile, 5)
    context = DaemonContext(**daemon_kwargs)
    try:
        context.open()
    except lockfile.LockTimeout:
        click.echo("""\
Could not acquire lock on pid file {0}\
Check if the daemon is already running.\
""".format(pidfile))
        ctx.exit(1)
    except KeyboardInterrupt:
        click.echo()
        ctx.exit(1)
    with context:
        import gevent
        from gevent import monkey
        monkey.patch_all()
        gevent.reinit()
        gevent.wait(timeout=1)
        import_name = prepare_import(consumer)
        consumer = locate_consumer(import_name)
        consumer.process(debug=debug)


def main(as_module: bool = False) -> None:
    args = sys.argv[1:]

    if as_module:
        this_module = 'kinsumer'
        name = 'python -m ' + this_module
        sys.argv = ['-m', this_module] + args
    else:
        name = None
    cli(args=args, prog_name=name)


if __name__ == '__main__':
    main(as_module=True)
