import click
from cas.config import DEBUG, CAS_ROOT, CAS_PLUGIN_DIR
from cas.log import enable_debug
from cas import CAS
from cas.files import DEFAULT_TYPE, types, get_type, InvalidFileType
import json
import os
from cas.util import load_plugin_dir

@click.group(name='cas')
@click.option('--debug', is_flag=True)
@click.option('--root', metavar='DIRECTORY')
@click.option('--plugins-dir', metavar='DIRECTORY', default=CAS_PLUGIN_DIR)
@click.pass_context
def main(ctx, debug, root, plugins_dir):
    if debug and not DEBUG:
        enable_debug()

    rootdir = CAS_ROOT or root

    if not rootdir:
        raise click.UsageError('must specify a CAS root directory, either using'
            ' the CAS_ROOT env var or the --root option.')
    elif (os.path.exists(rootdir) and not os.path.isdir(rootdir)):
        raise click.UsageError('"%s" is not a directory' % rootdir)
    elif os.path.isdir(rootdir) and os.listdir(rootdir) and not CAS.check(rootdir):
        raise click.UsageError('"%s" does not look like a valid CAS directory' % rootdir) 

    # load plugins
    load_plugin_dir(plugins_dir)

    ctx.obj = CAS(rootdir)

@click.command(name='add')
@click.argument('filename', nargs=-1, required=True)
@click.option('-t', '--type', metavar='TYPE', default=DEFAULT_TYPE)
@click.pass_obj
def add(storage, filename, type):
    valid_types = types()
    if type not in valid_types:
        raise click.UsageError('invalid type, must pass one of: %s' % ', '.join(valid_types))

    if not filename:
        raise click.UsageError('must pass one or more files to add')

    sums = set()

    for f in filename:
        try:
            sum = storage.add(f, type=get_type(type))
            sums.add(sum)
        except InvalidFileType, e:
            raise click.UsageError('file "%s" is not of type "%s"' % (f, type))

    for sum in sorted(list(sums)):
        click.echo(sum)

@click.command(name='rm')
@click.argument('checksum', nargs=-1, required=True)
@click.pass_obj
def rm(storage, checksum):
    if not checksum:
        raise click.UsageError('must pass one or more checksums to remove')

    for c in checksum:
        storage.remove(c)

@click.command(name='ls')
@click.pass_obj
def ls(storage):
    for sum in storage.list():
        click.echo(sum)

@click.command(name='path')
@click.argument('checksum', required=True)
@click.pass_obj
def path(storage, checksum):
    if not checksum:
        raise click.UsageError('must pass a checksum')

    if not storage.has_sum(checksum):
        raise click.UsageError("no such checksum '%s' in storage" % checksum)

    click.echo(storage.path(c))

@click.command(name='meta')
@click.pass_obj
def meta(storage):
    click.echo(json.dumps(storage.meta(), sort_keys=True, indent=2))

@click.command(name='match')
@click.argument('key', required=True)
@click.argument('value', metavar='REGEX', required=True)
@click.option('-e', '--exact', is_flag=True)
@click.pass_obj
def match(storage, key, value, exact):
    if exact:
        func = storage.equals
    else:
        func = storage.match
    for sum in func(key, value):
        click.echo(sum)

main.add_command(add)
main.add_command(rm)
main.add_command(ls)
main.add_command(path)
main.add_command(meta)
main.add_command(match)

if __name__ == '__main__':
    main()
