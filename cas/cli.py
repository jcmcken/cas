import click
from cas.config import DEBUG, CAS_ROOT
from cas.log import enable_debug
from cas import CAS
import json
import os

@click.group(name='cas')
@click.option('--debug', is_flag=True)
@click.option('--root', metavar='DIRECTORY')
@click.pass_context
def main(ctx, debug, root):
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

    ctx.obj = CAS(rootdir)

@click.command(name='add')
@click.argument('filename', nargs=-1)
@click.pass_obj
def add(storage, filename):
    if not filename:
        raise click.UsageError('must pass one or more files to add')

    for f in filename:
        storage.add(f)

@click.command(name='rm')
@click.argument('checksum', nargs=-1)
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
@click.argument('checksum', nargs=-1)
@click.pass_obj
def path(storage, checksum):
    if not checksum:
        raise click.UsageError('must pass one or more checksums')

    for c in checksum:
        click.echo(storage.path(c))

@click.command(name='meta')
@click.pass_obj
def meta(storage):
    click.echo(json.dumps(storage.meta(), sort_keys=True, indent=2))

main.add_command(add)
main.add_command(rm)
main.add_command(ls)
main.add_command(path)
main.add_command(meta)

if __name__ == '__main__':
    main()
