import click


@click.group(short_help="csvtocsvw CLI.")
def csvtocsvw():
    """csvtocsvw CLI."""
    pass


@csvtocsvw.command()
@click.argument("name", default="csvtocsvw")
def command(name):
    """Docs."""
    click.echo("Hello, {name}!".format(name=name))


def get_commands():
    return [csvtocsvw]
