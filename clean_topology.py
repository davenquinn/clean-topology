import click
from click import get_current_context
from click import echo, secho
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ArgumentError, InternalError, OperationalError
from functools import wraps

def get_connection():
    ctx = get_current_context()
    return ctx.conn

def remove_node(toponame, node_id):
    c = get_connection()
    with c.begin() as trans:
        removed = False
        try:
            A = c.execute("SELECT abs((getnodeedges(%s, %s)).edge)", (toponame, node_id))
            if A.rowcount == 2:
                (edge1_id,) = A.fetchone()
                (edge2_id,) = A.fetchone()
                if edge1_id != edge2_id:
                    c.execute("SELECT ST_ModEdgeHeal(%s, %s, %s)", (toponame, edge1_id, edge2_id))
                    removed = str(node_id)
            elif A.rowcount == 0:
                c.execute("SELECT ST_RemIsoNode(%s, %s)", (toponame, node_id))
                removed = str(node_id)
            trans.commit()
        except InternalError as e:
            trans.rollback()
            removed = False
        return removed

def remove_edge(toponame,edge_id):
    c = get_connection()
    with c.begin() as trans:
        try:
            c.execute("SELECT ST_RemEdgeModFace(%s, %s)", (toponame, edge_id))
            secho("{}".format(edge_id),fg='red', nl=False, bold=True)
            trans.commit()
            return str(edge_id)
        except InternalError as e:
            trans.rollback()
            return False

def show_query_results(iterable):
    """
    Provides a graphical representation on the CLI of a large number
    of operations. If the returned value is truthy, a representation of
    the value is printed. If False, the record is represented as a dot.
    """
    for val in iterable:
        if val:
            secho("{}".format(val),fg='red', nl=False, bold=True)
        else:
            secho(".",fg='green', nl=False, bold=True)
        yield val
    echo("")

def n_true(values):
    n = total = 0
    for i in values:
        if i: n += 1
        total += 1
    return n,total

def validate_database(ctx, param, value):
    try:
        engine = create_engine(value)
    except ArgumentError:
        engine = create_engine("postgresql://localhost/"+value)

    try:
        engine.begin()
    except OperationalError as err:
        raise click.BadParameter(err.orig.message)
    ctx.engine = engine
    ctx.conn = engine.connect()
    return engine

def validate_topology(ctx,param,value):
    res = ctx.conn.execute("SELECT name FROM topology WHERE name = %s",(value,))
    if res.rowcount != 1:
        raise click.BadParameter("Topology {} was not found".format(value))
    else:
        return value

@click.command()
@click.option("--db",callback=validate_database,
        required=True, help="Database name or connection string")
@click.option("--topology", callback=validate_topology, required=True,
        help="Name of a PostGIS topology schema")
@click.pass_context
def cli(ctx, db, topology):
    """
    Script to clean disused nodes and edges of a PostGIS topology
    """
    c = get_connection()
    edges = c.execute("SELECT edge_id FROM {}.edge_data".format(topology))
    results = show_query_results(remove_edge(topology, edge_id) for edge_id, in edges)
    echo("Removed {} of {} edges".format(*n_true(results)))

    nodes = c.execute("SELECT node_id FROM {}.node".format(topology))
    results = show_query_results(remove_node(topology, node_id)
            for node_id, in nodes)
    echo("Removed {} of {} nodes".format(*n_true(results)))
    c.close()

if __name__ == '__main__':
    cli()

