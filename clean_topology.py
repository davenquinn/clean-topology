#!/usr/bin/env python

import click
from click import echo, secho
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ArgumentError,InternalError

def remove_node(conn, toponame, node_id):
    try:
        A = conn.execute("SELECT abs((getnodeedges(%s, %s)).edge)", (toponame, node_id))
        if A.rowcount == 2:
            (edge1_id,) = A.fetchone()
            (edge2_id,) = A.fetchone()
            if edge1_id != edge2_id:
                conn.execute("SELECT ST_ModEdgeHeal(%s, %s, %s)", (toponame, edge1_id, edge2_id))
                removed = True
        elif A.rowcount == 0:
            conn.execute("SELECT ST_RemIsoNode(%s, %s)", (toponame, node_id))
            removed = True
        else:
            removed = False
    except InternalError as e:
        removed = False
    return removed

def remove_edge(conn,toponame,edge_id):
    try:
        conn.execute("SELECT ST_RemEdgeModFace(%s, %s)", (toponame, edge_id))
        secho("{}".format(edge_id),fg='red', nl=False, bold=True)
        return True
    except InternalError as e:
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

@click.command()
@click.argument("db")
@click.argument("topo_schema")
def cli(db, topo_schema):
    try:
        engine = create_engine(db)
    except ArgumentError:
        engine = create_engine("postgresql://localhost/"+db)

    with engine.begin() as conn:
        res = conn.execute("SELECT name FROM topology WHERE name = %s",(topo_schema,))
        if res.rowcount != 1:
            raise Exception("Topology matching the name was not found")

    with engine.begin() as conn:
        edges = conn.execute("SELECT edge_id FROM {}.edge_data".format(topo_schema))
        results = show_query_results(remove_edge(conn, topo_schema, edge_id) for edge_id, in edges)
        echo("Removed {} of {} edges".format(*n_true(results)))

    with engine.begin() as conn:
        nodes = conn.execute("SELECT node_id FROM {}.node".format(topo_schema))
        results = show_query_results(remove_node(conn, topo_schema, node_id)
                for node_id, in nodes)
        echo("Removed {} of {} nodes".format(*n_true(results)))

if __name__ == '__main__':
    cli()

