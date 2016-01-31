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

def remove_over_query(engine, toponame, query, function):
    with engine.begin() as conn:
        res = conn.execute(query)
        counter = 0
        for edge_id in [i[0] for i in res.fetchall()]:
            removed = function(conn, toponame, edge_id)

            if removed:
                counter += 1
                secho("{}".format(edge_id),fg='red', nl=False, bold=True)
            else:
                secho(".",fg='green', nl=False, bold=True)
        echo("")
        echo("Removed {} of {} edges".format(counter,res.rowcount))

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

    remove_over_query(engine, topo_schema, "SELECT edge_id FROM {}.edge_data".format(topo_schema),remove_edge)
    remove_over_query(engine, topo_schema, "SELECT node_id FROM {}.node".format(topo_schema),remove_node)

if __name__ == '__main__':
    cli()

