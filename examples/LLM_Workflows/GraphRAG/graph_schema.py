"""
Code courtesy of the FalkorDB.
"""


# collect graph's schema
def graph_schema(g):
    schema = {}

    # ---------------------------------------------------------------------------
    # process nodes
    # ---------------------------------------------------------------------------

    nodes = {}

    q = "CALL db.labels()"
    labels = [x[0] for x in g.query(q).result_set]

    for label in labels:
        nodes[label] = {}
        nodes[label]["attributes"] = {}

        q = f"MATCH (n:{label}) RETURN n LIMIT 50"
        result = g.query(q).result_set

        for row in result:
            node = row[0]
            for attr in node.properties:
                val = node.properties[attr]
                if attr not in nodes[label]["attributes"]:
                    nodes[label]["attributes"][attr] = {"type": type(val).__name__}

    schema["nodes"] = nodes

    # ---------------------------------------------------------------------------
    # process relations
    # ---------------------------------------------------------------------------

    edges = {}

    q = "CALL db.relationshiptypes()"
    rels = [x[0] for x in g.query(q).result_set]

    for r in rels:
        edges[r] = {}
        edges[r]["attributes"] = {}

        q = f"MATCH ()-[e:{r}]->() RETURN e LIMIT 50"
        result = g.query(q).result_set

        for row in result:
            edge = row[0]
            for attr in edge.properties:
                val = edge.properties[attr]
                if attr not in edges[r]["attributes"]:
                    edges[r]["attributes"][attr] = {"type": type(val).__name__}

        edges[r]["connects"] = []

        # collect edge endpoints
        for src in labels:
            for dest in labels:
                q = f"MATCH (:{src})-[:{r}]->(:{dest}) RETURN 1 LIMIT 1"
                res = g.query(q).result_set
                if len(res) == 1:
                    edges[r]["connects"].append((src, dest))

    schema["edges"] = edges

    return schema
