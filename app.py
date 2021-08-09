import os
from json import dumps
import logging
import re

from flask import Flask, g, Response, request, jsonify
from flask_cors import CORS
from neo4j import GraphDatabase, basic_auth

app = Flask(__name__, static_url_path='/static/')
CORS(app)

url = os.getenv("NEO4J_URI", "bolt://localhost:7687")
username = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "coaDB_2021")
neo4jVersion = os.getenv("NEO4J_VERSION", "4.2.6")
database = os.getenv("NEO4J_DATABASE", "wappendatenbank")

port = os.getenv("PORT", 8080)

driver = GraphDatabase.driver(url, auth=basic_auth(username, password))


def get_db():
    if not hasattr(g, 'neo4j_db'):
        if neo4jVersion.startswith("4"):
            g.neo4j_db = driver.session(database=database)
        else:
            g.neo4j_db = driver.session()
    return g.neo4j_db


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'neo4j_db'):
        g.neo4j_db.close()


@app.route("/locations/upsertLocation", methods=["POST"])
def upsertLocation():
    uuid = request.json["UUID"]
    name = request.json["name"]
    parent = request.json["parent"]

    db = get_db()
    results = ""

    # node does not exist
    if uuid == "":
        if parent == "":
            results = db.write_transaction(lambda tx : tx.run( "CREATE (loc:Location {uuid: randomUUID(), name: $name}) "
                                                    "RETURN loc.uuid AS UUID", 
                                                    {"name": name}).single())
        else:
            results = db.write_transaction(lambda tx : tx.run( "MATCH (parent:Location) "
                                                    "WHERE parent.uuid = $parent "
                                                    "CREATE (parent)-[:HAS_CHILD]->(loc:Location {uuid: randomUUID(), name: $name}) "
                                                    "RETURN loc.uuid AS UUID",
                                                    {"parent": parent, "name": name}).single())
    # node already exists, update name
    else:
        results = db.write_transaction(lambda tx : tx.run("MATCH (loc:Location) " 
                                                    "WHERE loc.uuid = $uuid "
                                                    "SET loc.name = $name "
                                                    "RETURN loc.uuid AS UUID",
                                                    {"name": name, "uuid": uuid}).single())

    return jsonify(results["UUID"])


@app.route("/locations/deleteLocation", methods=["GET"])
def deleteLocation():
    uuid = request.args["uuid"]

    db = get_db()
    results = db.write_transaction(lambda tx : tx.run("MATCH (loc:Location)-[:HAS_CHILD*0..]->(child:Location) "
                                                    "WHERE loc.uuid = $uuid "
                                                    "DETACH DELETE child "
                                                    "RETURN count(child) AS Number",
                                                    {"uuid": uuid}).single())
    return jsonify(results["Number"])


@app.route("/terms/upsertTerm", methods=["POST"])
def upsertTerm():
    uuid = request.json["uuid"]
    parent = request.json["parent"]
    term = request.json["term"]

    db = get_db()
    result = ""

    if uuid == "":
        if parent == "":
            result = db.write_transaction(lambda tx : tx.run("CREATE (t:Term {uuid: randomUUID()}) "
                                                        "SET t += $attributes "
                                                        "RETURN t.uuid AS UUID",
                                                        {"attributes": term}).single())
        else:
            result = db.write_transaction(lambda tx : tx.run("MATCH (parent:Term) "
                                                        "WHERE parent.uuid = $parent "
                                                        "CREATE (parent)-[:NEXT_TERM]->(t:Term {uuid: randomUUID()}) "
                                                        "SET t += $attributes "
                                                        "RETURN t.uuid AS UUID",
                                                        {"parent": parent, "attributes": term}).single())
    else:
        result = db.write_transaction(lambda tx : tx.run("MATCH (t:Term) "
                                                        "WHERE t.uuid = $uuid "
                                                        "SET t += $attributes "
                                                        "RETURN t.uuid AS UUID",
                                                        {"uuid": uuid, "attributes": term}).single())

    return jsonify(result["UUID"])

@app.route("/terms/addTermRelationship", methods=["GET"])
def addTermRelationship():
    parent = request.args["parent"]
    child = request.args["child"]

    db = get_db()
    result = db.write_transaction(lambda tx : tx.run("MATCH (parent:Term), (child:Term) "
                                                    "WHERE parent.uuid = $parent AND child.uuid = $child "
                                                    "CREATE (parent)-[:NEXT_TERM]->(child)",
                                                    {"parent": parent, "child": child}))

    return ("", 204)

@app.route("/terms/removeTermRelationship", methods=["GET"])
def removeTermRelationship():
    parent = request.args["parent"]
    child = request.args["child"]

    db = get_db()
    result = db.write_transaction(lambda tx : tx.run("MATCH (parent:Term)-[r:NEXT_TERM]->(child:Term) "
                                                    "WHERE parent.uuid = $parent AND child.uuid = $child "
                                                    "AND NOT (parent)<-[:CONTAINS_TERM]-(:Chain)-[:CONTAINS_TERM]->(child) "
                                                    "DELETE r "
                                                    "RETURN count(r) AS NumberDeleted",
                                                    {"parent": parent, "child": child}).single())
    
    return (jsonify(result["NumberDeleted"]))

@app.route("/terms/deleteTerm", methods=["GET"])
def deleteTerm():
    termUUID = request.args["termUUID"]

    db = get_db()
    result = db.write_transaction(lambda tx : tx.run("MATCH (term:Term) "
                                                    "WHERE term.uuid = $termUUID "
                                                    "WITH term, size((term)<-[:CONTAINS_TERM]-()) AS numberChains, size((term)-[:NEXT_TERM]->()) AS numberTerms "
                                                    "CALL apoc.do.when( "
                                                    "   numberChains = 0 AND numberTerms = 0,"
                                                    "   'DETACH DELETE term RETURN numberChains, numberTerms',"
                                                    "   'RETURN numberChains, numberTerms',"
                                                    "   {term: term, numberChains: numberChains, numberTerms: numberTerms}"    
                                                    ") YIELD value "
                                                    "RETURN value.numberChains AS Chains, value.numberTerms AS Terms",
                                                    {"termUUID": termUUID}).single())
    
    if result != None:
        resultDict = {"Chains": result["Chains"], "Terms": result["Terms"]}
        return jsonify(resultDict)
    else:
        return ("Begriff existiert nicht", 400)


@app.route("/coa/upsertCoA", methods=["POST"])
def upsertCoA():
    uuid = request.json["uuid"]
    location = request.json["location"]
    coa = request.json["coa"]

    db = get_db()
    result = ""

    if uuid == "":
        result = db.write_transaction(lambda tx : tx.run("OPTIONAL MATCH (loc:Location) "
                                                    "WHERE loc.uuid = $location "
                                                    "CREATE (coa:CoA {uuid: randomUUID()}) "
                                                    "SET coa += $attributes "
                                                    "WITH coa, loc "
                                                    "CALL apoc.do.when("
                                                    "   loc IS NOT NULL,"
                                                    "   'CREATE (coa)-[:AT_LOCATION]->(loc) RETURN coa',"
                                                    "   '',"
                                                    "   {coa: coa, loc: loc}"
                                                    "   ) YIELD value "
                                                    "RETURN value.coa.uuid AS UUID ",
                                                    {"location": location, "attributes": coa}).single())
    else:
        result = db.write_transaction(lambda tx : tx.run("MATCH (coa:CoA) "
                                                    "WHERE coa.uuid = $uuid "
                                                    "OPTIONAL MATCH (loc:Location) "
                                                    "WHERE loc.uuid = $location "
                                                    "OPTIONAL MATCH (coa)-[r:AT_LOCATION]->(old:Location) "
                                                    "SET coa += $attributes "
                                                    "WITH coa, loc, r, old "
                                                    "CALL apoc.do.case(["
                                                    "   (r IS NULL OR old IS NULL) AND loc IS NOT NULL,"
                                                    "   'CREATE (coa)-[:AT_LOCATION]->(loc) RETURN coa',"
                                                    "   r IS NOT NULL AND old IS NOT NULL AND loc is NULL,"
                                                    "   'DELETE r RETURN coa',"
                                                    "   loc.uuid <> old.uuid,"
                                                    "   'DELETE r CREATE (coa)-[:AT_LOCATION]->(loc) RETURN coa'],"
                                                    "   'RETURN coa',"
                                                    "   {coa: coa, loc: loc, r: r, old: old}"
                                                    "   ) YIELD value "
                                                    "RETURN value.coa.uuid AS UUID",
                                                    {"uuid": uuid, "location": location, "attributes": coa}).single())

    return jsonify(result["UUID"])

@app.route("/coa/deleteCoA", methods=["GET"])
def deleteCoA():
    coa = request.args["termUUID"]

    db = get_db()
    if coa != "":
        db.write_transaction(lambda tx : tx.run("MATCH (coa:CoA) "
                                                    "WHERE coa.uuid = $coa "
                                                    "MATCH (chain:Chain)<-[:HAS_CHAIN]-(coa) "
                                                    "DETACH DELETE chain "
                                                    "DETACH DELETE coa ",
                                                    {"coa": coa}))
    
    return ("", 204)


@app.route("/chain/insertChains", methods=["POST"])
def insertChains():
    coa = request.json["coa"]
    chains = request.json["chains"]

    db = get_db()
    result = ""

    if coa != "":
        result = db.write_transaction(lambda tx : tx.run("MATCH (coa:CoA) "
                                                    "WHERE coa.uuid = $coa "
                                                    "UNWIND $chains AS chains "
                                                    "CREATE (coa)-[hc:HAS_CHAIN {order: chains.order}]->(cha:Chain {uuid: randomUUID()}) "
                                                    "WITH chains, cha "
                                                    "UNWIND chains.terms AS terms "
                                                    "MATCH (term:Term) "
                                                    "WHERE term.uuid = terms.uuid "                                                                                                     
                                                    "CREATE (cha)-[ct:CONTAINS_TERM {order: terms.order}]->(term) "
                                                    "RETURN collect(DISTINCT cha.uuid) AS UUIDs",
                                                    {"coa": coa, "chains": chains}).single())

    return jsonify(result["UUIDs"])

@app.route("/chain/deleteChains", methods=["POST"])
def deleteChains():
    coa = request.json["coa"]
    chains = request.json["chains"]

    db = get_db()
    result = 0

    if coa != "":
        result = db.write_transaction(lambda tx : tx.run("MATCH (coa:CoA) "
                                                    "WHERE coa.uuid = $coa "
                                                    "MATCH (coa)-[:HAS_CHAIN]->(chain:Chain) "
                                                    "WHERE chain.uuid in $chains "
                                                    "WITH coa, count(chain) AS NumberDeleted, collect(chain) AS chains "
                                                    "UNWIND chains AS chain "
                                                    "DETACH DELETE chain "
                                                    "WITH coa, NumberDeleted "
                                                    "MATCH (coa)-[hc:HAS_CHAIN]->(chain:Chain) "
                                                    "WITH DISTINCT hc, NumberDeleted "
                                                    "ORDER BY hc.order "
                                                    "WITH collect(hc) AS remainingChains, NumberDeleted "
                                                    "FOREACH (c IN remainingChains | SET c.order = apoc.coll.indexOf(remainingChains, c)) "
                                                    "RETURN NumberDeleted",
                                                    {"coa": coa, "chains": chains}).single())
    
    if not result is None:
        return jsonify(result["NumberDeleted"])
    else:
        return jsonify(0)




def serialize_term (term):
    return {
        'uuid': term['uuid'],
        'name': term['name'],
        'synonyms': term['synonyms'],
        'hide': term['hide'],
        'comment': term['comment'],
        'children': [],
        'parents': []
    }


@app.route("/termeditor/firstTerms", methods=["GET"])
def firstTerms():
    db = get_db()

    queryResult = db.read_transaction(lambda tx : list(tx.run("MATCH (start:Term) "
                                                        "WHERE NOT (:Term)-[:NEXT_TERM]->(start) "
                                                        "OPTIONAL MATCH (start)-[:NEXT_TERM]->(child:Term) "
                                                        "RETURN start, collect(child) AS children "
                                                        "ORDER BY start.name")))
    
    resultList = []
    newRecord = None

    for record in queryResult:
        newRecord = serialize_term(record["start"])
        newRecord["children"] = [serialize_term(child) for child in record["children"]]
        resultList.append(newRecord)
        
    return jsonify(resultList)


@app.route("/termeditor/updateListsOfClicked", methods=["GET"])
def updateListsOfClicked():
    uuid = request.args["uuid"]
    mode = int(request.args["mode"])
    db = get_db()

    queryResult = None
    if mode == 1:
        queryResult = db.read_transaction(lambda tx : tx.run("MATCH (clicked:Term) "
                                                                "WHERE clicked.uuid = $uuid "
                                                                "OPTIONAL MATCH (clicked)-[:NEXT_TERM]->(childOfClicked:Term) "
                                                                "RETURN collect(childOfClicked) AS newItems ",                                                           
                                                                {"uuid": uuid}).single())
    elif mode == 0:
        queryResult = db.read_transaction(lambda tx : tx.run("MATCH (clicked:Term) "
                                                                "WHERE clicked.uuid = $uuid "
                                                                "OPTIONAL MATCH (parentOfClicked:Term)-[:NEXT_TERM]->(clicked) "
                                                                "RETURN collect(parentOfClicked) AS newItems",                                                           
                                                                {"uuid": uuid}).single())
    else:
        return ("Request misses mode", 400)                                                           
    
    return jsonify([serialize_term(newItem) for newItem in queryResult["newItems"]])


@app.route("/termeditor/allTerms", methods=["GET"])
def allTerms():
    db = get_db()
    queryResult = db.read_transaction(lambda tx : list(tx.run("MATCH (term:Term) "
                                                            "RETURN term "
                                                            "ORDER BY term.name")))
    
    return jsonify([serialize_term(term["term"]) for term in queryResult])


def serialize_coa (coa):
    return {
        'uuid': coa['uuid'],
        'name': coa['name'],
        'description': coa['description'],
        'location': coa['location'],
        'containedChains': []
    }

def serialize_chain (chain):
    return {
        'uuid': chain['uuid'],
        'containedTerms': []
    }

@app.route("/coaeditor/allCoA", methods=["GET"])
def allCoA():
    db = get_db()

    queryResult = db.read_transaction(lambda tx : list(tx.run("MATCH (coa:CoA)-[hc:HAS_CHAIN]->(chain:Chain) "
                                                                "WITH coa, chain ORDER BY hc.order "
                                                                "WITH coa, collect(chain) AS chains "
                                                                "UNWIND chains AS chain "
                                                                "OPTIONAL MATCH (chain)-[ct:CONTAINS_TERM]->(term:Term) "
                                                                "WITH coa, chain, term ORDER BY ct.order "
                                                                "WITH coa, chain{.*, terms: collect(term)} "
                                                                "return coa, collect(chain) AS chains")))
    
    resultList = []   

    for resultRecord in queryResult:
        newRecord = serialize_coa(resultRecord["coa"])
        for indexChain, chain in enumerate(resultRecord["chains"]):         
            newChain = serialize_chain({"uuid": chain["uuid"]})
            newChain["containedTerms"] = [{"position": indexTerm, "term": serialize_term(term)} for indexTerm, term in enumerate(chain["terms"])]
            newRecord["containedChains"].append({"position": indexChain, "chain": newChain})
        resultList.append(newRecord)

    queryResult = db.read_transaction(lambda tx : list(tx.run("MATCH (coa:CoA) "
                                                                "WHERE NOT (coa)-[:HAS_CHAIN]->(:Chain) "
                                                                "RETURN coa")))
    for resultRecord in queryResult:
        newRecord = serialize_coa(resultRecord["coa"])
        resultList.append(newRecord)
        
    return jsonify(resultList)



if __name__ == '__main__':
    logging.info('Running on port %d, database is at %s', port, url)
    app.run(port=port)