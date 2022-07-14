from dbf_query import dbf_query
from tableToRdf import tableToRdf
from postProcess import postProcess
import os
import datetime 
from rdflib import Graph, Namespace, URIRef
from rdflib.plugins.stores.memory import Memory
from rdflib.graph import ConjunctiveGraph
from rdflib.namespace import OWL, PROV
import time
#import urllib.request

#request_url = urllib.request.urlopen('https://annegoebels.github.io/')
#print(request_url.read())

start = time.time()

now = datetime.datetime.now()
date_string = now.strftime('%Y-%m-%d')

ANS = Namespace("https://w3id.org/asbingowl/core#")
AONS = Namespace("https://w3id.org/asbingowl/keys/2013#")
ANSK = Namespace("https://w3id.org/asbingowl/keys#")
AOI = Namespace ("https://w3id.org/aoi#")
OPM = Namespace("https://w3id.org/opm#")
SCHEMA = Namespace("https://schema.org/")
DOT = Namespace("https://w3id.org/dot#")

idOnt = URIRef("https://w3id.org/asbingowl/core")
idAoi = URIRef("https://w3id.org/aoi")
idOld = URIRef("https://w3id.org/asbingowl/keys/2013")
idNew = URIRef("https://w3id.org/asbingowl/keys")

#aoi = Graph(store = store, identifier = idAoi)
#aoi.parse (r"C:\GitHub\TwinGen\DataResources\AOI_ontology.ttl",format = "ttl")


p = r"C:\Users\Anne\Desktop\cab\next"
cabs = os.listdir(p)

for sibbwfile in cabs:
    store = Memory()
    g = ConjunctiveGraph(store=store)

    g.bind("owl", OWL)
    g.bind("prov", PROV)
    g.bind("asb", ANS)
    g.bind("asbkey13", AONS)
    g.bind("asbkey", ANSK)
    g.bind("aoi", AOI)
    g.bind("opm", OPM)
    g.bind("schema", SCHEMA)
    g.bind("dot", DOT)

    ont = Graph(store=store, identifier=idOld)
    ont.parse(r"C:\GitHub\TwinGen\Ontologies\ASB_Ontology_Core.ttl", format="ttl")
    # ont.parse("https://annegoebels.github.io/ontology.ttl")
    # ont.parse("http://www.w3.org/People/Berners-Lee/card")
    #

    old = Graph(store=store, identifier=idOld)
    old.parse(r"C:\GitHub\TwinGen\Ontologies\oldKeys.ttl", format="ttl")
    # old.parse("https://annegoebels.github.io/SubOntOldKeys.ttl")
    # print(len(old))

    new = Graph(store=store, identifier=idNew)
    new.parse(r"C:\GitHub\TwinGen\Ontologies\newKeys.ttl", format="ttl")
    # new.parse("https://annegoebels.github.io/SubOntNewKeys.ttl")


    file_name, file_extension = os.path.splitext(sibbwfile)
    print(file_name)
    
    path = os.path.join(p,sibbwfile)
    
    bw_nr, containsBauteile = dbf_query(path , g)
    print(bw_nr)
    print(containsBauteile)
    #bw_nr = "7936662"
    #containsBauteile = True


    INS = Namespace("http://example.org/sibbw"+bw_nr+"#")
    g.bind("", INS)
    idIns = URIRef("http://example.org/sibbw" + bw_nr + "")
    
    rdfGraph = tableToRdf(g, bw_nr, store, INS, idIns)
    rdfGraph.serialize(destination= r"C:\GitHub\TwinGen\OutputGraphs\SIB"+bw_nr+"_"+date_string+"_Graph1.ttl")
    print("first graph done")
    #rdfGraph = Graph()
    #rdfGraph.parse(r"C:\GitHub\TwinGen\OutputGraphs\SIB7936662_2022-07-14_Graph1.ttl", format="ttl")

    extendedGraph = postProcess(g, rdfGraph, bw_nr, INS, containsBauteile)
    extendedGraph.serialize(destination= r"C:\GitHub\TwinGen\OutputGraphs\SIB"+bw_nr+"_"+date_string+".ttl")

end = time.time()
duration = round(end-start, 2)
print("Dauer:", duration / 60, "Minuten")


