def tableToRdf ():


    import sqlite3
    import random
    from rdflib import Graph, Literal, Namespace, URIRef, BNode
    from rdflib.namespace import OWL, RDF, RDFS, XSD
    from rdflib.plugins.sparql import prepareQuery
    from rdflib.plugins.sparql.parser import AskQuery

    #get connection to database
    conn = sqlite3.connect ("ASBING.db")
    c=conn.cursor()

    #create graph
    graph =Graph()
    INS = Namespace("http://example.org/sibbw#")
    ANS = Namespace("https://w3id.org/asbingowl/core#")
    AONS = Namespace("https://w3id.org/asbingowl/keys/2013#")
    ANSK = Namespace("https://w3id.org/asbingowl/keys#")


    graph.bind("", INS)
    graph.bind("owl", OWL)
    graph.bind("asb",  ANS)
    graph.bind("asbkey13",  AONS)
    graph.bind("asbkey",  ANSK)

    #connection to ontology (currnently local, later to online version)
    ont = Graph()
    ont.parse(r"C:\GitHub\TwinGen\Ontologies\ASB_Ontology_Merged.ttl", format = "turtle")

    ont.bind("", INS)
    ont.bind("owl", OWL)
    ont.bind("asb",  ANS)
    ont.bind("asbkey13",  AONS)
    ont.bind("asbkey",  ANSK)

    #functions
    def randomnumber(N):
        minimum = pow(10, N-1)
        maximum = pow(10, N) - 1
        id = random.randint(minimum, maximum)
        return "N_"+str(id)

    def getAsbClass(name):
        cl_query = prepareQuery(
        """select
        ?class
        where 
        {?class rdfs:label ?literal.
        FILTER(STRSTARTS(STR(?class), "https://w3id.org/asbingowl/core#"))}
        """)
        for cl in ont.query (cl_query, initBindings={'literal':name}):
            return cl[0]
                
    def getAsbProp(name):
        LitName = Literal(str(name).replace(" ",""), lang='de')
        prop_query = prepareQuery(
        """select ?prop ?type ?range
            where
            {?prop rdfs:label ?literal.
            ?prop rdf:type ?type.
            ?prop rdfs:range ?range.}
        """)
        for prop in ont.query (prop_query, initBindings={'literal':LitName}):
            return prop

    def getKeyClass(keynumber):
        valLit = Literal(keynumber)
        key_query = prepareQuery(
        """select ?keyclass ?newkeyclass
        where 
        {?keyclass asbkey13:Kennung ?kennung
        OPTIONAL { ?keyclass owl:sameAs ?newkeyclass.}
        }
        """, initNs= {"asbkey13": AONS, "owl":OWL})
        for keycl in ont.query (key_query, initBindings={'kennung':valLit}):
            if keycl[0] == None:
                if keycl[1] == None:
                    return keynumber
                else:
                    return keycl[1]
            else:
                return keycl[0]

    def getKeyClassOpt(keynumber):
        key_query = prepareQuery(
        """select ?keyclass ?newkeyclass
        where 
        {?keyclass asbkey13:Kennung ?kennung.
        FILTER regex(?kennung, '"""+keynumber+"""')
        OPTIONAL {?keyclass owl:sameAs ?newkeyclass.}
        }
        """, initNs= {"asbkey13": AONS, "owl":OWL})
        for keycl in ont.query (key_query):
            if keycl[0] == None:
                if keycl[1] == None:
                    return keynumber
                else:
                    return keycl[1]
            else:
                return keycl[0]

    def getKeyFixClass(name):
        LitName = Literal(name, lang='de')
        key_query = prepareQuery(
        """select  ?newkeyclass
        where 
        {?newkeyclass rdfs:label ?literal.
        ?newkeyclass rdfs:subClassOf asbkey:Schluesseltabellen.
        }
        """,initNs= {"asbkey": ANSK})
        for keycl in ont.query (key_query, initBindings={'literal':LitName}):
            return keycl[0]
        
    def getNewKeyClass(supercl, newKeyPos):
        LitNewKeyPos = Literal(str(newKeyPos))
        key_query = prepareQuery(
        """select  ?newkeyclass
        where 
        {?newkeyclass asbkey:Kennung ?literal.
        ?newkeyclass rdfs:subClassOf ?superKeyClass. 
        }
        """,initNs= {"asbkey": ANSK, "asb": ANS})
        for keycl in ont.query (key_query, initBindings={'superKeyClass':URIRef(supercl),'literal':LitNewKeyPos}):
            return keycl[0]

    def createRel(s,p,o):
        cons_query= prepareQuery("""
                construct
                {?sub ?p ?obj.}
                where
                {?obj asb:ObjektMitID_hatObjektID/asb:ObjektID_ID ?asbid.}""" 
                , initNs={'asb':ANS})

        for row in graph.query(cons_query, initBindings={'sub': s, 'p':p, 'asbid':Literal(o)}):
            return row

    
    def createBlankTriple(blank, prop):
        asbprop = getAsbProp(prop)
        if asbprop != None:
            b2 = BNode()
            graph.add((blank, asbprop[0], b2))
            return b2

    def hasDecimal (range):
        ask_query = prepareQuery("""
                ask
                {asb:hasdecimal rdfs:domain ?range.}"""
                , initNs={'asb':ANS})
        for row in ont.query(ask_query, initBindings={'range': range}):
            return row
        

    def createValueTriple(s,p,o,i):
        asbProp = getAsbProp(p)   
        if asbProp != None:
            propRes = asbProp[0] 
            propType = asbProp[1]
            propRange = asbProp[2]
            
            if "DatatypeProperty" in propType:
                xsdType = propRange.split('#')[1]
                #print(xsdType)
                if xsdType != "string":
                    graph.add((s, propRes, Literal(o, datatype = XSD+xsdType)))
                else:
                    graph.add((s, propRes, Literal(o)))
            
            elif hasDecimal(propRange) == True:
                #print(propRange)
                b = BNode()
                graph.add ((s, propRes, b))
                graph.add((b, URIRef(ANS + "hasdecimal"), Literal(o, datatype = XSD+"decimal")))
                
            else:
                if "associatedWith" in propRes:
                    assodict = {}
                    assodict['subject']= s
                    assodict['objectId'] = o
                    assodictlist.append(assodict)
                
                elif "isPartOf" in propRes:
                    partdict = {}
                    partdict['subject']= s
                    partdict['objectId'] = o
                    partdictlist.append(partdict)
                
                elif propRes == URIRef(ANS+"Schaden_Schaden"):
                    #print(o)
                    c.execute("""select 
                            SchadenPosition, 
                            RissartPosition, 
                            RissbreitenklassePosition,
                            SchadenRissbreite
                            from SCHADEN_MAPPING_TABLE 
                            where
                            alterSchluessel = ? """,(o,))
                    x = c.fetchone()
                    #print(x)
                    if x != None:
                        #print(x)
                        if x[0]!= None and  str(x[0]).strip() != '':
                            objKey = getNewKeyClass(propRange,x[0])
                            #print(objKey)
                            graph.add((s, propRes, URIRef(objKey)))
                        
                        elif x[1] != None:
                            prop = getAsbProp("Schaden_Rissart")
                            #print(prop[-1])
                            objKey = getNewKeyClass(prop[-1],x[1])
                            #print(objKey)
                            #print(prop[0])
                            graph.add((s, prop[0],URIRef(objKey)))
                        
                        elif x[2] != None:
                            prop = getAsbProp("Schaden_Rissbreitenklasse")
                            objKey = getNewKeyClass(prop[-1],x[1])
                            graph.add((s, prop[0], URIRef(objKey)))
                        
                        else:
                            pass
                        
                        if x[3] != None:
                            c.execute("select OBJECT from CURRENT_BRIDGE where FIXVALUE = 'SCHAD_M' and ASBID=?", (i))
                            Menge = c.fetchall()
                            if Menge != None:
                                M = Menge[0][0]
                                #print(M)
                                createValueTriple(s, "Schaden_Rissbreite", M, i)
                            else:
                                b=BNode()
                                prop1 = getAsbProp("Schaden_DimensionierteMengenangabe")
                                graph.add((s, prop1[0],b))
                                prop2 = "MengeMitDimensionErlaeuterung_Menge"
                                b2 = createBlankTriple(b, prop2)
                                prop3 = "MengeMitDimension_Anzahl"
                                createValueTriple(b2, prop3, Menge[0], i)
                    else:
                        keyClass = getKeyClass(o)
                        if keyClass != None:
                            graph.add((s, propRes, URIRef(keyClass)))
                    
                
                else:
                    if len(o)>= 14:
                        if len(o) == 14:
                            o = "0"+ o
                        keyClass = getKeyClass(o)
                        if keyClass != None:
                            graph.add((s, propRes, URIRef(keyClass)))
                        else:
                            #print(o)
                            keyClass = getKeyClassOpt(o)
                            if keyClass != None:
                                graph.add((s, propRes, URIRef(keyClass)))
                            else:
                                pass
                                #notProcessedProps.append(p)
                                #notProcessedProps.append(o)
                    else:
                        #search in newkey Classes
                        print(propRange)
                        keyClass = getNewKeyClass(propRange, o)
                        if keyClass != None:
                            graph.add((s, propRes, URIRef(keyClass)))
                        else:
                            #print(s)
                            #print (propRes)
                            #print (o)
                            #print(propRange)
                            notProcessedProps.append(s)
                            notProcessedProps.append(p)
                            notProcessedProps.append(o)
        else:
            pass
     #       print(p)

    #####

    assodictlist = []
    partdictlist = []
    notProcessedProps = []


    OrtsangabenFix = ["LAENGS", "QUER", "HOCH","FELD", "DI", "SCHAD_M" ]

    c.execute("select distinct ASBID from CURRENT_BRIDGE")
    #print(c.fetchall())

    for asbid in c.fetchall():
        c.execute("select distinct SUBJECT from CURRENT_BRIDGE where ASBID =?", asbid)
        x =c.fetchall()
        subName = x[0]
    
        InsObj = URIRef(INS + (str(asbid[0].replace(" ","_"))+"_"+subName[0]))
        
        for sub in x:
            subLit = Literal(str(sub[0]),lang='de')
            asbClass = getAsbClass(subLit)

            if asbClass != None:
                graph.add((InsObj, RDF.type , asbClass))
            else:
                notProcessedProps.append(sub)
            
        c.execute("select distinct PROPERTY, OBJECT, FIXVALUE from CURRENT_BRIDGE where ASBID =?", asbid)
        x =c.fetchall()
        
        OrtsangabenProcessed = []
        Dimensionen = []
            
        for i in x:
            prop =i[0]
            val = i[1] 
            fix = i[2]
            #print (fix)
            
            if "," in prop:
                props = prop.split(",")
            
            else:
                props = []
                props.append(prop)
            
            if len(props) ==1:
                createValueTriple(InsObj, props[0], val,asbid)
                
                    
                
                if fix == "Bruecke":
                    cl = getAsbClass(fix)
                    if cl != None:
                        graph.add((InsObj, RDF.type, cl))
            
            if len(props) >1 and fix not in OrtsangabenFix:
                #if fix in OrtsangabenFix:
                #    pass
                
                #else:
                asbProp1 = getAsbProp(props[0])
                blank1 = BNode()
            
                graph.add((InsObj, asbProp1[0],blank1))
                
                if len(props) <3 :
                    createValueTriple(blank1, props[-1], val, asbid)
                
                if len(props) >2 and fix != None:
                    asbProp2 = getAsbProp(props[1])
                    fixObj = getKeyFixClass(fix)
                    graph.add((blank1, asbProp2[0], fixObj))
                    createValueTriple(blank1, props[-1], val, asbid)
                
                if len(props) >2 and fix == None:
                    midProps = props[1:-1]
                    b = blank1
                    for p in midProps:
                        b = createBlankTriple(b,p)
                    
                    createValueTriple(b,props[-1],val,asbid)
                
            if fix in OrtsangabenFix: 
                print (fix)
                if fix not in OrtsangabenProcessed:
                    print(OrtsangabenProcessed)
                    c.execute("""select distinct PROPERTY, OBJECT 
                            from CURRENT_BRIDGE where (ASBID =? and FIXVALUE =?) """,(asbid[0], fix))
                    rows = c.fetchall()
                    print (rows)
                    print (len(rows))
                    b = BNode()
                    if len(rows) == 2:
                    # print(rows)
                        for t in rows:
                            print (t)
                            props = t[0].split(",")
                            print(props)
                            prop0 = getAsbProp(props[0])[0]
                            print(prop0)
                            prop1 = props[1]
                            print(prop1)
                            graph.add((InsObj, prop0, b))
                            if len(props) == 2:
                                print(t[1])
                                createValueTriple(b, prop1, t[1],asbid)
                            else:
                                subject = createBlankTriple(b,prop1)
                                #print(t[1])
                                createValueTriple(subject, props[-1], t[1],asbid)
                    
                    if len(rows) == 1:
                        props = rows[0][0]
                        value = rows[0][1]
                        if "," in props:  
                            propList = props.split(",")
                            prop1 = propList[0]
                            prop2 = propList[1]
                            
                            asbProp1 = getAsbProp(prop1)[0]
                            graph.add((InsObj, asbProp1,b))
                            
                        
                            createValueTriple(b,prop2,value, asbid)
                                
                        else:
                            pass
                        
                    else:
                        pass
                    
                    OrtsangabenProcessed.append(fix)
                


    assoprop = URIRef(ANS + "associatedWith")       
    for asso in assodictlist:
        sub = asso.get("subject")
        o = asso.get("objectId")
        #if len(o)!= 15:
        rel = createRel(sub, assoprop, o)
        if rel != None:
            graph.add(rel)
        else:
            pass
   

    partprop = URIRef(ANS + "isPartOf")       
    for part in partdictlist:
        sub = part.get("subject")
        o = part.get("objectId")
        rel = createRel(sub, partprop, o)
        if rel != None:
            graph.add(rel)
        else:
            pass

    print (notProcessedProps)

    return graph

        
                
                
            
            
            
            
            
            
            
            
                    
            
    