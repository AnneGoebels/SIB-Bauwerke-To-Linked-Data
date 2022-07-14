from rdflib import Graph, Literal, Namespace, URIRef, BNode
from rdflib.namespace import OWL, RDF, RDFS, XSD
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.stores.memory import Memory
from rdflib.graph import ConjunctiveGraph
import random

# einbauort in script lösen

def createBDefElementLink (g, ins):

    def randomnumber(N):
        minimum = pow(10, N - 1)
        maximum = pow(10, N) - 1
        id = random.randint(minimum, maximum)
        return str(id)

    ANS = Namespace("https://w3id.org/asbingowl/core#")
    AONS = Namespace("https://w3id.org/asbingowl/keys/2013#")
    ANSK = Namespace("https://w3id.org/asbingowl/keys#")
    INS = Namespace ("http://example.org/sibbw7936662#")
    AOI = Namespace ("https://w3id.org/aoi#")

    queryBdef = prepareQuery("""select ?s ?btlgrp ?konteil ?bauteil
                        where
                        {?s a asb:ASBING13_BauteilDefinition.
                        ?s asb:ASBING13_Bauteilgruppe ?btlgrp.
                        OPTIONAL { ?s asb:ASBING13_Konstruktionsteil ?konteil.}
                        OPTIONAL {?s asb:ASBING13_Bauteil ?bauteil.}
                        } """,
                        initNs={"asb":ANS})

    queryBdefOrt= prepareQuery("""SELECT ?o ?a
                                   WHERE {
                                   ?s asb:Schaden_Ortsangabe  ?b.
                                   ?b asb:Ortsangabe_Ortsangabe ?o.
                                   OPTIONAL{
                                   ?b asb:Ortsangabe_GroesseOrtsangabe [asb:AbstandAnzahl_Anzahl ?a] .}
                                   } """, initNs={"asb": ANS})


    clsInsQuery = prepareQuery(""" select ?asbBauteil ?ort
                                    where {
                                           {
                                           ?asbBauteil a ?cls. 
                                           ?cls rdfs:subClassOf* asb:AbstraktesBauteil.
                                           OPTIONAL{
                                           ?asbBauteil asb:AbstraktesBauteil_Einbauort ?ort. }
                                            }
                                           UNION 
                                           {
                                           ?asbBauteilart a ?cls.
                                           ?cls rdfs:subClassOf* asb:AbstrakteBauteilart.
                                           ?asbBauteil asb:associatedWith ?asbBauteilart. 
                                           ?asbBauteil a ?bCls.
                                           ?bCls rdfs:subClassOf* asb:AbstraktesBauteil.
                                           OPTIONAL {
                                           ?asbBauteil asb:AbstraktesBauteil_Einbauort ?ort. }
                                           }
                                           }""", initNs={"asb":ANS})

    newElementArt = URIRef(INS + "N_" + randomnumber(7) + "_TragendesBauteil")

    for row in g.query(queryBdef):

        bdef = row.s
        bauteilgruppe = row.btlgrp
        konteil = row.konteil
        bauteil = row.bauteil

        bauteilorte = []
        for orte in g.query(queryBdefOrt, initBindings={"s":URIRef(bdef)}):
            bauteilorte.append(orte)
        # print(bauteilorte)

        # gruppennamen vereinfachen (hier auch label abfragen möglich)
        try:
            groupName = str(bauteilgruppe).split("Bauteilgruppe")[1]
        except:
            groupName = str(bauteilgruppe)

        # Fahrzeugrueckhaltesystem neue Bez. für Schutzeinrichtungen
        if groupName == "Schutzeinrichtungen":
            groupName = "FahrzeugRueckhaltesystem"
        else:
            pass

        #bsp.: aus GroupName : Kappe --> asb:Kappe --> Inst. suchen
        bauteilClsName = URIRef(ANS+groupName)

        # mögliche Instanzen dieser Klasse finden
        possibleIns = []
        for res in g.query(clsInsQuery, initBindings={"cls":bauteilClsName}):
            possibleIns.append(res)


        #print(possibleIns)

        if len(possibleIns) == 1:
            # Ueberbau besteht aus mehreren Traegern und einer Platte (zusammeng. Bauteil) ,
            # einzelne Tr. als TragendesBauteil definieren und mit Ueberbau verbinden
            if bauteil is not None:
                if groupName == "Ueberbau" and "traeger" in str(bauteil):
                    newId = randomnumber(7)
                    newElement = URIRef(INS + "N_" +newId+"_Bauteil")

                    ins.add((newElement, RDF.type, ANS.Bauteil))
                    blank =BNode()
                    ins.add((newElement, ANS.ObjektMitID_hatObjektID, blank))
                    ins.add((blank, ANS.ObjektID_ID, Literal(newId)))
                    ins.add((blank, ANS.ObjektID_NamensraumVerfahren, ANSK.NamensraumVerfahren_ASBObjektId))

                    for ort in bauteilorte:
                        keyOrt = ort[0]
                        b1 = BNode()
                        ins.add((newElement, ANS.AbstraktesBauteil_Ortsangabe, b1))
                        ins.add((b1, ANS.Ortsangabe_Ortsangabe, URIRef(keyOrt)))

                        if len(ort)>1:
                         anzOrt = ort[1]
                         b = BNode()
                         ins.add((b1, ANS.Ortsangabe_GroesseOrtsangabe, b))
                         ins.add((b, ANS.AbstandAnzahl_Anzahl, Literal(anzOrt)))

                        else:
                            pass

                    ins.add((newElement, ANS.isPartOf, URIRef(possibleIns[0][0])))
                    ins.add((newElement, ANS.associatedWith, newElementArt))
                    ins.add((newElementArt, RDF.type, ANS.TragendesBauteil))
                    ins.add((newElementArt, ANS.TragendesBauteil_Art, ANSK.ArtTragendesBauteil_Traeger )) # hier Variable möglich für Art?
                    ins.add((bdef, ANS.beschreibtBauteil, newElement))

                elif groupName == "Ueberbau" and "Ueberbau" in str(bauteil):
                    ins.add((bdef, ANS.beschreibtBauteil, possibleIns[0][0]))

                elif groupName == "Ueberbau" and "Platte" in str(bauteil):
                    ins.add((bdef, ANS.beschreibtBauteil, possibleIns[0][0]))


            elif groupName == "Ueberbau" and  bauteil == None :
                ins.add((bdef, ANS.beschreibtBauteil, possibleIns[0][0]))




            else:
                print(row, possibleIns)



        elif len(possibleIns)> 1:

            # den block nach oben, verallgemeinern ,auch für andere bauteile
            if groupName == "Unterbau" and bauteil is not None and len(bauteilorte) != 0:
                anzahlFelder = g.query("""select ?feld (count(?feld) as ?count)
                                            where {?feld a asb:Feld.
                                            FILTER EXISTS {?feld asb:ASBObjekt_Name ?name.}}
                                            """, initNs={"asb":ANS})

                for a in anzahlFelder:
                    first = "1"
                    last = a[1]
                    if "Hinten" in str(bauteilorte[0][0]):
                        nr = last
                    elif "Vorn" in str(bauteilorte[0][0]):
                        nr = first
                    else:
                        nr = ""

                #print(bauteil)
                for unterbau in g.query("""select ?unterbau
                                                    where {
                                                   {?unterbau asb:Unterbau_Art ?artkey.
                                                   ?artkey rdfs:label ?label.
                                                   ?bauteil rdfs:label ?label.
                                                   ?feld a asb:Feld.
                                                   ?feld asb:associatedWith ?unterbau. 
                                                   ?feld asb:ASBObjekt_Name ?nr.}
                                                   UNION
                                                   {?unterbau asb:Unterbau_Art ?artkey.
                                                   ?artkey rdfs:label ?label.
                                                   ?bauteil rdfs:subClassOf* ?superbauteil.
                                                   ?superbauteil rdfs:label ?label.
                                                   ?feld a asb:Feld.
                                                   ?feld asb:associatedWith ?unterbau. 
                                                   ?feld asb:ASBObjekt_Name ?nr.}
                                                }""", initNs={"asb":ANS}, initBindings={"bauteil":bauteil, "nr":Literal(str(nr))}):
                    if unterbau is not None:
                        ins.add((URIRef(bdef),ANS.beschreibtBauteil, unterbau.unterbau ))
                        #print("added", unterbau)
                    else:
                        print(bdef)

            elif groupName == "Fahrbahnuebergang":
                #print(labelLit)

                for ele, einbauort in possibleIns:
                    #print(ele,einbauort)
                    if len(bauteilorte) == 0 or "beide"  in str(bauteilorte[0][0]):
                        ins.add((URIRef(bdef), ANS.beschreibtBauteil, URIRef(ele)))

                    else:
                        x = {}
                        if "10" in str(einbauort):
                            x["Vorn"] = ele
                            #print(einbauort)
                        elif "30" in str(einbauort):
                            x["Hinten"] = ele
                            #print(einbauort)

                        #print(x)

                        #print(bauteilorte[0][0])
                        if "vorneUndHinten" in  str(bauteilorte[0][0]):
                            ins.add((URIRef(bdef), ANS.beschreibtBauteil, URIRef(ele)))
                        elif "Vorn" in str(bauteilorte[0][0]):
                            fahrbu = x.get("Vorn")
                            #print(fahrbu)
                            ins.add((URIRef(bdef), ANS.beschreibtBauteil, URIRef(fahrbu)))
                        elif "Hinten" in str(bauteilorte[0][0]):
                            fahrbu = x.get("Hinten")
                            #print(fahrbu)
                            ins.add((URIRef(bdef), ANS.beschreibtBauteil, URIRef(fahrbu)))
                        else:
                            ins.add((URIRef(bdef), ANS.beschreibtBauteil, URIRef(ele)))


            elif groupName == "FahrzeugRueckhaltesystem"  and konteil is not None :
                #print(konteil)
                answers = []

                subsetSeite = []

                ## noch rechts und links beachten (ost = Links, west = Rechts)
                if len(bauteilorte) > 0:
                    for ort in bauteilorte:
                        #print(ort)
                        for rueckBauteil, einbauort in possibleIns:

                           # print(einbauort)
                            if "rechts" in str(ort[0].lower()) and "West" in str(einbauort):
                                if rueckBauteil not in subsetSeite:
                                    subsetSeite.append(rueckBauteil)
                            elif "links" in str(ort[0].lower()) and "Ost" in str(einbauort):
                                if rueckBauteil not in subsetSeite:
                                    subsetSeite.append(rueckBauteil)
                            elif "West" not in str(einbauort) and "Ost" not in str(einbauort) and rueckBauteil not in subsetSeite:
                                    subsetSeite.append(rueckBauteil)
                else:
                    for rueckBauteil, einbauort in possibleIns:
                        subsetSeite.append(rueckBauteil)

                #print(subsetSeite)
                for i in subsetSeite:
                   # print(i)
                    for ans in g.query("""ask {{
                                        ?asbBauteil asb:associatedWith ?rBauteilart.
                                        ?rBauteilart a asb:FahrzeugRueckhaltesystem.
                                        ?rBauteilart asb:FahrzeugRueckhaltesystem_Art 
                                         [ asb:ArtFahrzeugRueckhaltesystem_Systembezeichnung
                                         [ asb:Systembezeichnung_Bezeichnung ?key] ].
                                        ?key rdfs:label ?label.
                                        ?konteil rdfs:label ?label. }
                                        UNION
                                        {?asbBauteil asb:associatedWith ?rBauteilart.
                                        ?rBauteilart a asb:FahrzeugRueckhaltesystem.
                                        ?rBauteilart asb:FahrzeugRueckhaltesystem_Art 
                                         [ asb:ArtFahrzeugRueckhaltesystem_Systembezeichnung
                                         [ asb:Systembezeichnung_Bezeichnung ?key] ].
                                        ?key rdfs:label ?label.
                                        ?konteil rdfs:subClassOf+ ?superkonteil.
                                        ?superkonteil rdfs:label ?label. }} """,
                                       initNs={"asb":ANS}, initBindings={"asbBauteil":URIRef(i),"konteil":URIRef(konteil)}):
                        #print(ans, konteil, rueckBauteil)
                        if ans == True:
                            ins.add((URIRef(bdef), ANS.beschreibtBauteil, URIRef(i)))
                            #print("added", bdef, "fahzeugrueck")
                        else:
                            answers.append(ans)

                if len(answers) == len(possibleIns):
                    #print(bauteil)
                    bauteilAsClsName = bauteil.split("_")[1]
                    for cls in g.query(clsInsQuery, initBindings={"cls":URIRef(ANS+bauteilAsClsName)}):
                        #print(cls.asbBauteil)
                        ins.add((URIRef(bdef), ANS.beschreibtBauteil, URIRef(cls.asbBauteil)))
                        #print("added", konteil, cls.asbBauteil)

            elif groupName == "Kappe":
                for kappe_ort in possibleIns:
                    #print(kappe_ort)
                    kappe = kappe_ort[0]
                    einbauort = kappe_ort[1]
                    #print(einbauort)
                    if len(bauteilorte)!= 0:
                        if "Recht" in str(bauteilorte[0][0]) and str(einbauort):
                            ins.add((bdef, ANS.beschreibtBauteil, URIRef(kappe)))
                            break
                        elif "Link" in str(bauteilorte[0][0]) and str(einbauort):
                            ins.add((bdef, ANS.beschreibtBauteil, URIRef(kappe)))
                            break

                        else:
                            # wenn nicht rechts oder links mit beiden verbinden
                            #print(einbauort, bauteilorte)
                            ins.add((bdef, ANS.beschreibtBauteil, URIRef(kappe)))
                    else:
                        ins.add((bdef, ANS.beschreibtBauteil, URIRef(kappe)))




            else:
                #print(bdef,possibleIns)
                for bauteil, ort in possibleIns:
                    ins.add((URIRef(bdef), ANS.beschreibtBauteil, URIRef(bauteil)))
                    #print( "addedtoAll", bdef, i )

        elif len(possibleIns)== 0:
            if konteil is not None:
                if "Fahrbahn" in konteil and "Belaege" in groupName :
                    fahrbahn = prepareQuery("""select ?fahrbahnBelag
                                          where
                                          { ?fahrbahnBelag a asb:Bauteil;
                                             asb:associatedWith ?belag.
                                            ?belag a asb:BelagAbdichtung;
                                             asb:associatedWith ?strasse.
                                             ?strasse a asb:StrasseWeg.
                                             FILTER NOT EXISTS {?strasse asb:Verkehrsanlage_AbweichendeZuordnung true .}
                                             } """, initNs={"asb":ANS} )
                    for fb in g.query(fahrbahn):
                        #print(fb)
                        ins.add((URIRef(bdef), ANS.beschreibtBauteil, fb[0]))



                elif "Sonstige" in str(bauteilgruppe):
                    #sonstige can be anything, testing only for  stairs
                    askTreppe = prepareQuery("""ask 
                                                {?konteil  rdfs:comment ?comment. 
                                                FILTER regex (?comment, 'treppe', 'i')}""")

                    for ans in g.query(askTreppe,initBindings={"konteil": URIRef(konteil)}):
                        if ans == True:

                            for treppe in g.query("""select ?asbBauteil ?ort where {
                                                 ?asbBauteil a ?asbBauteilCls.
                                                  ?asbBauteilCls rdfs:subClassOf* asb:AbstraktesBauteil.
                                                  ?asbBauteil asb:associatedWith ?asbBauteilart.
                                                  ?asbBauteilart a ?bauteilartCls.
                                                  ?bauteilartCls rdfs:subClassOf* asb:AbstrakteBauteilart.
                                                  ?asbBauteil asb:AbstraktesBauteil_Einbauort ?ort.
                                                  ?asbBauteilart asb:EinfacheBauteilart_Art ?keyArt.
                                                  ?keyArt rdfs:label ?label.
                                                  ?konteil rdfs:subClassOf* ?superKonteil.
                                                  ?superKonteil rdfs:label ?label. }
                                                    """, initNs={"asb":ANS}, initBindings={"konteil":URIRef(konteil)}):

                                if treppe is not None:
                                    if len(bauteilorte)==0 or "beide" in str(bauteilorte[0][0]):
                                        ins.add((URIRef(bdef), ANS.beschreibtBauteil, URIRef(treppe.asbBauteil)))

                                    else:

                                        o ={}
                                        if "30" in str(treppe.ort):
                                            o["Vorn"] = treppe.asbBauteil
                                        elif "10" in str(treppe.ort):
                                            o["Hinten"] = treppe.asbBauteil
                                        else:
                                            print(treppe.ort)


                                        if "Vorn" in str(bauteilorte[0][0]):
                                            trp = o.get("Vorn")
                                            ins.add((URIRef(bdef), ANS.beschreibtBauteil, URIRef(trp)))
                                        elif "Hinten" in str(bauteilorte[0][0]):
                                            trp= o.get("Hinten")
                                            ins.add((URIRef(bdef), ANS.beschreibtBauteil, URIRef(trp)))
                                        else:
                                            pass
                                else:
                                    print(bdef)

                        else:
                            print("Nicht zuordbar" , konteil)

            else:
                print("no Matching elements", bauteilgruppe, bauteil, konteil, bauteilorte)

    return ins



       
