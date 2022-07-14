import datetime

from rdflib.namespace import RDF, XSD, OWL, PROV
from rdflib import Graph, Literal, Namespace, URIRef,  BNode
import random
from createBauteilDefTable import createBauteilDefTable
from LinkBauteilDeftoASBelement import createBDefElementLink
import sqlite3
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.stores.memory import Memory
from rdflib.graph import ConjunctiveGraph


def postProcess(g, ins, bw_nr, INS, containsBauteile) :
    ANS = Namespace("https://w3id.org/asbingowl/core#")
    AONS = Namespace("https://w3id.org/asbingowl/keys/2013#")
    ANSK = Namespace("https://w3id.org/asbingowl/keys#")
    AOI = Namespace("https://w3id.org/aoi#")
    #OPM = Namespace("https://w3id.org/opm#")
    #SCHEMA = Namespace("https://schema.org/")
    DOT = Namespace("https://w3id.org/dot#")

    # get connection to database
    conn = sqlite3.connect(r'C:\GitHub\TwinGen\scripts\instanceGraph\ASBING.db')
    c = conn.cursor()


    def randomnumber(N):
        minimum = pow(10, N-1)
        maximum = pow(10, N) - 1
        id = random.randint(minimum, maximum)
        return str(id)

    # noch Unterbau mit Teilbauwerk verbinden(ispartof), und zusätzliches Feld entfernen, bzw, Infos zu echten Feldern hinzufügen

    #geländer mit zwei Ortsangaben verbinden
    gel_query= prepareQuery("""
    select ?s
    where {
    ?s a asb:Schaden.
    ?s asb:ASBING13_Konstruktionsteil ?konteil.
    }""", initNs={"asb":ANS})

    for res_u in ins.query(gel_query, initBindings={"konteil":URIRef(AONS["130022112234000_Fussplatte"])}):
        print (res_u)
        b = BNode()
        ins.add((res_u.s, ANS.Schaden_Ortsangabe, b))
        ins.add((b, ANS.Ortsangabe_Ortsangabe, ANSK.OrtsangabeBauteilSchaden_Allgemein_Unterseite))

    for res_o in ins.query(gel_query, initBindings={"konteil":URIRef(AONS["130022121141000_Handlauf"])}):
        print(res_o)
        b = BNode()
        ins.add((res_o.s, ANS.Schaden_Ortsangabe, b))
        ins.add((b, ANS.Ortsangabe_Ortsangabe, ANSK.OrtsangabeBauteilSchaden_Allgemein_Oberseite))


    feld_unterbau_query = prepareQuery("""select ?unterbau ?tbw ?feldOrig
                                               where 
                                               {
                                                ?feldOrig a asb:Feld;
                                                asb:associatedWith ?unterbau;
                                                asb:associatedWith ?tbw.
                                                ?tbw a asb:Teilbauwerk. 
                                                ?unterbau a asb:Unterbau. 
                                               }  """, initNs={"asb": ANS})
    count = 0
    for result in ins.query(feld_unterbau_query):
        #print(len(result))
        count = count + 1
        #print(count)
        ins.add((result.unterbau, ANS.isPartOf, result.tbw))

        feld_orig_new_query = prepareQuery("""select ?feldNew ?p ?o ?p2 ?o2
                                                       where 
                                                       {
                                                        ?feldNew a asb:Feld.
                                                        ?feldNew ?p ?o. 
                                                        ?tbw asb:associatedWith ?feldNew.
                                                        OPTIONAL {?o ?p2 ?o2.}
                                                        MINUS {?o a asb:Ueberbau.}
                                                       }  """, initNs={"asb": ANS})

        for newFeld in ins.query(feld_orig_new_query, initBindings={"tbw": result.tbw}):
            #print(newFeld)
            if newFeld.p2 is not None :
                if newFeld.p != ANS.ObjektMitID_hatObjektID or newFeld.p != ANS.associatedWith :
                    b = BNode()
                    ins.add((result.feldOrig, newFeld.p, b))
                    ins.add((b, newFeld.p2, newFeld.o2))
            elif newFeld.p2 is None :
                if newFeld.p != ANS.ObjektMitID_hatObjektID or newFeld.p != ANS.associatedWith:
                    ins.add((result.feldOrig, newFeld.p, newFeld.o))
            else:
                pass
            if count == len(result):
                ins.remove((result.tbw, ANS.associatedWith, newFeld.feldNew))
                ins.remove((newFeld.feldNew, newFeld.p, newFeld.o ))
                if newFeld.p2 is not None:
                    ins.remove((newFeld.o, newFeld.p2, newFeld.o2))


    #wenn ein Schaden mit 2 Jahren verbunden ist, das ältere jahr löschen
    deleteQuery = g.query(""" SELECT ?schaden ?jahr1
                            WHERE{
                            ?schaden asb:PruefungUeberwachung_Pruefjahr ?jahr1 .
                            ?schaden asb:PruefungUeberwachung_Pruefjahr ?jahr2 
                            FILTER (?jahr1 < ?jahr2)}""")
        
    for row in deleteQuery:
        g.remove((row["schaden"], ANS["PruefungUeberwachung_Pruefjahr"], row["jahr1"]))

    # connect pruef elements on one overall pruefung (Same year)
    yearQuery = prepareQuery ("""select ?datum 
                                    where 
                                    { ?s asb:PruefungUeberwachung_Pruefungsabschluss ?datum .}""",
                                     initNs={"asb":ANS})

    conPrufPruf = prepareQuery("""construct {?pruf1 asb:associatedWith ?pruf2}
                        where{
                        ?pruf1 a asb:PruefungMitZustandsnote;
                        asb:PruefungUeberwachung_Pruefungsabschluss ?datum.
                        ?pruf2 a asb:PruefungMitZustandsnote;
                        asb:PruefungUeberwachung_Pruefungsabschluss ?datum.
                        ?pruf1 ?p ?o.
                        FILTER(?pruf1 != ?pruf2)
                        }LIMIT 1
                        """,initNs= {"asb": ANS, "xsd":XSD})

    for datum in g.query(yearQuery):
        #print(datum)
        for row in g.query (conPrufPruf, initBindings={'datum': Literal(str(datum["datum"]), datatype=XSD.date)}):
            ins.add(row)

    # add status to inspections
    aktPruf = prepareQuery("""select ?aktYear ?aktPruf ?oldPruf
                              where {
                              ?aktPruf asb:PruefungUeberwachung_Pruefjahr ?aktYear.
                              ?aktPruf a asb:PruefungMitZustandsnote.
                              ?oldPruf asb:PruefungUeberwachung_Pruefjahr ?oldYear.
                              ?oldPruf a asb:PruefungMitZustandsnote.
                              FILTER (?aktYear != ?oldYear)
                              {
                                select (MAX(?year) as ?aktYear)
                                where
                                {?pruf a asb:PruefungMitZustandsnote.
                                 ?pruf asb:PruefungUeberwachung_Pruefjahr ?year. } 
                                 LIMIT 1
                              }
                              }
                              """, initNs={"asb": ANS})

    for p in g.query(aktPruf):
        print(p)
        ins.add((p.aktPruf, ANS.PruefungUeberwachung_Status, ANSK.StatusPruefung_abgeschlossen))
        ins.add((p.oldPruf, ANS.PruefungUeberwachung_Status, ANSK.StatusPruefung_ersetzt_ungueltig))

    # connect damages to inspections by year
    conSchadPruf = g.query("""
    CONSTRUCT {?pruf asb:associatedWith ?schaden.
               ?schaden dot:coveredInInspection ?pruf.}
    WHERE{
    ?schaden a asb:Schaden .
    ?schaden asb:associatedWith ?tbw.
    ?tbw a asb:Teilbauwerk.
    ?pruf a asb:PruefungMitZustandsnote .
    ?pruf asb:associatedWith ?tbw.
    ?schaden asb:PruefungUeberwachung_Pruefjahr ?jahr .
    ?pruf asb:PruefungUeberwachung_Pruefjahr ?jahr .}""",initNs= {"asb": ANS, "dot":DOT})

    for row in conSchadPruf:
        ins.add(row)
        #print(row)




    ortQuery = prepareQuery("""select distinct ?group ?ort ?ortlabel 
                               where  {?schaden a asb:Schaden;
                                       asb:Schaden_ID-Nummer-Schaden ?SchadenId;
                                       asb:associatedWith ?tbw;
                                       asb:Schaden_Ortsangabe/asb:Ortsangabe_Ortsangabe ?ort;
                                       asb:ASBING13_Bauteilgruppe ?group.
                                       ?ort rdfs:label ?ortlabel.
                                       }
                               """, initNs={"asb": ANS})
    
    BauteilDef = prepareQuery("""SELECT DISTINCT ?p ?o 
    WHERE 
    {?s a asb:Schaden.
    ?s asb:associatedWith ?tbw.
    ?tbw a asb:Teilbauwerk.
    ?s asb:Schaden_ID-Nummer-Schaden ?SchadenId.
    ?s ?p ?o. 
    ?p rdfs:domain asb:ASBING13_BauteilDefinition.}
    """, initNs={"asb" :ANS})

    ortBauteilDef = prepareQuery("""SELECT DISTINCT ?o ?a ?olabel ?group
       WHERE {
       ?s a asb:Schaden.
       ?s asb:Schaden_ID-Nummer-Schaden ?SchadenId.
       ?s asb:associatedWith ?tbw.
       ?s asb:ASBING13_Bauteilgruppe ?group.
       ?s asb:Schaden_Ortsangabe  ?b.
       ?b asb:Ortsangabe_Ortsangabe ?o.
       ?o rdfs:label ?olabel.
       OPTIONAL{
       ?b asb:Ortsangabe_GroesseOrtsangabe [asb:AbstandAnzahl_Anzahl ?a] .}
       } """, initNs={"asb": ANS})

    connSchadenObjekte = prepareQuery("""select ?schaden ?jahr
                            where  {?schaden a asb:Schaden;
                                    asb:Schaden_ID-Nummer-Schaden ?SchadenId;
                                    asb:associatedWith ?tbw;
                                    asb:PruefungUeberwachung_Pruefjahr ?jahr .
                                    } ORDER BY ?jahr   """, initNs={"asb" :ANS})

    schadenObjekt = prepareQuery("""select distinct ?SchadenId ?tbw
                                where  {?schaden a asb:Schaden;
                                        asb:Schaden_ID-Nummer-Schaden ?SchadenId;
                                        asb:associatedWith ?tbw.
                                        ?tbw a asb:Teilbauwerk.
                                        }
                                """, initNs={"asb": ANS})



    for row in g.query(schadenObjekt):
        #print (row)
        SchadenObjName = randomnumber(7)+"_SchadenObjekt"
        #print(SchadenObjName)
        SchadenObjRes = URIRef(INS + SchadenObjName)
        ins.add((SchadenObjRes, RDF.type, ANS.SchadenObjekt))
        #ins.add((SchadenObjRes, RDF.type, OPM.Property))
        ins.add((SchadenObjRes, ANS["Schaden_ID-Nummer-Schaden"], Literal(row[0])))
        ins.add((SchadenObjRes, ANS.associatedWith, row[1]))
        
        #print(row[0])
        IndAoi = URIRef(INS + (randomnumber(7)+"_AOI"))
        aoiExisting =[]
        for gruppe, ortclass, ortlabel in g.query(ortQuery, initBindings={'SchadenId':Literal(row[0]),'tbw':row.tbw}) :
            try:
                grp = str(gruppe).split("Bauteilgruppe")[1]
            except:
                grp = str(gruppe).split("_")[1]
            try:


                c.execute("select " + grp + " from ORTNACHELEMENT where Ortsangaben = UPPER(?)", (ortlabel,))
                res = c.fetchone()
                #print(res)
                if "AOI" in res:
                    ins.add((IndAoi, RDF.type, ortclass))
                    aoiExisting.append("yes")
                else:
                    print("noAOI" + ortlabel)

            except Exception as E:
                print(gruppe)
                print(E)

        if "yes" in aoiExisting:
            ins.add((IndAoi, AOI.locatesDamage, SchadenObjRes))
            ins.add((IndAoi, RDF.type, AOI.AreaOfInterest))


        SchadVerList =[]
        for schadVer in g.query(connSchadenObjekte,initBindings={'SchadenId':Literal(row[0]),'tbw':row.tbw} ):
            #print(schadVer)
            """
            newState = URIRef(INS + (randomnumber(7)+"_DamageState"))
            if schadVer.jahr == latestInspYear:
                ins.add((newState, RDF.type, OPM.CurrentPropertyState))

            else:
                ins.add((newState, RDF.type, OPM.OutdatedPropertyState))
            ins.add((newState, RDF.type, OPM.PropertyState))
            ins.add((SchadenObjRes, OPM.hasPropertyState, newState))
            ins.add((newState, PROV.generatedAtTime, Literal(schadVer.jahr, datatype=XSD.year)))
            ins.add((newState, SCHEMA.value, URIRef(schadVer.schaden)))
            SchadVerList.append(newState)
            """
            ins.add((SchadenObjRes, ANS.hatPruefDokumentation, URIRef(schadVer.schaden)))
            SchadVerList.append(schadVer.schaden)


        pairs = list(zip( SchadVerList,  SchadVerList[1:] +  SchadVerList[:1]))
        #print(pairs)
        
        for pair in pairs: 
            if pair == pairs[-1]:
                break
            else:
                #print(pair[0], pair[1])
                ins.add((URIRef(pair[0]), ANS.hasLaterState, URIRef(pair[1])))

    prior = g.query("""CONSTRUCT {?schadenNeu asb:hasPriorState ?schadenAlt . }
       WHERE {
       ?schadenAlt asb:hasLaterState ?schadenNeu.}""", initNs={"asb": ANS})

    for row in prior:
        # print(row)
        ins.add(row)


    # add SchadenObjekt Status
    # active status
    aktSchaden = prepareQuery("""select  ?schadenakt 
                                    where {?schadenakt a asb:SchadenObjekt;
                                           asb:hatPruefDokumentation ?schaden.
                                           ?schaden dot:coveredInInspection ?pruf.
                                           ?pruf asb:PruefungUeberwachung_Status asbkey:StatusPruefung_abgeschlossen.} 
                                """, {"asb": ANS, "dot": DOT, "asbkey": ANSK})

    for s in g.query(aktSchaden):
        print(s)
        ins.add((s.schadenakt, ANS.Schaden_Status, ANSK.StatusSchaden_erfasstUndBewertet))

    # inactive / beseitigt status
    SchadenObj = prepareQuery("""select  ?schadenobj
                                where {?schadenobj a asb:SchadenObjekt. } 
                                """, {"asb": ANS})

    askSchadenActive = prepareQuery(""" ask {?schadenobj asb:hatPruefDokumentation ?schaden.
                                         ?schaden dot:coveredInInspection ?pruf.
                                         ?pruf asb:PruefungUeberwachung_Status asbkey:StatusPruefung_abgeschlossen. }
                                    """, {"asb": ANS, "dot": DOT, "asbkey": ANSK})

    for schadobj in g.query(SchadenObj):
        for reply in g.query(askSchadenActive, initBindings={"schadenobj": schadobj.schadenobj}):
            print(reply)
            if reply == False:
                ins.add((schadobj.schadenobj, ANS.Schaden_Status, ANSK.StatusSchaden_beseitigt))


    note = createBauteilDefTable(g)
    print(note)
    conn = sqlite3.connect(r'C:\GitHub\TwinGen\scripts\instanceGraph\ASBING.db')
    c = conn.cursor()

    c.execute("select distinct SCHADOBJ,SCHADID,TEILBAUWERK from BAUTEILDEF where UNIONID is null or UNIONID = SCHADID")
    bDefinitions = c.fetchall()
    for bDef in bDefinitions:
        IndBauteilDef = URIRef(INS + (randomnumber(7)+"_BauteilDefinition"))
        ins.add((IndBauteilDef, RDF.type, ANS.ASBING13_BauteilDefinition))
        SchadenObjRes = URIRef(bDef[0])
        #print(SchadenObjRes)
        ins.add((SchadenObjRes, ANS.hatBauteilDefinition, IndBauteilDef))

        for p,o in g.query(BauteilDef, initBindings={'SchadenId':Literal(bDef[1]),'tbw':URIRef(bDef[2])}):
            ins.add((IndBauteilDef, p, o))
        ins.add((IndBauteilDef, ANS.associatedWith ,URIRef(bDef[2]) ))

        for orte in g.query(ortBauteilDef, initBindings={'SchadenId':Literal(bDef[1]),'tbw':URIRef(bDef[2])}):
            try:
                grp = str(orte.group).split("Bauteilgruppe")[1]
            except:
                grp = str(orte.group).split("_")[1]
            try:
                c.execute("select " + grp + " from ORTNACHELEMENT where Ortsangaben = UPPER(?)", (orte.olabel,))
                res = c.fetchone()
                #print(res)
                if "Bauteil" in res:
                    b1 = BNode()
                    ins.add((IndBauteilDef, ANS.Schaden_Ortsangabe, b1))
                    ins.add((b1, ANS.Ortsangabe_Ortsangabe, orte.o ))

                    if orte.a is not None:
                        b = BNode()
                        ins.add((b1, ANS.Ortsangabe_GroesseOrtsangabe, b ))
                        ins.add((b, ANS.AbstandAnzahl_Anzahl, orte.a))
                else:
                    pass

            except Exception as E:
                print(E)

        c.execute("select distinct SCHADOBJ from BAUTEILDEF where (UNIONID = ? and UNIONID != SCHADID and TEILBAUWERK = ?)",(bDef[1],bDef[2]))
        relSchad = c.fetchall()

        for obj in relSchad:
            #print(obj)
            SchadenObjResRel = URIRef(obj[0])
            #print(SchadenObjResRel)
            ins.add((SchadenObjResRel, ANS.hatBauteilDefinition , IndBauteilDef))

    # uneindeutige Ortsangaben (beidseitig, bzw. gar keine) jeweils mit beiden
    # (rechts/links bzw. vorne/hinten) verbinden für die spätere Verarbeitung

    newRechts = URIRef(ANSK.OrtsangabeBauteilSchaden_Allgemein_rechts)
    newLinks = URIRef(ANSK.OrtsangabeBauteilSchaden_Allgemein_links)
    newHinten = URIRef(ANSK.OrtsangabeBauteilSchaden_Widerlager_WiderlagerHinten)
    newVorn = URIRef(ANSK.OrtsangabeBauteilSchaden_Widerlager_WiderlagerVorn)
    newUnten = URIRef(ANSK.OrtsangabeBauteilSchaden_Allgemein_Unterseite)
    newOben =  URIRef(ANSK.OrtsangabeBauteilSchaden_Allgemein_Oberseite)
    labelBeid = Literal("beidseitig", lang="de")
    labelBeideWiderlager = Literal("beide Widerlager", lang="de")

    # beideWiderlager, oder keine Angabe  in vorne und hinten umwandeln
    for s in g.query (""" select ?s ?b ?ortkey
                        where 
                        {
                        ?s a asb:ASBING13_BauteilDefinition. 
                        ?s asb:ASBING13_Bauteilgruppe asbkey13:390021200000000_BauteilgruppeUnterbau.
                        ?s asb:Schaden_Ortsangabe ?b.
                        ?b asb:Ortsangabe_Ortsangabe ?ortkey.
                        ?ortkey rdfs:label ?l.
                        }
                        """, initNs={"asb":ANS, "asbkey13":AONS}, initBindings={"l":labelBeideWiderlager}):
        #print(s)
        ins.remove((s.b, ANS.Ortsangabe_Ortsangabe, s.ortkey))
        ins.add((s.b, ANS.Ortsangabe_Ortsangabe, newVorn))
        ins.add((s.b, ANS.Ortsangabe_Ortsangabe, newHinten))


    for s in g.query("""select ?s  where 
                        {
                        ?s a asb:ASBING13_BauteilDefinition. 
                        ?s asb:ASBING13_Bauteilgruppe asbkey13:390021200000000_BauteilgruppeUnterbau.
                        FILTER NOT EXISTS {?s asb:Schaden_Ortsangabe ?b. }
                         }
                        """, initNs={"asb":ANS, "asbkey13":AONS}):
        #print(s)
        b = BNode()
        ins.add((s.s, ANS.Schaden_Ortsangabe, b))
        ins.add((b,  ANS.Ortsangabe_Ortsangabe, newVorn))
        ins.add((b, ANS.Ortsangabe_Ortsangabe, newHinten))

    #kapppen und geländer die keine Ortsangabe haben mit beiden verbinden
    for s in g.query("""select ?s where {
                    {?s a asb:ASBING13_BauteilDefinition. 
                    ?s asb:ASBING13_Bauteilgruppe asbkey13:390022180000000_BauteilgruppeKappe.
                    FILTER NOT EXISTS {?s asb:Schaden_Ortsangabe ?b. } }
                    UNION
                    {?s a asb:ASBING13_BauteilDefinition. 
                    ?s asb:ASBING13_Bauteilgruppe asbkey13:390022210000000_BauteilgruppeSchutzeinrichtungen.
                    FILTER NOT EXISTS {?s asb:Schaden_Ortsangabe ?b. } }
                    }""", initNs={"asb":ANS, "asbkey13":AONS}):
        #print(s)
        b = BNode()
        ins.add((s.s, ANS.Schaden_Ortsangabe, b))
        ins.add((b, ANS.Ortsangabe_Ortsangabe, newRechts))
        ins.add((b, ANS.Ortsangabe_Ortsangabe, newLinks))

    # bauteile oder orte die mit "beidseitig" angegeben sind mit rechts und linsk stattdessen versehen

    for s in g.query(""" select ?s ?ortkey 
                where 
               {?s aoi:locatesDamage ?dam.
               ?s a ?ortkey.
               ?ortkey rdfs:label ?l.
               } """, initNs = {"aoi": AOI}, initBindings={"l":labelBeid}):

        #print(s)
        ins.remove((s.s, RDF.type, s.ortkey))
        ins.add((s.s, RDF.type, newRechts))
        ins.add((s.s, RDF.type, newLinks))

    for s in g.query(""" select ?s ?b ?ortkey
                         where{
                         ?s a asb:ASBING13_BauteilDefinition.
                         ?s asb:Schaden_Ortsangabe ?b.
                         ?b asb:Ortsangabe_Ortsangabe ?ortkey.
                         ?ortkey rdfs:label ?l.
                         } 
                        
                         
                         """, initNs={"asb": ANS}, initBindings={"l":labelBeid} ):
        #print(s)
        ins.remove((s.b, ANS.Ortsangabe_Ortsangabe, s.ortkey))
        ins.add((s.b, ANS.Ortsangabe_Ortsangabe, newRechts))
        ins.add((s.b, ANS.Ortsangabe_Ortsangabe, newLinks))



    # wenn vollständiger Datensatz inkl. Bauteile vorhanden:
    print(containsBauteile)
    if containsBauteile == True:



        # Kappe in zwei getrennte Bauteile umwandeln

        # neue kappe erstellen
        newId = randomnumber(7)
        newKappe = URIRef(INS + "N_" + newId + "_Bauteil")
        blank = BNode()
        ins.add((newKappe, RDF.type, ANS.Bauteil))
        ins.add((newKappe, ANS.AbstraktesBauteil_Einbauort, Literal("Linke Aussenkappe")))
        ins.add((newKappe, ANS.ObjektMitID_hatObjektID, blank))
        ins.add((blank, ANS.ObjektID_ID, Literal(newId)))
        ins.add((blank, ANS.ObjektID_NamensraumVerfahren, ANSK.NamensraumVerfahren_ASBObjektId))

        kappenBauteilQuery = prepareQuery("""select ?kappenBauteil ?kappenart ?o 
                                   where {
                                   ?kappenBauteil a asb:Bauteil;
                                     asb:associatedWith ?kappenart.
                                    ?kappenart a asb:Kappe. 
                                    ?kappenBauteil asb:AbstraktesBauteil_Einbauort ?o.
                                   }
                                   LIMIT 1
                                   """, initNs={"asb": ANS})

        for res in g.query(kappenBauteilQuery):
            ins.remove((res.kappenBauteil, ANS.AbstraktesBauteil_Einbauort, res.o))
            ins.add((res.kappenBauteil, ANS.AbstraktesBauteil_Einbauort, Literal("Rechte Aussenkappe")))
            ins.add((newKappe, ANS.associatedWith, res.kappenBauteil))
            ins.add((newKappe, ANS.associatedWith, res.kappenart))

        # alle einfacheBauteilarten in Entwässerung umschreiben, die Teil der Entwässerung sind
        for drain_element in g.query("""select ?bauteilcls  ?entBauteil 
                                           where {
                                               { 
                                               ?entBauteil a ?bauteilcls.
                                               ?bauteilcls rdfs:subClassOf* asb:AbstraktesBauteil.
                                                ?entBauteil asb:associatedWith ?bauteilart.
                                                ?bauteilart a ?artcls.
                                                ?artcls rdfs:subClassOf* asb:AbstrakteBauteilart.
                                                ?bauteilart  asb:EinfacheBauteilart_Art ?keyArt.
                                                ?keyArt rdfs:comment ?comment.
                                                FILTER regex (?comment, 'Entwaesserung', 'i')
                                                }
                                                UNION
                                                {?entBauteil a ?bauteilcls.
                                               ?bauteilcls rdfs:subClassOf* asb:AbstraktesBauteil.
                                                ?entBauteil asb:associatedWith ?bauteilart.
                                                ?bauteilart a ?artcls.
                                                ?artcls rdfs:subClassOf* asb:AbstrakteBauteilart.
                                                ?bauteilart  asb:EinfacheBauteilart_Art ?keyArt.
                                                ?keyArt rdfs:comment ?comment.
                                                FILTER regex (?comment, 'abwasser', 'i')}
                                                }
                                                """, initNs={"asb": ANS}):
            ins.remove((drain_element.entBauteil, RDF.type, drain_element.bauteilcls))
            ins.add((drain_element.entBauteil, RDF.type, ANS.Entwaesserung))




        insLinked = createBDefElementLink(g,ins)

        elementsWithDamages =  prepareQuery("""select ?bauteil ?SchadenObj ?aoi
                                            where {
                                            ?bdef asb:beschreibtBauteil ?bauteil.
                                            ?SchadenObj asb:hatBauteilDefinition ?bdef. 
                                            OPTIONAL {
                                            ?aoi aoi:locatesDamage ?SchadenObj.
                                            }
                                            }
                                            """, initNs={"asb":ANS, "aoi":AOI})

        for result in insLinked.query(elementsWithDamages):
            #print(result)
            if result.aoi is not None:
                insLinked.add((result.bauteil, AOI.hasAreaOfInterest, result.aoi))
            else:
                insLinked.add((result.bauteil ,DOT.hasDamage, result.SchadenObj ))

        return insLinked

    else:
        return ins





        
