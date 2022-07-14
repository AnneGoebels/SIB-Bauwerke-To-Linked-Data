from rdflib import Literal, Namespace, URIRef
from rdflib.plugins.sparql.processor import prepareQuery
import sqlite3


def createBauteilDefTable(g) :
    # get connection to database
    conn = sqlite3.connect(r'C:\GitHub\TwinGen\scripts\instanceGraph\ASBING.db')
    c = conn.cursor()

    # create bauteildef table
    c.execute('drop table if exists BAUTEILDEF')

    conn.commit()

    c.execute('''
       CREATE TABLE [BAUTEILDEF](
           [TEILBAUWERK] [varchar],
           [SCHADOBJ] [varchar],
           [SCHADID] [int],
           [UNIONID] [int] NULL,
           [KONTEIL] [varchar] NULL,
           [BAUTEIL][varchar] NULL,
           [GRUPPE][varchar]  NOT NULL,
           [AOI][varchar] NULL)
       ''')

    conn.commit()

    ANS = Namespace("https://w3id.org/asbingowl/core#")
    #INS = Namespace("http://example.org/sibbw"+bw_nr+"#")
    AOI = Namespace ("https://w3id.org/aoi#")
    #ANSK = Namespace("https://w3id.org/asbingowl/keys#")

    schadenObjekt = prepareQuery("""select distinct ?SchadenObj ?SchadenId ?tbw
                            where  {?schaden a asb:Schaden;
                                    asb:Schaden_ID-Nummer-Schaden ?SchadenId;
                                    asb:associatedWith ?tbw.
                                    ?SchadenObj a asb:SchadenObjekt;
                                    asb:Schaden_ID-Nummer-Schaden ?SchadenId;
                                    asb:associatedWith ?tbw.
                                    ?tbw a asb:Teilbauwerk.
                                    }
                            """, initNs={"asb" :ANS})

    KonBauteilGrupDef = prepareQuery(""" SELECT DISTINCT ?gruppe ?bauteil ?konteil
                                        WHERE 
                                        {?s a asb:Schaden.
                                        ?s asb:Schaden_ID-Nummer-Schaden ?SchadenId.
                                        ?s asb:ASBING13_Bauteilgruppe ?gruppe .
                                        ?s asb:associatedWith ?tbw.
                                        OPTIONAL { ?s asb:ASBING13_Bauteil ?bauteil. }
                                        OPTIONAL { ?s asb:ASBING13_Konstruktionsteil ?konteil. }
                                        }
                                        """, initNs={"asb": ANS})

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

    getSuperCl = prepareQuery(""" select ?supercl 
                                   where { ?keycl rdfs:subClassOf ?supercl.
                                   FILTER NOT EXISTS {?supercl rdfs:subClassOf asb:GesamteSchluessel.}
                                   }
                                   """,initNs ={"asb":ANS} )

    askSuperLabel = prepareQuery("ask {?keycl1 rdfs:subClassOf* ?root1. ?keycl2 rdfs:subClassOf* ?root2. ?root1 rdfs:label ?label. ?root2 rdfs:label ?label. MINUS {?root rdfs:label 'GesamteSchluessel'.} }")
    askSuperCl = prepareQuery("""ask {
                                 {?keycl1 rdfs:subClassOf* ?keycl2. }
                                 UNION {?keycl2 rdfs:subClassOf* ?keycl1.} }""")
    askLabel = prepareQuery("ask {?keycl1 rdfs:label ?label. ?keycl2 rdfs:label ?label.}")

    for row in g.query(schadenObjekt):
       #print(row)

        for bDef in g.query(KonBauteilGrupDef, initBindings={'SchadenId':Literal(row.SchadenId),'tbw':URIRef(row.tbw)}):
           #print(bDef)
            c.execute("insert into BAUTEILDEF (TEILBAUWERK,SCHADOBJ,SCHADID,KONTEIL,BAUTEIL,GRUPPE) values (?,?,?,?,?,?)",
                      (row.tbw,row.SchadenObj,row.SchadenId,bDef.konteil,bDef.bauteil,bDef.gruppe))
            conn.commit()

        aoiList = []
        for orte in g.query(ortBauteilDef, initBindings={'SchadenId':Literal(row.SchadenId),'tbw': URIRef(row.tbw)}):
           #print(orte.group)
            try:
                grp = str(orte.group).split("Bauteilgruppe")[1]
            except:
                grp = str(orte.group).split("_")[1]
            try:
                c.execute("select " + grp + " from ORTNACHELEMENT where Ortsangaben = UPPER(?)", (orte.olabel,))
                res = c.fetchone()
               #print(res)
                if "Bauteil" in res:
                    aoiList.append(orte[:2])
                   #print(aoiList)
                else:
                   print("AOI" + orte.olabel)
            except Exception as E:
               print(E)

        if len(aoiList) > 0 :
            c.execute("update BAUTEILDEF set AOI = ? where ( SCHADID = ? and TEILBAUWERK = ?)",
                      (str(aoiList), row.SchadenId, row.tbw))
            conn.commit()


    c.execute("select distinct GRUPPE,TEILBAUWERK from BAUTEILDEF")
    gruppen = c.fetchall()

    for gruppe in gruppen:
       #print(gruppe)
        c.execute("select distinct AOI,KONTEIL from BAUTEILDEF where ( GRUPPE = ? and TEILBAUWERK = ?) ",(gruppe[0],gruppe[1]))
        aois = c.fetchall()
        #print(aois)

        for aoi in aois:
           #print(aoi)
            if aoi[0] is not None :
                c.execute("select SCHADID, KONTEIL, BAUTEIL from BAUTEILDEF where (AOI =? and GRUPPE = ? and TEILBAUWERK = ? )",(aoi[0],gruppe[0],gruppe[1]))
                sameBauteile = c.fetchall()
                if len(sameBauteile)>1:
                    origId = sameBauteile[0][0]
                    konteil = sameBauteile[0][1]
                    if konteil is not None:
                        for sameBauteil in sameBauteile[1:]:
                           #print(sameBauteil[1])
                            if sameBauteil[1] == konteil:
                                c.execute("update BAUTEILDEF set UNIONID =? where (SCHADID =? and TEILBAUWERK =?)",(origId,sameBauteil[0],gruppe[1]))
                                conn.commit()
                               #print("yay sofort")
                            else:
                               #print(konteil, sameBauteil[1])
                                if sameBauteil[1] is not None:
                                    sameLabel = g.query(askLabel, initBindings={"keycl1": URIRef(konteil), "keycl2": URIRef(sameBauteil[1])})
                                    sameSuperCl = g.query(askSuperCl, initBindings={"keycl1": URIRef(konteil), "keycl2": URIRef(sameBauteil[1])})
                                    sameSuperLabel = g.query(askSuperLabel, initBindings={"keycl1": URIRef(konteil), "keycl2": URIRef(sameBauteil[1])})
                                    for answer in sameLabel:
                                        y = [answer, ]
                                    for answer2 in sameSuperCl:
                                        y.append(answer2)
                                    for answer3 in sameSuperLabel:
                                        y.append(answer3)

                                    if True in y:
                                        c.execute("update BAUTEILDEF set UNIONID =? where (SCHADID =? and TEILBAUWERK =?)",
                                                  (origId, sameBauteil[0], gruppe[1]))
                                        conn.commit()
                                       #print("beim 2. mal",konteil, sameBauteil[1])

                                    else:
                                        pass


                                else:
                                    pass

                    else:
                        pass

            else:
                c.execute("select SCHADID, KONTEIL from BAUTEILDEF where (AOI is null and GRUPPE = ? and TEILBAUWERK =?)",(gruppe[0],gruppe[1]))
                konteile = c.fetchall()
                if konteile[0][1] is not None:
                    if len(konteile) > 1:
                        #print("KON", konteile)
                        test = {}
                        for k in konteile:
                            #print(k)
                            konteil = str(k[1])
                            #print(konteil)
                            existKeys = list(test.keys())
                           #print(existKeys)
                           #print(len(existKeys))
                            if len(existKeys) > 0:
                                if konteil in existKeys:
                                    idList = test[konteil]
                                    #print(idList)
                                    idList.append(k[0])
                                    test[konteil] = idList
                                else:
                                    itertimes = []
                                    for existKey in existKeys:
                                        sameLabel = g.query(askLabel, initBindings={"keycl1": URIRef(konteil), "keycl2": URIRef(existKey)})
                                        sameSuperCl = g.query(askSuperCl, initBindings={"keycl1": URIRef(konteil), "keycl2": URIRef(existKey)})
                                        sameSuperLabel = g.query(askSuperLabel, initBindings={"keycl1": URIRef(konteil), "keycl2": URIRef(existKey)})
                                        for answer in sameLabel:
                                            x = [answer, ]
                                        for answer2 in sameSuperCl:
                                            x.append(answer2)
                                        for answer3 in sameSuperLabel:
                                            x.append(answer3)
                                        #print(x)
                                        if True in x:
                                           #print("true", konteil, existKey)
                                            idList = test[existKey]
                                            idList.append(k[0])
                                            test[existKey] = idList
                                            break
                                        else:
                                           #print("false", konteil, existKey)
                                            itertimes.append("i")
                                    if len(itertimes) == len(existKeys):
                                        test[konteil] = [k[0], ]
                            else:
                                test[konteil] = [k[0], ]

                        #print(test.items())
                        for key, value in test.items():
                            #print(key, value)
                            if len(value) > 1:
                                unionId = value[0]
                                for schadIdB in value[1:]:
                                    c.execute("update BAUTEILDEF set UNIONID = ? where (SCHADID =? and TEILBAUWERK =?)", (unionId, schadIdB,gruppe[1]))
                                    conn.commit()
                else:
                    c.execute("select SCHADID, BAUTEIL from BAUTEILDEF where (AOI is null and GRUPPE = ? and TEILBAUWERK =?)",(gruppe[0],gruppe[1]))
                    bauteile = c.fetchall()
                    if len(bauteile) > 1:
                        #print("BAU", bauteile)
                        test = {}

                        #print(existKeys)
                        #print(len(existKeys))
                        for b in bauteile:
                            existKeys = list(test.keys())
                            #print(b)
                            if b[1] is None:
                                if str(gruppe[0]) in existKeys:
                                    idList = test[str(gruppe[0])]
                                    #print(idList)
                                    idList.append(b[0])
                                    test[str(gruppe)] = idList
                                else:
                                    test[str(gruppe)] = [b[0],]
                            else:
                                bauteil = str(b[1])
                               #print(bauteil)

                                if len(existKeys) > 0:
                                    if bauteil in existKeys:
                                        idList = test[bauteil]
                                        #print(idList)
                                        idList.append(b[0])
                                        test[bauteil] = idList
                                    else:
                                        itertimes = []
                                        for existKey in existKeys:
                                            sameLabel = g.query(askLabel, initBindings={"keycl1": URIRef(bauteil),"keycl2": URIRef(existKey)})
                                            sameSuperCl = g.query(askSuperCl, initBindings={"keycl1": URIRef(bauteil),"keycl2": URIRef(existKey)})
                                            sameSuperLabel = g.query(askSuperLabel,initBindings={"keycl1": URIRef(bauteil),"keycl2": URIRef(existKey)})
                                            for answer in sameLabel:
                                                x = [answer,]
                                            for answer2 in sameSuperCl:
                                                x.append(answer2)
                                            for answer3 in sameSuperLabel:
                                                x.append(answer3)
                                            #print(x)
                                            if True in x:
                                               #print("true", bauteil, existKey)
                                                idList = test[existKey]
                                                idList.append(b[0])
                                                test[existKey] = idList
                                                break
                                            else:
                                               #print("false",bauteil, existKey)
                                                itertimes.append("i")
                                        if len(itertimes) == len(existKeys):
                                            test[bauteil] = [b[0], ]
                                else:
                                    test[bauteil] = [b[0], ]

                        #print(test)
                        #print(test.items())
                        for key,value in test.items():
                           #print(key, value)
                            if len(value)> 1:
                                unionId = value[0]
                                for schadIdB in value[1:]:
                                    c.execute("update BAUTEILDEF set UNIONID = ? where (SCHADID =? and TEILBAUWERK =?)", (unionId, schadIdB,gruppe[1]))
                                    conn.commit()

    return "created BAUTEILDEF Table in DB"
        
