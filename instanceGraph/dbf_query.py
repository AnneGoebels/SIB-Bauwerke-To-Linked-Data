def dbf_query(filepath, ont):
    import os
    import sqlite3
    import dbf
    import random
    from rdflib import Graph, Namespace, URIRef
    from rdflib.plugins.sparql import prepareQuery

    #get connection to database
    conn = sqlite3.connect (r"C:\GitHub\TwinGen\scripts\instanceGraph\ASBING.db")
    c=conn.cursor()

    #create current table 
    c.execute ('drop table if exists CURRENT_BRIDGE')

    conn.commit()

    c.execute('''
    CREATE TABLE [CURRENT_BRIDGE](
        [TAB] [varchar],
        [ID] INTEGER PRIMARY KEY,
        [ASBID][varchar]  NULL,
        [SUBJECT] [varchar] NOT NULL,
        [PROPERTY][varchar] NOT NULL,
        [OBJECT][varchar]  NULL,
        [FIXVALUE][varchar] NULL)
    ''')

    conn.commit()

    #functions
# get mapped classes and props
    def getNewOntTypes (OldTable, OldColumn):
        c.execute("select Ontology_Class, Ontology_ObjectProperty, Fixed_KeyValue from MAPPING where (QTabelle = (?) and QName = (?))", (OldTable.upper(), OldColumn.upper()))
        x = c.fetchone()
        if x != None:
            if x != (None, None, None) and x[1] != None:
                return x
            
            else:
                return "ENTFAELLT"
        else:
            return "ENTFAELLT"
        
#create id for new instances 
    def randomnumber(N):
        minimum = pow(10, N-1)
        maximum = pow(10, N) - 1
        id = random.randint(minimum, maximum)
        return "N_"+str(id)

    #ont = Graph()
    #ont.parse(r"C:\GitHub\TwinGen\Ontologies\ASB_Ontology_Merged_Nov21.ttl", format = "turtle")
    ANS = Namespace("https://w3id.org/asbingowl/core#")
    
    def checkSubClRel(class1, class2):
        check_query = prepareQuery(
        """ ask { 
        {?class1 rdfs:subClassOf ?class2 }
        UNION {?class2 rdfs:subClassOf ?class1 }
        UNION {?class2 rdfs:subClassOf/rdfs:subClassOf ?class1}}""")
        for answer in ont.query (check_query, initBindings={'class1':URIRef(ANS+class1), 'class2': URIRef(ANS+class2)}):
            #print(class1 , class2, answer)
            return answer
    
    
    def getRelation(class1, class2):
        rel_query = prepareQuery(
        """select ?p ?x ?y
        where
        {?class1 rdfs:subClassOf* ?x.
        ?class2 rdfs:subClassOf* ?y.
        ?x ?p ?y.
        FILTER(STRSTARTS(STR(?y), "https://w3id.org/asbingowl/core#"))
        }
        """)
        for relClLabel in ont.query (rel_query,initBindings= {'class1':URIRef(ANS [class1]), 'class2': URIRef(ANS [class2])}):
            #print(relClLabel)
            if "ASBObjekt" in relClLabel[1] or "ASBObjekt" in relClLabel[2]:
                pass
            else:
                return relClLabel[0]
    
    

#acess to exported SIB-BW data
    path = filepath
    files = os.listdir(path)

    zeroProps = ("Schaden_SchadensbewertungDauerhaftigkeit", "Schaden_SchadensbewertungVerkehrssicherheit", "Schaden_SchadensbewertungStandsicherheit")
    #workaround da sonst zu langsam:
    JaNeinDict = {"20141000000000"  : "true",
                  "20202000000000"  : "false",
                  "320081000000000" : "true",
                  "320082000000000" : "false",
                  "30182000000000"  : "false",
                  "30262000000000"  : "false",
                  "200071000000000" : "true",
                  "200112000000000" : "false",
                  "150021000000000" : "false",
                  "150022000000000" : "true",
                  "020142000000000" : "false",
                   "20142000000000" : "false"}
    notMappedTabCol = []
    
    #process files in cab
    for f in files:
        file_name, file_extension = os.path.splitext(f)
        #filter for dbf files
        if file_extension == ".DBF" and file_name != "schad_empf":
            t_path = os.path.join(path,f)
            obj_table = dbf.Table(t_path)  
            obj_table.open()
            
            col_names = obj_table.field_names
            
            #process each row (= one element)
            for record in obj_table:
                rows = []
        
                for col in col_names:
                    value = record[str(col)]
                    #filter empty entries
                    v = str(value).strip() 
                    #search new ont types 
                    
                    cl_prop = getNewOntTypes(file_name, col)
                    if cl_prop[0] == None:
                        print(cl_prop)
                    if cl_prop != "ENTFAELLT":
                        
                        #if value empty or zero
                        if v != "" and  v != '0' and v!='0.0' and v != "***" :
                            
                            if v in JaNeinDict:
                                v = JaNeinDict[v]
                               #print(v)
                            else:
                                v=v
                            
                            #fill table with data
                            c.execute("insert into CURRENT_BRIDGE (TAB,SUBJECT,PROPERTY,OBJECT,FIXVALUE) values (?,?,?,?,?) ", (file_name, cl_prop[0], cl_prop[1], v, cl_prop[2]))
                            row = c.lastrowid
                            conn.commit()
                            rows.append(row)
                    
                    #if zero is a valid value
                        elif v != "" and cl_prop[1] in zeroProps:
                            #fill table with data
                            c.execute("insert into CURRENT_BRIDGE (TAB,SUBJECT,PROPERTY,OBJECT,FIXVALUE) values (?,?,?,?,?) ", (file_name, cl_prop[0], cl_prop[1], v, cl_prop[2]))
                            row = c.lastrowid
                            conn.commit()
                            rows.append(row)
                        
                        else:
                            pass
                    
                    else:
                        pass
                       # if v != "" and  v != '0':
                        #    noMapDic ={}
                         #   noMapDic['tab']=file_name
                          #  noMapDic['col']=col
                           # noMapDic['val']= v
                #            notMappedTabCol.append(noMapDic)
                
                if len(rows) != 0:
                
                    #assign asbid to each row of same element
                    c.execute("select SUBJECT, OBJECT from CURRENT_BRIDGE where(FIXVALUE= 'ASB-Objekt-ID' and ID BETWEEN ? and ?)",(rows[0], rows[-1]))
                    asbid_cl = c.fetchall()
                    
                    
                    for i in asbid_cl:
                        if i[1] == '      ':
                            id = i[1].replace('      ',(randomnumber(7)))  
                        else:
                            id = i[1]
                        id.strip()

                        c.execute ("update CURRENT_BRIDGE set ASBID = ? where (SUBJECT =? and ID BETWEEN ? and ?) ", (id,i[0],rows[0], rows[-1]))
                        conn.commit()

                    #create ids for objects wihtout id in old version
                    newClasses= []
                    c.execute("select distinct SUBJECT from CURRENT_BRIDGE where(ASBID IS NULL and ID BETWEEN ? and ?)",(rows[0], rows[-1]))
                    empty_classes = list(c.fetchall())
                    #print(empty_classes)
                    
                    for cl in empty_classes:
                        #for orig_cl in asbid_cl:
                       # print(asbid_cl, cl)
                        if len(asbid_cl) > 0:
                            subRel1 = checkSubClRel(asbid_cl[0][0],cl[0])
                            try:
                                subRel2 = checkSubClRel(asbid_cl[1][0],cl[0])
                                #print(asbid_cl[1][0])
                            except:
                                subRel2 = False


                            if subRel1 == True:
                                c.execute ("update CURRENT_BRIDGE set ASBID = ? where (SUBJECT =? and ID BETWEEN ? and ?) ", (asbid_cl[0][1],cl[0],rows[0], rows[-1]))
                                conn.commit()
                                #print("trueSubclass",asbid_cl[0][0], cl[0])
                                #empty_classes.remove(cl)

                            elif subRel2 == True:
                                c.execute ("update CURRENT_BRIDGE set ASBID = ? where (SUBJECT =? and ID BETWEEN ? and ?) ", (asbid_cl[1][1],cl[0],rows[0], rows[-1]))
                                conn.commit()
                                #print("trueSubclass",asbid_cl[1][0], cl[0])

                            else:
                                #print(cl[0])
                                newClasses.append(cl[0])

                        else:
                            pass
                            
                            
                    
                    #print(newClasses)         
                    for newCl in newClasses:
                        id = randomnumber(7)
                        c.execute ("update CURRENT_BRIDGE set ASBID = ? where (SUBJECT =? and ID BETWEEN ? and ?) ", (id,newCl,rows[0], rows[-1]))
                        conn.commit()
                        c.execute ("insert into CURRENT_BRIDGE (ASBID, SUBJECT, PROPERTY, OBJECT, FIXVALUE) values (?,?,?,?,?)",(id,newCl,"ObjektMitID_hatObjektID, ObjektID_NamensraumVerfahren, ObjektID_ID",id,"ASB-Objekt-ID") )
                        conn.commit()
                    
                    
                    c.execute("select distinct SUBJECT, ASBID from CURRENT_BRIDGE where (ID BETWEEN ? and ?) ",(rows[0], rows[-1]))
                    diffClasses = c.fetchall()
                    #print(diffClasses)
                    if len(diffClasses)>1:
                   
                        for clId in diffClasses :
                            clSub=clId[0]
                            
                            for clId2 in diffClasses:
                                cl2Sub=clId2[0]
                                
                                if clSub == cl2Sub:
                                    pass
                                else:

                                    relProp = getRelation(clSub, cl2Sub)
                                   # print(clSub, cl2Sub,relProp)
                                    if relProp is not None:
                                        if  relProp == URIRef("https://w3id.org/asbingowl/core#associatedWith"):
                                            c.execute("insert into CURRENT_BRIDGE (ASBID, SUBJECT,PROPERTY,OBJECT)values (?,?,?,?) ",(clId[1], clSub,'associatedWith',clId2[1]))
                                            conn.commit()
                                            #print("assoGemacht")
                                        elif  relProp == URIRef("https://w3id.org/asbingowl/core#isPartOf"):
                                            c.execute("insert into CURRENT_BRIDGE (ASBID, SUBJECT,PROPERTY,OBJECT)values (?,?,?,?) ",(clId[1], clSub,'isPartOf',clId2[1]))
                                            conn.commit()
                                            #print("partOFgemacht")
                                            
                                        
                                        elif 'subClassOf' in relProp:
                                            pass
                                        else:
                                            print ("nichtverabeitet",relProp)
                                    else:
                                        pass
                                        #print(clSub, cl2Sub)
        
                    
        if file_extension == ".DBF" and file_name == "schad_empf":
            t_path = os.path.join(path,f)
            obj_table = dbf.Table(t_path)  
            obj_table.open()
            
            for record in obj_table:
            
                schad_id =  record['schad_id'].strip()
                empf_id =  record['empf_id'].strip()
        
            
                c.execute("select ASBID from CURRENT_BRIDGE where (PROPERTY = 'Schaden_ID-Nummer-Schaden' and OBJECT =?)", (schad_id,))
                zugSchad = c.fetchall()
                #print(zugSchad)
                c.execute("select ASBID from CURRENT_BRIDGE where (PROPERTY = 'Massnahmeempfehlung_ID-Nummer-Massnahmeempfehlung' and OBJECT =?)", (empf_id,))
                zugMass = c.fetchone()
                
                for a in zugSchad :  
                    c.execute("insert into CURRENT_BRIDGE (ASBID, SUBJECT, PROPERTY,OBJECT) values (?,?,?,?)", (zugMass[0],'Massnahmeempfehlung','associatedWith',a[0]))
        
                conn.commit()
            
        if file_extension == ".DBF" and file_name == "ges_bw":
            t_path = os.path.join(path,f)
            obj_table = dbf.Table(t_path)  
            obj_table.open()
            
        
            for record in obj_table:
            
                bw_id =  record['bwnr'].strip()

        #check if Bauteilinfos are availible
        if file_extension == ".DBF" and file_name == "bruecke":
            t_path = os.path.join(path, f)
            obj_table = dbf.Table(t_path)
            obj_table.open()
            if obj_table.__len__() == 0:
                containsBauteile = False
            else:
                containsBauteile = True



    ## Status für Schäden und Prüfungen später mit sparql erstellen
       
    #ID aktuelle pr�fung mit verb, tbw finden
   # c.execute("select distinct ASBID, OBJECT from CURRENT_BRIDGE where TAB = 'akt_pruf' and PROPERTY = 'associatedWith' and SUBJECT = 'PruefungMitZustandsnote'")
   # aktPrufIds= c.fetchall()
    #
    #ohne Berücksichtigung versch. TBW
  #  c.execute("select distinct ASBID from CURRENT_BRIDGE where TAB = 'akt_pruf' and SUBJECT = 'PruefungMitZustandsnote'")
  #  aktPrufIds= c.fetchall()
    #print(aktPrufIds)
    
  #  for prufId in aktPrufIds:
        #aktuelle Pr�fungen mit Status "abgeschlossen" versehen
  #      c.execute("insert into CURRENT_BRIDGE (ASBID, SUBJECT, PROPERTY, OBJECT) values (?,?,?,?)", (prufId[0], 'PruefungMitZustandsnote', 'PruefungUeberwachung_Status','5'))
  #      conn.commit()
    
   #    #ID alte Pr�fungen
    #c.execute("select distinct ASBID from CURRENT_BRIDGE where TAB = 'prufalt' and SUBJECT =  'PruefungMitZustandsnote'")
    #pruf_oldid = c.fetchall()
    #for p_oi in pruf_oldid :
     #   #mit Status "ersetzt" versehen
      #  c.execute("insert into CURRENT_BRIDGE (ASBID, SUBJECT, PROPERTY,OBJECT) values (?,?,?,?)", (p_oi[0], 'PruefungMitZustandsnote', 'PruefungUeberwachung_Status','6'))
      #  conn.commit()
    
    
    print(bw_id,containsBauteile)
    return bw_id, containsBauteile

    
