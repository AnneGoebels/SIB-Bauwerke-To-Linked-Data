def dbf_query(filepath):
    import os
    import sqlite3
    import dbf
    import random

    #get connection to database
    conn = sqlite3.connect ("ASBING.db")
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
        [OBJECT][varchar] NOT NULL,
        [FIXVALUE][varchar] NULL)
    ''')

    conn.commit()

    #functions

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
        

    def randomnumber(N):
        minimum = pow(10, N-1)
        maximum = pow(10, N) - 1
        id = random.randint(minimum, maximum)
        return "N_"+str(id)


    path = filepath
    files = os.listdir(path)

    zeroProps = ("Schaden_SchadensbewertungDauerhaftigkeit", "Schaden_SchadensbewertungVerkehrssicherheit", "Schaden_SchadensbewertungStandsicherheit")

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
                    if cl_prop != "ENTFAELLT":
                        
                        if v != "" and  v != '0':
                            
                            #fill table with data
                            c.execute("insert into CURRENT_BRIDGE (TAB,SUBJECT,PROPERTY,OBJECT,FIXVALUE) values (?,?,?,?,?) ", (file_name, cl_prop[0], cl_prop[1], v, cl_prop[2]))
                            row = c.lastrowid
                            conn.commit()
                            rows.append(row)
                    
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
                    
                    c.execute("select distinct SUBJECT from CURRENT_BRIDGE where(ASBID IS NULL and ID BETWEEN ? and ?)",(rows[0], rows[-1]))
                    classes = c.fetchall()
                    
                    for cl in classes:
                        id = randomnumber(7)
                        
                        c.execute ("update CURRENT_BRIDGE set ASBID = ? where (SUBJECT =? and ID BETWEEN ? and ?) ", (id,cl[0],rows[0], rows[-1]))
                        conn.commit()
                        c.execute ("insert into CURRENT_BRIDGE (ASBID, SUBJECT, PROPERTY, OBJECT, FIXVALUE) values (?,?,?,?,?)",(id,cl[0],"ObjektMitID_hatObjektID, ObjektID_NamensraumVerfahren, ObjektID_ID",id,"ASB-Objekt-ID") )
                        conn.commit()
                    
                    #check for relations between classes without association yet
                    #detailed fixes
                    #1 
                    c.execute("select distinct SUBJECT, ASBID from CURRENT_BRIDGE  where (SUBJECT ='Massnahmeempfehlung' or SUBJECT = 'Massnahmebewertung' or SUBJECT = 'BauUndErhaltungsmassnahme') and ID BETWEEN ? and ?  ",(rows[0], rows[-1]))
                    s = c.fetchall()

                    if len(s) >= 2:
                        empf = s[0]
                        bew = s[1]
                        
                        if len(s) == 3:
                            erh = s[2]
                            c.execute("insert into CURRENT_BRIDGE (ASBID, SUBJECT,PROPERTY,OBJECT)values (?,?,?,?) ",(bew[1], bew[0],'associatedWith',erh[1]))
                            conn.commit()
                            
                        c.execute("insert into CURRENT_BRIDGE (ASBID, SUBJECT,PROPERTY,OBJECT)values (?,?,?,?) ",(empf[1],empf[0],'associatedWith',bew[1]))
                        conn.commit()
                    #2
                    c.execute("select distinct SUBJECT, ASBID from CURRENT_BRIDGE  where (SUBJECT ='PruefungMitZustandsnote' or SUBJECT = 'Bauwerkszustand') and ID BETWEEN ? and ?  ",(rows[0], rows[-1]))
                    s = c.fetchall()
                    
                    if len(s)==2:
                        pr = s[0]
                        zu = s[1]
                        
                        c.execute("insert into CURRENT_BRIDGE (ASBID, SUBJECT,PROPERTY,OBJECT)values (?,?,?,?) ",(zu[1], zu[0],'associatedWith',pr[1]))
                        conn.commit()
                    
                    
        if file_extension == ".DBF" and file_name == "schad_empf":
            t_path = os.path.join(path,f)
            obj_table = dbf.Table(t_path)  
            obj_table.open()
            
        # col_names = obj_table.field_names
            
            #process each row (= one element)
            for record in obj_table:
            
                schad_id =  record['schad_id'].strip()
                empf_id =  record['empf_id'].strip()
        
            
                c.execute("select ASBID from CURRENT_BRIDGE where (PROPERTY = 'Schaden_ID-Nummer-Schaden' and OBJECT =?)", (schad_id,))
                a = c.fetchone()
                #print(a)
                c.execute("select ASBID from CURRENT_BRIDGE where (PROPERTY = 'Massnahmeempfehlung_ID-Nummer-Massnahmeempfehlung' and OBJECT =?)", (empf_id,))
                b = c.fetchone()
                
                c.execute("insert into CURRENT_BRIDGE (ASBID, SUBJECT, PROPERTY,OBJECT) values (?,?,?,?)", (a[0],'Schaden','associatedWith',b[0]))
                conn.commit()
                c.execute("insert into CURRENT_BRIDGE (ASBID, SUBJECT, PROPERTY,OBJECT) values (?,?,?,?)", (b[0],'Massnahmeempfehlung','associatedWith',a[0]))
                conn.commit()
                
                
                
    return "done"

        
    #4 

        
        