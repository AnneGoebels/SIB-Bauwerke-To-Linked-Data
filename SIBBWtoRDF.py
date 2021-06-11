from dbf_query import dbf_query
from tableToRdf import tableToRdf
import os

p = r"C:\Users\Anne\Desktop\cab\next"
cabs = os.listdir(p)

for sibbwfile in cabs:
    file_name, file_extension = os.path.splitext(sibbwfile)
    print(file_name)
    
    path = os.path.join(p,sibbwfile)
    print(path)
    a = dbf_query(path)
    if a == "done":
        print(a)
        rdfGraph = tableToRdf()


    f = open (r"C:\GitHub\TwinGen\OutputGraphs\'"+file_name+"'.ttl","wb")
    f.write(rdfGraph.serialize( format='turtle'))