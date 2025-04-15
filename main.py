import configparser
import json
import neo4j_utilities
#import pypref as p
import pandas as pd
import matplotlib.pyplot as plt

from skyline import find_skyline_bnl

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('configs/flights.ini')


    USER = "PM"

    # Database connection parameters
    dbname = config[USER]['dbname']
    user = config[USER]['user']
    password = config[USER]['password']
    host = config[USER]['host']
    port = int(config[USER]['port'])


    # Replace these with your Neo4j instance details
    #NEO4J_URI = "bolt://localhost:7687"  # Typically starts with "bolt://"
    NEO4J_URI = "bolt://"+str(host)+":"+ str(port)  # Typically starts with "bolt://"

    column_names = ['id', 'degree','mean delay','mean sp length']
    # Create an empty DataFrame with the specified columns
    dfres = pd.DataFrame(columns=column_names)


    # Initialize the connection
    conn = neo4j_utilities.Neo4jConnection(uri=NEO4J_URI, user=user, password=password)

    # get airport ids
    tabIds=[]
    dictDegree={}
    dictMeanDelay={}
    dictMeanSpLength={}
    QUERY = "MATCH (n:Airport)-[]->(f:Flight) RETURN distinct(elementId(n)) as elementId"
    results = conn.query(QUERY)
    for result in results:
        #print(result['elementId(n)'])
        tabIds.append(result['elementId'])

    print(len(tabIds))

    # get direct connections (this is out degree since it is (airport)->(flight)->(airtport) )
    QUERY = "match(a1: Airport), (a1)-[*2]->(d1:Airport) return elementId(a1), count(distinct d1)"
    results = conn.query(QUERY)
    for result in results:
        dictDegree[result['elementId(a1)']] = result['count(distinct d1)']
    print(len(dictDegree))

    # get mean departure delay
    QUERY = "match (a1:Airport), (a1)-[]->(f:Flight) return elementId(a1),avg(f.departure_delay)"
    results = conn.query(QUERY)
    for result in results:
        dictMeanDelay[result['elementId(a1)']] = result['avg(f.departure_delay)']
    print(len(dictMeanDelay))


    for id in tabIds:
        QUERY = ("MATCH (d:Airport) where elementId(d)<>\"" + id +
                 "\" WITH collect(d) as nodes unwind nodes as n with n " +
                "MATCH (a:Airport), path = shortestPath((a)-[*]->(n)) where elementId(a)=\""
                 + id + "\" RETURN elementId(a),avg(length(path))")
        results = conn.query(QUERY)
        for result in results:
            dictMeanSpLength[result['elementId(a)']] = result['avg(length(path))']

    print(len(dictMeanSpLength))

    #insert into dataframe
    for id in tabIds:
        dfres.loc[len(dfres)] = [id,dictDegree[id],dictMeanDelay[id],dictMeanSpLength[id]]
    print(dfres.head())

    #compute skyline
    skyline = find_skyline_bnl(df=dfres, to_min=['mean delay','mean sp length'], to_max=['degree'])
    print(skyline[skyline==True].index)
    for pos in skyline[skyline==True].index:
        print(dfres.loc[pos] )

    # 2D plot
    #colors = skyline.map({True: 'C1', False: 'C0'})
    #plt.scatter(x=dfres['degree'], y=dfres['mean delay'],c=colors, alpha=0.8)
    #plt.show()

    conn.close()



