import os
import networkx as nx
import sqlite3
# bulide entity_centrality table: store the centrality of entity
conn = sqlite3.connect('/base_data/HSP_ALL.sqlite')
c = conn.cursor()
c.execute('''CREATE TABLE entity_centrality
       (ID INT PRIMARY KEY     NOT NULL,
        centrality         REAL);''')
conn.commit()
conn.close()

# Calculate the center of the entity by the pagerank algorithm
os.chdir('E:\\Entity_Profiles_v1\\')
filename = 'jamendo_relation.txt'
G = nx.DiGraph()
with open(filename) as file:
    for line in file:
        head, tail = [int(x) for x in line.split()]
        G.add_edge(head, tail)
pr = nx.pagerank(G, alpha=0.85)

# store the centrality of entity in the entity_centrality table
conn = sqlite3.connect('E:\\Entity_Profiles_v1\\jamendo.sqlite')
c = conn.cursor()
sql = ''' insert into entity_centrality (ID, centrality)
              values
              (:entity_id, :centrality)'''

for node, value in pr.items():
    c.execute(sql, {'entity_id': node, 'centrality': value})

conn.commit()
conn.close()
