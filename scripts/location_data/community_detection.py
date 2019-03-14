import community
import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd


df = pd.read_csv('jaccard_hsahtag_final.tsv', names=["Hashtags", "Jaccard"], sep='\t')

def filter_df(x):
    nodes = x.split(',')
    if(len(nodes) > 1):
        edgelist.append((nodes[0].strip('[]()\\ '), nodes[1].strip('[]()\\ ')))
    return nodes[0].strip('[]() ')

df['Nodes'] = list(map(lambda x: filter_df(x), df['Hashtags']))
G = nx.Graph()
G.add_nodes_from(list(df['Nodes'])[1:])
jaccard = list(df['Jaccard'][1:])
i = 0
for edge in list(df['Hashtags'][1:]):
    edge_list = edge.split(',')
    G.add_edge(edge_list[0].strip("() "), edge_list[1].strip("() "), weight=jaccard[i])
    i += 1
# # #first compute the best partition
partition = community.best_partition(G)
with open('community_detect.csv', 'w') as csv_file:
    writer = csv.writer(csv_file)
    for key, value in partition.items():
        writer.writerow([key, value])

modd = community.modularity(partition, G)
print("Modularity Score: {}".format(modd))
