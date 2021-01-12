import os
from google.cloud import storage
import pandas as pd
# import community
import networkx as nx
import matplotlib.pyplot as plt
import time
import os
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/ir177/Documents/ID/GCP-CSET Projects-e90692810866 Storage.json"
import glob
from helpers.gcs_storage import list_blobs, delete_blob, download_blob, upload_blob
import pandas as pd
import csv
import gzip
import igraph as ig
import leidenalg as la
import igraph as ig
import pickle
import cairo
from itertools import combinations
from scipy.special import comb as Ncomb
from multiprocessing import Pool
import seaborn as sns
import numpy as np
import random
import tqdm


## Data preparation functions


# Prepares data for Clustering
def prepare_Leiden_Cnet(Cnet):
    #Normilize CNet weights to fractions
    # calculate total weight of connections per paper
    sumC = Cnet.groupby(['id'])['count'].sum()
    # save total connection weights
    sumC = sumC.to_frame()
    sumC['id'] = sumC.index
    sumC = sumC.reset_index(drop=True)
    # Match total connection weight to each paper
    Cnet = Cnet.merge(sumC,  left_on='id', right_on='id')
    Cnet['weight'] = Cnet['count_x'] / Cnet['count_y']
    # Normilize connection weights by dividing by the sum of connections. All connection weights are in (0,1]
    Cnet = Cnet.drop(columns = ['count_x', 'count_y'])
    #end of scaling
    #prepare data for Leiden in tuples
    tuples = [tuple(x) for x in Cnet.values]
    # load data into iGraph to be fed to leiden
    Gm = ig.Graph.TupleList(tuples, directed=False, edge_attrs=['weight'])
    # Data of paper IDs
    ID_list = Gm.vs['name']
    # create a list of IDs
    Cluster_main = pd.DataFrame(ID_list)
    Cluster_main.columns = ['id']
    # Set base clusters to None
    Cluster_main['ClusterID'] = None
    Cluster_main.index = Cluster_main['id']
    # Export template to cluster-map output (Cluster_main), and processed connections data (Cnet)
    return Cluster_main, Cnet




# Function creates identified a layers of clusters
def networkLV(Cluster_main, Cnet,  tailcut, level):
    clustlist = Cluster_main['ClusterID'].unique()
    N_Clusters = len(clustlist)
# Loop over the clustering to level:
    levelName = 'Cluster' + str(level)
    Cluster_main[levelName] = None
    for i in range(0, N_Clusters ):
        # Break only clusters has more than 50 papers
        if (len(Cluster_main.loc[Cluster_main['ClusterID'] == clustlist[i]]) > 50) or (level == 1):
        # extract next level clusters
            IDg =  LeidenCom(Cnet, Cluster_main, levelName, clustlist[i],  tailcut)
            # print cluster size distribution
            #print(IDg[levelName].value_counts())
            IDg = IDg.set_index('id')
            #print("Running cluster", i)
            # cluster name to the list
            IDg[levelName] = IDg[levelName] + 1
            # set Cluster name
            Cluster_main[levelName] = Cluster_main[levelName].fillna(IDg[levelName])
            #print(Cluster_main.isnull().sum())
            del IDg
    # Number of clusters in a lelve
    max_clust = Cluster_main[levelName].max()
    # if no clusters were broken report it
    if np.isnan(max_clust):
        #print('No clusters were added in the level ', level)
        max_clust = 1
    # cluster naming concetions. If clusters are 1,25, 45, then we normilzie names to 10, 35, 55, etc + adding 10.
    dec = 10**(len(str(max_clust)))
    max_in = max_clust + 1
    # Where no new clusters appeared (residual papers) the cluster name is max_cluster + 1
    Cluster_main[levelName] = Cluster_main[levelName].fillna(max_in)
    Cluster_main[levelName] = Cluster_main[levelName] + dec
    # first gen create cluster ID = level 1
    if level == 1:
        Cluster_main['ClusterID'] = Cluster_main[levelName].astype(int)
    # other generation the name of the cluster should include the name of the parent cluster + child cluster
    else:
        Cluster_main['ClusterID'] = Cluster_main['ClusterID'].astype(int).astype(str) + Cluster_main[levelName].astype \
            (int).astype(str)
    Cluster_main['ClusterID'] = Cluster_main['ClusterID'].astype(int)
    Cluster_main[levelName] = Cluster_main['ClusterID']
    # return a map of clusters
    return Cluster_main





# clusterN is the current level of ClusterID to be disaggregated
# Function to estimate Leiden community for a specific cluster
def LeidenCom(Cnet, Cluster_main, generation1, clusterN, tailcut):
    # keep only records for ClusterN
    # If the first generation clusterN is None and run the whole data
    if clusterN is None:
        cluster1 = Cluster_main.index[Cluster_main['ClusterID'].isnull()]
    else:
    # if not run specific cluster
        cluster1 = Cluster_main.index[Cluster_main['ClusterID'] == clusterN]
    cluster1 = cluster1.to_frame()
    cluster1.columns = ['id']
    # Keep only the network that is part of clusterN
    Cnet1 = Cnet[Cnet['id'].isin(cluster1['id'])]
    Cnet1 = Cnet1[Cnet['ref_id'].isin(cluster1['id'])]
    sample = Cnet1
    # prepare data to ipgrah
    tuples = [tuple(x) for x in sample.values]
    # load data into iGraph
    Gm = ig.Graph.TupleList(tuples, directed=False, edge_attrs=['weight'])
    # Estimate communities with weighted edges. Stop iterations after no improvement in possible
    partition = la.find_partition(Gm, la.ModularityVertexPartition,  weights='weight', n_iterations = -1)
    # export cluster membership
    gen1 = pd.DataFrame(partition.membership)
    gen1.columns = [generation1]
    # export node IDs
    ID_list = Gm.vs['name']
    ID = pd.DataFrame(ID_list)
    ID.columns = ['id']
    # merge node IDs with clusters
    ID_g1 = pd.concat([ID, gen1], axis=1)
    Clsum = 0
    # average cluster size
    for i in range(0, max(ID_g1[generation1])):
        # average cluster size of the largest i clusters. Tail_cut keeps only the largest clusters to mimic
        # CSO or other classifications, it tends to have 10-20 clusters each level
        if tailcut == True:
            meanCl = ID_g1[generation1].value_counts()[0: i +1].mean()
        # add weight of the current cluster to the average
            Clsum = Clsum + ID_g1[generation1].value_counts()[i]
        # calculate the weight of the tail of the (n-i) smallest clusters
            tail = len(ID_g1) - Clsum
        # if the tail is smaller than the average of i largest clusters or smaller than 15, aggregate the tail
        # in the Miscelamneous cluster
        else:
            tail = 1
            meanCl = 0
        if (tail < meanCl) or ID_g1[generation1].value_counts()[i] < 15:
            # replace values to create tail
            ID_g1.loc[ID_g1[generation1] > i, generation1] = i
            break
    # return results dataframe of IDs and ClusterID
    return ID_g1

# Function to build several layers of network.
def NTKBuild(Cluster_main, Cnet,  maxlevel, tailcut = True):
    for i in range(1 ,maxlevel + 1):
        # add layers to Network
        Cluster_main = networkLV(Cluster_main, Cnet,  tailcut, i)
    return Cluster_main