import numpy as np
import csv
import matplotlib.pyplot as plt 
import matplotlib as lib
#datasets = [101,170,4]
datasets = [999]
xy = []
pivots = []

for d in datasets:
    
    #read from input files
    #format: x, y, voronoi partition num, centroid partition num, circumcirle partition num
    xy = np.genfromtxt ('results'+str(d)+'.csv', delimiter=",")
    centroids = np.genfromtxt ('centroids'+str(d)+'.csv', delimiter=",")
    circumcenters= np.genfromtxt ('circumcenters'+str(d)+'.csv', delimiter=",")
    pivots = np.genfromtxt ('pivots'+str(d)+'.csv', delimiter=",")        

    colors = ['red','green','orange', 'blue','yellow', 'purple','pink','grey']

    #first plot initial voronoi partitioning
    plt.scatter(xy[:, 0], xy[:, 1], c=xy[:,2], cmap=lib.colors.ListedColormap(colors))
    plt.scatter(pivots[:, 0], pivots[:, 1], c='black', s=50)
    plt.savefig('voronoi_partitioning'+str(d)+'.png')
    plt.close()
    
    #then plot new partitioning using triangulation centroids
    plt.scatter(xy[:, 0], xy[:, 1], c=xy[:,3], cmap=lib.colors.ListedColormap(colors))
    plt.scatter(centroids[:, 0], centroids[:, 1], c='black', s =50)
    plt.savefig('triangulation_centroids_partitioning'+str(d)+'.png')
    plt.close()

    #then plot new partitioning using triangulation circumcenters
    plt.scatter(xy[:, 0], xy[:, 1], c=xy[:,4], cmap=lib.colors.ListedColormap(colors))
    plt.scatter(circumcenters[:, 0], circumcenters[:, 1], c='black', s = 50)
    plt.savefig('triangulation_circumcenters_partitioning'+str(d)+'.png')
    plt.close()
        
    


