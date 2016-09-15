import numpy as np
import csv
import matplotlib.pyplot as plt 

#datasets = [101,170,4]
datasets = [999]

xy = []

for d in datasets:
    
    #read from input files
    xy = np.genfromtxt ('dataset'+str(d)+'.csv', delimiter=",")
    centroids = np.genfromtxt ('centroids'+str(d)+'.csv', delimiter=",")
    circumcenters= np.genfromtxt ('circumcenters'+str(d)+'.csv', delimiter=",")
    pivots = np.genfromtxt ('pivots'+str(d)+'.csv', delimiter=",")
    centroid_triangles = np.genfromtxt ('centroid_triangles'+str(d)+'.csv', delimiter=",")
    circumcircle_triangles = np.genfromtxt ('circumcircle_triangles'+str(d)+'.csv', delimiter=",")

    #plot triangulation using triangle centroids
    # create the scatter plot
    plt.scatter(xy[:, 0], xy[:, 1], c='blue')
    plt.scatter(pivots[:, 0], pivots[:, 1], c='red')
    plt.scatter(centroids[:, 0], centroids[:, 1], c='yellow')

    #draw lines of triangulation
    for i in range(0, len(centroid_triangles), 3):
        plt.plot([centroid_triangles[i][0], centroid_triangles[i+1][0]], [centroid_triangles[i][1], centroid_triangles[i+1][1]])
        plt.plot([centroid_triangles[i+1][0], centroid_triangles[i+2][0]], [centroid_triangles[i+1][1], centroid_triangles[i+2][1]])
        plt.plot([centroid_triangles[i+2][0], centroid_triangles[i][0]], [centroid_triangles[i+2][1], centroid_triangles[i][1]])
    
    plt.savefig('centroids'+str(d)+'.png')
    plt.close()

    #plot triangulation using triangle circumcenters
    # create the scatter plot
    plt.scatter(xy[:, 0], xy[:, 1], c='blue')
    plt.scatter(pivots[:, 0], pivots[:, 1], c='red')
    plt.scatter(circumcenters[:, 0], circumcenters[:, 1], c='yellow')

    #draw lines of triangulation
    for i in range(0, len(circumcircle_triangles), 3):
        plt.plot([circumcircle_triangles[i][0], circumcircle_triangles[i+1][0]], [circumcircle_triangles[i][1], circumcircle_triangles[i+1][1]])
        plt.plot([circumcircle_triangles[i+1][0], circumcircle_triangles[i+2][0]], [circumcircle_triangles[i+1][1], circumcircle_triangles[i+2][1]])
        plt.plot([circumcircle_triangles[i+2][0], circumcircle_triangles[i][0]], [circumcircle_triangles[i+2][1], circumcircle_triangles[i][1]])
    
    plt.savefig('circumcircles'+str(d)+'.png')
    plt.close()


