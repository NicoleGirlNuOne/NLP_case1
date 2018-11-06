import pandas as pd
import pickle
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

df = pd.read_pickle('Data/matrix_df.pkl')
# df = df.ix[:,3:]
terms = df.columns.tolist()
clf = KMeans(n_clusters = 10)
s = clf.fit(df)
output = open('Data/clf_fit.pkl','wb')
pickle.dump(s,output)
# s = pd.read_pickle('Data/clf_fit.pkl')
order_centroids = s.cluster_centers_.argsort()[:, ::-1]

for i in range(10):
    print("Cluster %d:" % i, end='')
    for ind in order_centroids[i, :10]:
        print(' %s' % terms[ind], end='')
    print()

# keyword = pd.read_pickle('Data/keywords.pkl')
# print(keyword)




















