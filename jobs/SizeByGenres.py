from base import ClusterConnection
from models import GooglePlayStoreData

cluster = ClusterConnection.ClusterConnection()
cluster.Open()

data = GooglePlayStoreData.GooglePlayStoreData(cluster.Session, "D:\AMMO_Challenge\data\googleplaystore_2.csv")
data.GenerateDataframeWithoutNans()
data.SizeByGenres().show()
data.SizeByGenres().toPandas().to_csv("D:/AMMO_Challenge/data/SizeByGenres.csv")

cluster.Close()