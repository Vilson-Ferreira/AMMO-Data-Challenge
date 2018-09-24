from base import ClusterConnection
from models import GooglePlayStoreData

cluster = ClusterConnection.ClusterConnection()
cluster.Open()

data = GooglePlayStoreData.GooglePlayStoreData(cluster.Session, "D:\AMMO_Challenge\data\googleplaystore.csv")
data.GenerateDataframeWithoutNans()
data.RatingMeanByCategory().show()
data.RatingMeanByCategory().toPandas().to_csv("D:/AMMO_Challenge/data/RatingByCategory.csv")

cluster.Close()