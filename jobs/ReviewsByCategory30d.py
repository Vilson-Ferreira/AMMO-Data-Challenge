from base import ClusterConnection
from models import GooglePlayStoreData

cluster = ClusterConnection.ClusterConnection()
cluster.Open()

data = GooglePlayStoreData.GooglePlayStoreData(cluster.Session, "D:\AMMO_Challenge\data\googleplaystore.csv")
data.GenerateDataframeWithoutNans()

# Chamada do cálculo
# Gravacao do arquivo

cluster.Close()