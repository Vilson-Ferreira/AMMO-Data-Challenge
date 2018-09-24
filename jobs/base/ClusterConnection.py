###############################################################
# Cluster Connection Class
# ------------------------
# Abstração de conexção ao cluster.
###############################################################
from pyspark import SparkContext
from pyspark.sql import SparkSession

class ClusterConnection():

    # Construtor
    def __init__(self):
        self._sc = None
        self._ss = None
        self._connection = False

    # Contexto Spark
    @property
    def Context(self):
        return self._sc

    # Session Spark
    @property
    def Session(self):
        return self._ss

    # Conexão ao Spark
    def Open(self):
        
        self._connection = False
        
        try:
            self._sc = SparkContext()
            self._ss = SparkSession(self._sc)
            self._conection = True    

        except:
            self._connection = False
            # Log de erros conforme padrão da empresa.

        return self._connection

    # Desconexão ao Spark
    def Close(self):

        self._sc.stop()