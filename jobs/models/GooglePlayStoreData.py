###############################################################
# GooglePlayStore Class
# ------------------------
# Abstração do acesso aos dados da Google Play Store
# Vilson Ferreira.
###############################################################
from pyspark.sql import functions

class GooglePlayStoreData():

    # Construtor (inicialização das configs de dados)
    def __init__(self, sparksession, filePath):
        self._fileName = filePath
        self._delimiter = ','
        self._decimal = '.'
        self._quote = '"'
        self._escape = '"'
        self._sparksession = sparksession
        self._dataframe = None

    @property
    def fileName(self):
        return self._fileName

    @property
    def delimiter(self):
        return self._delimiter

    @property
    def decimal(self):
        return self._decimal

    @property
    def quote(self):
        return self._quote

    # Criação do Dataframe Spark
    def GenerateDataframe(self):
        self._dataframe = self._sparksession.read.csv(self._fileName, sep=self._delimiter, quote=self._quote, header=True, escape=self._escape)
        return self._dataframe

    # Criação do Dataframe Spark sem NaNs.
    def GenerateDataframeWithoutNans(self):
        self._dataframe = self.GenerateDataframe().na.drop()
        return self._dataframe

    # Media de Rating por Categoria
    def RatingMeanByCategory(self):
        return self._dataframe.filter(~functions.isnan("Rating") & ~functions.isnull("Rating")).groupBy("Category").agg(functions.mean("Rating").alias("Mean"))

    # Media de Size por Genres
    def SizeByGenres(self):
        return self._dataframe.filter("Size != 'Varies with device'").groupBy("Genres").agg(functions.mean("Size").alias("Mean"), functions.stddev("Size").alias("Std"))