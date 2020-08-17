import sys
import os

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import substring

class etl_class():

    def __init__(self, activity_name,sc):
        self.spark=SparkSession. \
            builder. \
            master('local'). \
            appName(activity_name). \
            getOrCreate()

    def load_csv_parquet(self,inputfilepath):
	inputfile=inputfilepath+'/'+'weather*.csv'
	#print('file check ..',os.path.exists(inputfile))
	#print(inputfile)
	try:
            csvdf=self.spark.read.format('csv').\
                load('file:///'+inputfile \
                , header=True, inferSchema='True')
            # csvdf.show(1)
            outdf=csvdf.repartition(1).write.mode('overwrite').\
            parquet('file:///'+inputfilepath+'/'+'out_parquet')
	    return 1	

	except Exception as e:
	    print('Weather file does not exist')
	    return -1

    def read_parquet(self,inputfilepath):
        out_df=self.spark.read.parquet('file:///'+inputfilepath+'/'+'out_parquet')

        ldf2=out_df. \
            orderBy(out_df['ScreenTemperature'].desc()). \
            limit(1). \
            select(out_df['ForecastSiteCode'], substring(out_df['ObservationDate'],1,10).alias('Date'),out_df['Region'],out_df['ScreenTemperature'] ). \
            show(10,truncate=False)

    def workflow(self):
        path=sys.argv[1]
        print(path)
	print(os.path.isdir(path))
 	inputfilepath=path	

	if os.path.isdir(path):
            ret=self.load_csv_parquet(inputfilepath)
	    if ret==1:		
                self.read_parquet(inputfilepath)
	    else:
		exit		
	else:
	    print('Directory does not exist..!')	
	    exit		

if __name__=='__main__':
    activity_name='DLG'
    sc =SparkContext(appName=activity_name)
    etl=etl_class(activity_name,sc)

    etl.workflow()
# end of the program
