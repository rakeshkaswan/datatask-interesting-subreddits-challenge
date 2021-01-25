from pipeline.schema import Registory 
from pipeline.ingestion import Inject 
from pipeline.transformation import Transform 
from pipeline.querying import Query 


#Reading config file for ETL pipelind
print ("++++++++++++++++++++++")
print ("--- Starting Pipeline")
print ("--- Reading config")


config = {
    "DB" : {
        "user" : "user",
        "password" : "password",
        "host" : "postgres",
        "database" : "database"
    },
    "transformation_year" : [2013]
}

#Preparing DB params
print ("--- Preparing DB details config")

print ("--- Creating Tables")

Registory.prepare_db(config)

inject = Inject(config)
inject.load("data/allposts.csv")

Transform(config, inject._date)

for year in config['transformation_year']:
    print ("+++++++++++++++++++++++++++++++")
    print ("Querying - posts_{}".format(year))
    Query.execute(config['DB'], year)

print ("--- Finishing Pipeline")
print ("++++++++++++++++++++++")


