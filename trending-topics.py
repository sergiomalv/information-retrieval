import json 

from elasticsearch import Elasticsearch
from elasticsearch import helpers

 # Devuelve los términos significativos de un intervalo de tiempo
def trendingTopicsByHour(es,day,started,finish,lan,size,metrica):
    results = es.search(
        index="tweets-20090624-20090626-en_es-10percent",
            body={
                "size": 0,
                "query": {
                    "bool" : {
                        "must": [
                                {
                                "range" :
                                    { "created_at":
                                        { "format": "date_hour_minute_second",
                                        "gte": "2009-06-"+ str(day) + "T" + str(started) + ":00:00",
                                        "lte": "2009-06-"+ str(day) + "T" + str(finish) + ":00:00"}
                                    }
                                },
                                { "query_string" :
                                    { "query":"lang:"+lan}
                                }
                                ]
                            }
                        },
                        "aggs": {
                        "TrendingTopics": {
                            "significant_terms": {
                                "field": "text",
                                "size": size,
                                metrica : {}
                            }
                        }
                    }
                }
            )
    return results

# Añade los nuevos trending topics a un archivo .txt
def procesarResultados(f,results,dia,started,finish):
    cabecera = "Tendring Topics del día "+str(dia)+" de "+str(started)+" a "+str(finish)+"\n"
    f.write(cabecera.encode("UTF-8"))
    for result in results["aggregations"]["TrendingTopics"]["buckets"]:
        topic = "\t"+result["key"]+"\n"
        f.write(topic.encode("UTF-8"))

# Crea un .txt con los trending topics de los 24,25,26 de cada hora
def main():
    # Nos conectamos por defecto a localhost:9200
    es = Elasticsearch()
    # Archivo donde guardaremos los resultados
    archivo = "tendringTopics.txt"
    f=open(archivo,"wb")
    # Parametrización de datos
    lan = "en"      # CAMBIAR esta variable para escoger el idioma que se desea
    size = 10       # CAMBIAR esta variable para escoger el número de resultados
    metrica = "gnd" # CAMBIAR esta variables para escoger la métrica
    for dia in range(24,27):
        for hora in range(23):
            trendingTopics = trendingTopicsByHour(es,dia,hora,hora+1,lan,size,metrica)
            procesarResultados(f,trendingTopics,dia,hora,hora+1)
    f.close()

if __name__ == '__main__':
    main()
