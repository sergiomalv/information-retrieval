import requests
import json

from elasticsearch import Elasticsearch
from elasticsearch import helpers

 # Saca los términos significativos de un día en un hora determinada
def trendingTopicsByHour(es,day,started,finish,lan,size,metrica):
    results = es.search(
        index="tweets-20090624-20090626-en_es-10percent-v4",
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
def procesarResultados(f,results,dia,started,finish,diccionario):
    for result in results["aggregations"]["TrendingTopics"]["buckets"]:
        topic = result["key"]
        if (topic not in diccionario):
            diccionario.add(topic)
            api = "https://www.wikidata.org/w/api.php?action=wbsearchentities&language=en&format=json&search="+topic
            request = requests.get(api)
            information = request.json()
            if (information["search"] != []):
                entity = information["search"][0]
                cabecera = "Primera entidad para "+topic+": " + entity["id"] +"\n"
                result = "\t"+entity["id"]+"\n"
                try:
                    cabecera += "\tDescripcion: " + entity["description"] + "\n"
                except:
                    cabecera += "\tNo tiene una descripcion\n"
                cabecera+= getInstanceOf(entity["id"])
                cabecera += getSinonimos(entity["id"])
                f.write(cabecera.encode("UTF-8"))
                print(cabecera)

# Dada una entidad encuentra los sinónimos asociados en Wikidata
def getSinonimos(entidad):
    api = "https://www.wikidata.org/w/api.php?action=wbgetentities&ids="+entidad+"&languages=en&format=json"
    request = requests.get(api)
    information = request.json()
    try:
        result = "\tSinonimos para la entidad " + entidad + "\n"
        for alias in information["entities"][entidad]["aliases"]["en"]:
            result += "\t\t"+ alias["value"]+"\n"
    except:
        result = "\tNo tiene sinónimos\n"
    return result

def getInstanceOf(entidad):
    try:
        api = "https://www.wikidata.org/w/api.php?action=wbgetentities&ids="+entidad+"&languages=en&format=json"
        request = requests.get(api)
        information = request.json()
        instance = information["entities"][entidad]["claims"]["P31"][0]["mainsnak"]["datavalue"]["value"]["id"]
        api2 = "https://www.wikidata.org/w/api.php?action=wbsearchentities&language=en&format=json&search="+instance
        request2 = requests.get(api2)
        information2 = request2.json()
        final = information2["search"][0]["label"]
        return "\tEntidad de tipo: " + final + "\n"
    except:
        return "\tNo tiene una instancia\n"

# Generar un índice de los tweets escritos en inglés con shinlges de 1,2,3 términos
def main():
    # Nos conectamos por defecto a localhost:9200
    es = Elasticsearch(timeout=5000)
    # Archivo donde guardaremos los resultados
    archivo = "entidades.txt"
    f=open(archivo,"wb")
    # Parametrización de datos
    lan = "en"      # CAMBIAR esta variable para escoger el idioma que se desea
    size = 30       # CAMBIAR esta variable para escoger el número de resultados
    metrica = "gnd" # CAMBIAR esta variables para escoger la métrica
    # Diccionario donde guardamos las palabras que ya hemos mirado
    diccionario = set()
    for dia in range(24,27):
        for hora in range(23):
            trendingTopics = trendingTopicsByHour(es,dia,hora,hora+1,lan,size,metrica)
            procesarResultados(f,trendingTopics,dia,hora,hora+1,diccionario)
    f.close()

if __name__ == '__main__':
    main()