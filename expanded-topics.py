import json # Para poder trabajar con objetos JSON

from elasticsearch import Elasticsearch
from elasticsearch import helpers

# Devuelve una lista tweets sobre una temática con términos expandidos
def busquedaExpandida(es,tematica,metrica,size):
    terminos = getTerminos(es,tematica,metrica,size)
    expandirConsulta = "lang:en AND (" + tematica
    for termino in terminos:
        expandirConsulta+= " OR " + termino
    expandirConsulta+= ")"
    results = helpers.scan(es,
        index="tweets-20090624-20090626-en_es-10percent",
         body={
                "query":
                    {"query_string":
                        {
                        "default_field": "text",
                        "query": expandirConsulta
                        }
                    }
            }
        )

    with open(tematica+"-"+metrica+"-"+str(size)+".ndjson", 'w') as file:
        for result in results:
            text = json.dumps({"ID de usuario": result["_source"]["user_id_str"],"Fecha de creacion": result["_source"]["created_at"],"Texto": result["_source"]["text"]}, sort_keys=True)
            file.write(text+"\n")
        file.close()


# Devuelve un diccionario con palabras vacías a evitar
def palabrasVacias():
    lista = []
    f = open("stopwordEN.txt","r")
    words = f.readlines()
    for word in range(len(words)):
        lista.append(words[word].strip())
    f.close()
    return lista

# Devuelve una lista con palabras relacionadas con la temática pasada por parámetro
def getTerminos(es,tematica,metrica,size):
    results = es.search(
        index="tweets-20090624-20090626-en_es-10percent",
            body={
                "query":
                    {"query_string":
                        {
                        "query": "(lang:en AND "+tematica+")"
                        }
                    },
                    "aggs": {
                        "tematica": {
                            "significant_terms": {
                                "field": "text",
                                "size": size,
                                "exclude": palabrasVacias(),
        				        metrica: {},
                                }
                    }
                }
            }
        )
    return palabrasSignificativas(results,size,metrica)

# Dada un conjunto de resultados, devuelve una lista con las palabras significativas
def palabrasSignificativas(results,size,metrica):
    terms = set()
    print("Palabras significativas con un tamaño de " + str(size) + " y " + metrica)
    for result in results["aggregations"]["tematica"]["buckets"]:
        print(result["key"])
        terms.add(result["key"])
    print("-----------------")
    return terms

# Devuelve una lista de tweets con una métrica y tamaño determinado
def comparacionMetricas(es,tematica,metrica,size):
    terminos = getTerminos(es,tematica,metrica,size)
    expandirConsulta = "lang:en AND (" + tematica
    for termino in terminos:
        expandirConsulta+= " OR " + termino
    expandirConsulta+= ")"
    results = es.search(
        index="tweets-20090624-20090626-en_es-10percent",
            body={
                "size": 20,
                "query":
                    {"query_string":
                        {
                        "query": expandirConsulta
                        }
                    }
            }
        )
    f=open(tematica+"-"+metrica+"-"+str(size)+".txt","wb")
    for result in results["hits"]["hits"]:
        text = result["_source"]["text"]+"\n"
        f.write(text.encode("UTF-8"))
    f.close()

# Tweets relativos a un temática con consulta expandida
def main():
    # Nos conectamos por defecto a localhost:9200
    es = Elasticsearch(timeout = 5000)
    tematica = "Farrah Fawcett"     # Cambiar para escoger otra temática

    # Búsqueda de tweets para 5 términos significativos
    busquedaExpandida(es,tematica, "chi_square", 5) # Volcado de los tweets
    comparacionMetricas(es,tematica, "gnd", 5)
    comparacionMetricas(es,tematica, "jlh", 5)
    comparacionMetricas(es,tematica, "mutual_information", 5)
    comparacionMetricas(es,tematica, "chi_square", 5)

    # Búsqueda de tweets para 10 términos significativos
    comparacionMetricas(es,tematica, "gnd", 10)
    comparacionMetricas(es,tematica, "jlh", 10)
    comparacionMetricas(es,tematica, "mutual_information", 10)
    comparacionMetricas(es,tematica, "chi_square", 10)

    # Búsqueda de tweets para 15 términos significativos
    comparacionMetricas(es,tematica, "gnd", 15)
    comparacionMetricas(es,tematica, "jlh", 15)
    comparacionMetricas(es,tematica, "mutual_information", 15)
    comparacionMetricas(es,tematica, "chi_square", 15)

if __name__ == '__main__':
    main()
