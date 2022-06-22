from elasticsearch import Elasticsearch, helpers
from exercise1 import save_results


def retrieve_data(client, index, big_query):
    data = helpers.scan(client, index=index, query=big_query)

    result_data = {}

    for piece in data:
        source = piece["_source"]
        result_data[source["user_id_str"]] = {
            "date": source["created_at"],
            "text": source["text"]
        }

    return result_data



def big_query(topics, first, language):
    return {
        "query": {
            "query_string": {
                "query": construct_query(topics) + first + " AND lang:" + language
            }
        }
    }

def construct_query(topics):
    query = "text:"
    for topic in topics:
        query += topic + " OR "

    return query

def initial_query(client, index, topic, language, metric, size):

    res = client.search(
        index = index,
        body = get_query(topic, language, metric, size)
    )
    return [bucket["key"] for bucket in res["aggregations"]["Trending topics"]["buckets"]]


def get_query(topic, language, metric, size):
    query = {
            "size": 0,
            "query": {
                "query_string": {
                    "query": "text:\"{}\" AND lang:{}".format(topic, language)
                }
            },
            "aggs": {
                "Trending topics": {
                    "significant_terms": {
                        "field": "text",
                        "size": size,
                        metric: {}
                    }
                }
            }
        }
    return query

def retrieve_docs(doc_size, client, index, big_query):
    big_query["size"] = doc_size
    data = client.search(index=index, body=big_query)

    res = {}
    for hit in data["hits"]["hits"]:
        source = hit["_source"]
        res[source["user_id_str"]] = {
            "created_at": source["created_at"],
            "text": source["text"]
        }

    return res


def main():
    client = Elasticsearch("http://localhost:9200")

    # Customize here the search script
    index = "tweets-20090624-20090626-en_es-10percent-v2"
    language = "en"
    metric = "gnd" # Metric to experiment for exercise 3
    size = 5

    # Exercise 3 parameters
    topic = "iran" # Change topic as desired
    file_name = "output3-" + metric + ".json"
    doc_size = 20 # Increment number of documents
    file_20docs_name = "output3-20docs-" + metric +".json"

    topics = initial_query(client, index, topic, language, metric, size)

    query = big_query(topics, topic, language)

    results = retrieve_data(client, index, query)

    save_results(results, file_name)

    docs = retrieve_docs(doc_size, client, index, query)

    save_results(docs, file_20docs_name )

if __name__ == '__main__':
    main()