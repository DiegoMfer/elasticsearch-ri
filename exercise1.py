# Import dependencies
from elasticsearch import Elasticsearch
import json

client = Elasticsearch("http://localhost:9200")


# Exercise 1
def trending_topic_list(index, trending_topic_amount, trending_topic_language, metric):

    # Time constraints
    start = 1245801600
    end = 1246073200

    # Data segments by hour will be stored here
    responses = []

    # We increment hour by hour
    for hour in range(start, end, 3600):
        responses.append(client.search(
            index = index,
            body = get_query(trending_topic_amount, trending_topic_language, metric, hour)
        ))

    return responses

def get_query(trending_topic_amount, trending_topic_language, metric, hour):

    next_hour = hour + 3600

    query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "lang": trending_topic_language

                            }
                        },
                        {
                            "range": {
                                "created_at": {
                                    "gte": str(hour),
                                    "lt": str(next_hour),
                                    "format": "epoch_second"
                                }
                            }
                        }
                        
                    ]
                }
            },
            "aggs": {
                "Trending topics": {
                    "significant_terms": {
                        "field": "text",
                        "size": trending_topic_amount,
                        metric: {}
                    }
                }
            }
        }
    return query


def save_results(results, name):
    file = open(name, "w")
    json.dump(results, file, indent = 4)
    file.close()

def parse_responses(responses):
    contents = {}
    for index, response in enumerate(responses):
        hour = []
        for bucket in response["aggregations"]["Trending topics"]["buckets"]:
            hour.append({
                "term": bucket["key"],
                "count": bucket["doc_count"]
            })
        contents[str(index)] = hour

    return contents

def main():

    # Customize here the search script
    index = "tweets-20090624-20090626-en_es-10percent-v2"
    trending_topic_amount = 5
    trending_topic_language = "en"
    metric = "gnd"

    responses = trending_topic_list(index, trending_topic_amount, trending_topic_language, metric)

    results = parse_responses(responses)

    save_results(results, "output1.txt")

if __name__ == '__main__':
    main()
