from exercise1 import trending_topic_list, parse_responses, save_results
import pywikibot
from pywikibot.data import api




def get_entities(trending_topics, language):
    entities = {}
    used = {}
    for index, key in enumerate(trending_topics):
        hour = {}

        for topic in trending_topics[key]:
            term = topic["term"]
            fterm = term.replace("_", "").replace(" ", "")

            if fterm in used:
                hour[term] = used[fterm]
            else:
                entity = get_entity(term, language)

                entity_type = get_type(entity)
                synonyms = get_synonyms(entity, language)

                data = {
                    "entity": entity,
                    "type": entity_type,
                    "synonyms": synonyms
                }

                hour[term] = data
                used[fterm] = data

        entities[index] = hour


    return entities


def get_type(entity):

    if entity == "No entity found":
        return "No type found"

    site = pywikibot.Site("wikidata", "wikidata")
    repo = site.data_repository()
    item = pywikibot.ItemPage(repo, entity)

    item_dict = item.get()
    claim_dict = item_dict["claims"]

    if "P31" in claim_dict.keys():
        return "Q" + str(claim_dict["P31"][0].toJSON()["mainsnak"]["datavalue"]["value"]["numeric-id"])
    else:
        return "No type found"

def get_synonyms(entity, language):

    if entity == "No entity found":
        return "No synonyms found"

    site = pywikibot.Site("wikidata", "wikidata")
    repo = site.data_repository()
    item = pywikibot.ItemPage(repo, entity)

    item_dict = item.get()
    if language in item_dict["aliases"].keys():
        return item_dict["aliases"][language]
    else:
        return "No synonyms found"

def get_entity(item_title, trending_topic_language):

    site = pywikibot.Site("wikidata", "wikidata")

    params = {
     "action" :"wbsearchentities",
     "format" : "json",
     "language" : trending_topic_language,
     "type" : "item",
     "search": item_title
     }

    request = api.Request(site=site,**params).submit()

    if len(request["search"]) == 0:
        return "No entity found"
    else:
        return request["search"][0]["id"]

def main():

    # Customize here the search script
    index = "tweets-20090624-20090626-en_es-10percent-v4"
    trending_topic_amount = 50
    trending_topic_language = "en"
    metric = "gnd"

    responses = trending_topic_list(index, trending_topic_amount, trending_topic_language, metric)

    results = parse_responses(responses)

    entities = get_entities(results, trending_topic_language)

    save_results(entities, "output2.txt")

if __name__ == '__main__':
    main()