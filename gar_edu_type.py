import csv
import pprint
import re
import extruct

import nltk as nltk
import spacy
import oaipmh.client
from oaipmh.metadata import MetadataRegistry, MetadataReader
from mappings_levels import levels
from levels_replacements import levels_replacements
from levels_classification import levels_classification
from bs4 import BeautifulSoup
import requests
from w3lib.html import get_base_url

nltk.download('punkt')
nlp = spacy.load('fr_core_news_md')

levels_index = list(levels.keys())
levels_labels = list(map(lambda hash: hash['label'], levels.values()))

all_values = list(levels.keys())
for values in map(lambda dict: dict['scolomfr'], levels.values()):
    all_values = all_values + values
for index, value in enumerate(levels_labels):
    print(f'{index} : {value}')
all_values = list(dict.fromkeys(all_values))

lom_reader = MetadataReader(
    fields={
        'uri': ('textList', 'lom:lom/lom:general/lom:identifier/lom:entry/text()'),
        'title': ('textList', 'lom:lom/lom:general/lom:title/lom:string/text()'),
        'description': ('textList', 'lom:lom/lom:general/lom:description/lom:string/text()'),
        'educationalDescription': ('textList', 'lom:lom/lom:description/lom:description/lom:string/text()'),
        'technicalLocation': ('textList', 'lom:lom/lom:technical/lom:location/text()'),
        'learningResourceTypeValue': ('textList',
                                      "lom:lom/lom:educational/lom:learningResourceType/lom:value/text()"),
        'learningResourceTypeLabel': ('textList',
                                      "lom:lom/lom:educational/lom:learningResourceType/lom:label/text()"),
    },
    namespaces={
        'lom': 'http://ltsc.ieee.org/xsd/LOM',
        'lomfr': 'http://www.lom-fr.fr/xsd/LOMFR',
        'scolomfr': 'http://www.lom-fr.fr/xsd/SCOLOMFR'}
)


def take_identifier(url):
    return url.rsplit('/', 1)[-1]


if __name__ == '__main__':
    URL = 'http://scolomfr.univ-valenciennes.fr/ori-oai-repository-open/OAIHandler'

    registry = MetadataRegistry()
    registry.registerReader('lom', lom_reader)
    client = oaipmh.client.Client(URL, registry)
    desc_line = counter = line = 0
    types_count = {}
    niveaux_ids_global = []
    missing_count = {}
    uris = []
    with open('gar_edutype_labeled_data_titles.csv', 'w', encoding='UTF8') as f_label_title:
        with open('gar_edutype_labeled_data_desc.csv', 'w', encoding='UTF8') as f_label_desc:
            label_title_writer = csv.writer(f_label_title, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            label_desc_writer = csv.writer(f_label_desc, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            label_header = ['id', 'text'] + levels_index
            label_title_writer.writerow(label_header)
            label_desc_writer.writerow(label_header)
            for record in client.listRecords(metadataPrefix='lom'):
                selected_levels = []
                counter += 1
                title = record[1].getField('title')
                desc = record[1].getField('description')
                type_labels = record[1].getField('learningResourceTypeLabel')
                type_ids = record[1].getField('learningResourceTypeValue')
                type_ids = list(map(lambda x: x.strip('\n').strip(' ').strip('\n').strip(' '), type_ids))
                type_labels = list(map(lambda x: x.strip('\n').strip(' ').strip('\n').strip(' '), type_labels))
                print(', '.join(type_labels))
                print(', '.join(type_ids))
                for type_label in type_labels:
                    if type_label not in types_count.keys():
                        types_count[type_label] = 1
                    else:
                        types_count[type_label] += 1
                pprint.pprint(types_count)
                print('---------------------------------')
    # pprint.pprint({k: v for k, v in missing_count.items()})
    # niveaux_ids_global = list(dict.fromkeys(niveaux_ids_global))
    # niveaux_ids_global.sort()
