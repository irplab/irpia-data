import csv
import pprint
import re
import extruct

import nltk as nltk
import spacy
import oaipmh.client
from oaipmh.metadata import MetadataRegistry, MetadataReader
from mappings import domains
from bs4 import BeautifulSoup
import requests
from w3lib.html import get_base_url

nltk.download('punkt')
nlp = spacy.load('fr_core_news_md')

categories = list(domains.keys())

lom_reader = MetadataReader(
    fields={
        'uri': ('textList', 'lom:lom/lom:general/lom:identifier/lom:entry/text()'),
        'title': ('textList', 'lom:lom/lom:general/lom:title/lom:string/text()'),
        'description': ('textList', 'lom:lom/lom:general/lom:description/lom:string/text()'),
        'educationalDescription': ('textList', 'lom:lom/lom:description/lom:description/lom:string/text()'),
        'technicalLocation': ('textList', 'lom:lom/lom:technical/lom:location/text()'),
        'domaineEnseignementId': ('textList',
                                  "lom:lom/lom:classification[lom:purpose/lom:value='http://data.education.fr/voc/scolomfr/concept/scolomfr-voc-028-num-003']/lom:taxonPath/lom:taxon/lom:id/text()"),
        'domaineEnseignementLabel': ('textList',
                                     "lom:lom/lom:classification[lom:purpose/lom:value='http://data.education.fr/voc/scolomfr/concept/scolomfr-voc-028-num-003']/lom:taxonPath/lom:taxon/lom:entry/lom:string/text()"),
    },
    namespaces={
        'lom': 'http://ltsc.ieee.org/xsd/LOM',
        'lomfr': 'http://www.lom-fr.fr/xsd/LOMFR',
        'scolomfr': 'http://www.lom-fr.fr/xsd/SCOLOMFR'}
)

if __name__ == '__main__':
    URL = 'http://scolomfr.univ-valenciennes.fr/ori-oai-repository-open/OAIHandler'

    registry = MetadataRegistry()
    registry.registerReader('lom', lom_reader)
    client = oaipmh.client.Client(URL, registry)
    desc_line = counter = line = 0
    domaines_count = {}
    missing_count = {}
    uris = []
    with open('gar_domain_labeled_data_titles.csv', 'w', encoding='UTF8') as f_label_title:
        with open('gar_domain_labeled_data_desc.csv', 'w', encoding='UTF8') as f_label_desc:
            label_title_writer = csv.writer(f_label_title, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            label_desc_writer = csv.writer(f_label_desc, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            label_header = ['id', 'text', 'label']
            label_title_writer.writerow(label_header)
            label_desc_writer.writerow(label_header)
            for record in client.listRecords(metadataPrefix='lom'):
                dom = None
                counter += 1
                title = record[1].getField('title')
                desc = record[1].getField('description')
                domaines = record[1].getField('domaineEnseignementLabel')
                locations = record[1].getField('technicalLocation')
                if len(locations) > 0:
                    location = locations[0]
                    try:
                        responses = requests.get(location, allow_redirects=True, verify=False)

                        if(len(responses.history)>0):
                            if responses.history[0].status_code == '301':
                                location = responses.history[0].headers['Location']
                                responses = requests.get(location, allow_redirects=True, verify=False)
                        base_url = get_base_url(responses.text, responses.url)
                        data = extruct.extract(responses.text, base_url=base_url)
                        uris.append(location)
                    except requests.exceptions.RequestException:
                        print(f'SSL error with {location}')
                for index, domaine in enumerate(domaines):
                    if domaine not in domaines_count.keys():
                        domaines_count[domaine] = 0
                    else:
                        domaines_count[domaine] += 1
                    for d in domains.keys():
                        if domaine.lower().strip() in map(lambda str: str.lower().strip(), domains[d]['scolomfr']):
                            dom = d
                if not dom:
                    for index, domaine in enumerate(domaines):
                        print(f'****Manquant : {domaine}')
                        if domaine not in missing_count.keys():
                            missing_count[domaine] = 1
                        else:
                            missing_count[domaine] += 1
                if dom:
                    line += 1
                    processed_title = next(iter(title or []), '').replace("\n", ".").replace("\xa0", " ")
                    processed_description = BeautifulSoup(next(iter(desc or []), ''), "lxml").text.replace("\n",
                                                                                                           ".").replace(
                        "\xa0", " ").replace(">", ".").replace(":", ".").replace(";", ".")
                    processed_description = re.sub(r'\.+\s*\.+', '.', processed_description)
                    spacy_doc = nlp(processed_description)
                    for sent in spacy_doc.sents:
                        print('------')
                        print(sent)
                    # sentences = nltk.sent_tokenize(processed_description, language='french')
                    label_title_writer.writerow([line, processed_title] + [categories.index(dom)])
                    for sentence in spacy_doc.sents:
                        if len(sentence) < 10:
                            continue
                        desc_line += 1
                        label_desc_writer.writerow([desc_line, sentence] + [categories.index(dom)])
                print(counter, title, domaines)

    pprint.pprint({k: v for k, v in missing_count.items()})
    uris.sort()
    pprint.pprint(uris)