import csv
import pprint

import oaipmh.client
from oaipmh.metadata import MetadataRegistry, MetadataReader
from mappings import domains

categories = list(domains.keys())

lom_reader = MetadataReader(
    fields={
        'uri': ('textList', 'lom:lom/lom:general/lom:identifier/lom:entry/text()'),
        'title': ('textList', 'lom:lom/lom:general/lom:title/lom:string/text()'),
        'description': ('textList', 'lom:lom/lom:general/lom:description/lom:string/text()'),
        'educationalDescription': ('textList', 'lom:lom/lom:description/lom:description/lom:string/text()'),
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
    counter = line = 0
    domaines_count = {}
    missing_count = {}
    with open('gar_labeled_data.csv', 'w', encoding='UTF8') as f_label:
        label_writer = csv.writer(f_label, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        label_header = ['id', 'text', 'label']
        label_writer.writerow(label_header)
        for record in client.listRecords(metadataPrefix='lom'):
            dom = None
            counter += 1
            title = record[1].getField('title')
            desc = record[1].getField('description')
            domaines = record[1].getField('domaineEnseignementLabel')
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
                label_writer.writerow([line, next(iter(title or []), '')] + [categories.index(dom)])

            print(counter, title, domaines)

    pprint.pprint({k: v for k, v in missing_count.items()})
