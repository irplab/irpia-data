import csv
import pprint
import re

import oaipmh.client
from bs4 import BeautifulSoup
from oaipmh.metadata import MetadataRegistry, MetadataReader

from domains_mappings import domains
from levels_classification import levels_classification
from levels_mappings import levels
from levels_replacements import levels_replacements
from utils import take_identifier, deduplicate

categories = list(domains.keys())

levels_index = list(levels.keys())
levels_labels = list(map(lambda hash: hash['label'], levels.values()))
domains_labels = list(map(lambda hash: hash['label'], domains.values()))

all_values = list(levels.keys())
for values in map(lambda dict: dict['scolomfr'], levels.values()):
    all_values = all_values + values
for index, value in enumerate(levels_labels):
    print(f'{index} : {value}')
for index, value in enumerate(domains_labels):
    print(f'{index} : {value}')
all_values = list(dict.fromkeys(all_values))

lom_reader = MetadataReader(
    fields={
        'uri': ('textList', 'lom:lom/lom:general/lom:identifier/lom:entry/text()'),
        'title': ('textList', 'lom:lom/lom:general/lom:title/lom:string/text()'),
        'description': ('textList', 'lom:lom/lom:general/lom:description/lom:string/text()'),
        'educationalDescription': ('textList', 'lom:lom/lom:description/lom:description/lom:string/text()'),
        'technicalLocation': ('textList', 'lom:lom/lom:technical/lom:location/text()'),
        'niveauEducatifId': ('textList',
                             "lom:lom/lom:classification[lom:purpose/lom:value='http://data.education.fr/voc/scolomfr/concept/educational_level']/lom:taxonPath/lom:taxon/lom:id/text()"),
        'niveauEducatifLabel': ('textList',
                                "lom:lom/lom:classification[lom:purpose/lom:value='http://data.education.fr/voc/scolomfr/concept/educational_level']/lom:taxonPath/lom:taxon/lom:entry/lom:string/text()"),
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
    niveaux_count = {}
    domaines_count = {}
    niveaux_ids_global = []
    missing_count = {}
    uris = []
    with open('gar_shared_data.csv', 'w', encoding='UTF8') as output_file:
        file_writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        label_header = ['id', 'title', 'desc', 'simple_level_labels', 'simple_level_ids', 'detailed_level_ids',
                        'simple_domain_label', 'simple_domain_id', 'detailed_domains_ids']
        file_writer.writerow(label_header)
        for record in client.listRecords(metadataPrefix='lom'):
            selected_level_ids = []
            selected_domain_id = None
            selected_domain_label = None
            counter += 1
            title = record[1].getField('title')
            desc = record[1].getField('description')
            source_domains = record[1].getField('domaineEnseignementLabel')
            source_domain_ids = record[1].getField('domaineEnseignementId')
            source_locations = record[1].getField('technicalLocation')
            source_levels = record[1].getField('niveauEducatifLabel')
            source_level_ids = record[1].getField('niveauEducatifId')
            for index, domaine in enumerate(source_domains):
                if domaine not in domaines_count.keys():
                    domaines_count[domaine] = 0
                else:
                    domaines_count[domaine] += 1
                for d in domains.keys():
                    if domaine.lower().strip() in map(lambda str: str.lower().strip(), domains[d]['scolomfr']):
                        selected_domain_id = d
                        selected_domain_label = domains[d]['label']
                        break
            if not selected_domain_id:
                for index, domaine in enumerate(source_domains):
                    if domaine not in missing_count.keys():
                        print(f'****Missing domain : {domaine}')
                        missing_count[domaine] = 1
                    else:
                        missing_count[domaine] += 1
                selected_domain_id = ''
                selected_domain_label = ''
            # clear all spaces from
            source_level_ids = list(map(lambda x: x.strip('\n').strip(' ').strip('\n').strip(' '), source_level_ids))
            for niveau_id in source_level_ids:
                match = re.search("\s", niveau_id)
                if match:
                    raise Exception(f'There is a space remaining in {niveau_id}')
            # replace level values that have to be replaced
            source_level_ids = [levels_replacements[niveau_id] if niveau_id in levels_replacements.keys() else niveau_id
                                for niveau_id in source_level_ids]
            # detect and ignore values from other vocabularies
            for niveau_id in source_level_ids:
                if niveau_id and not re.search("scolomfr-voc-022", str(niveau_id)):
                    print(f'>>>>>>> Wrong vocabulary : {niveau_id}')
            source_level_ids = [niveau_id if niveau_id and re.search("scolomfr-voc-022", niveau_id) else None
                                for niveau_id in source_level_ids]
            source_level_ids = list(filter(None, source_level_ids))
            for niveau_id in source_level_ids:
                if not take_identifier(niveau_id) in all_values:
                    print(f'Value not handled : {niveau_id}')
            niveaux_ids_global += source_level_ids
            # search in leaf levels
            for index, niveau_id in enumerate(source_level_ids):
                niveau_id = take_identifier(niveau_id.lower().strip())
                for lev in levels.keys():
                    if niveau_id == lev:
                        selected_level_ids.append(lev)
            # search in levels by decreasing depth in levels tree
            depth_array = ['level1', 'level2', 'level3', 'level4']
            current_depth = 3
            # whe dont take level1 (too generic) so strict inequality
            while current_depth >= 1 and not selected_level_ids:
                for index, niveau_id in enumerate(source_level_ids):
                    niveau_id = niveau_id.lower().strip()
                    if niveau_id not in levels_classification[depth_array[current_depth]]:
                        continue
                    niveau_id = take_identifier(niveau_id)
                    for lev in levels.keys():
                        if niveau_id in map(lambda str: str.lower().strip(), levels[lev]['scolomfr']):
                            selected_level_ids.append(lev)
                current_depth = current_depth - 1
            selected_level_ids = list(dict.fromkeys(selected_level_ids))
            if not selected_level_ids:
                for index, niveau_id in enumerate(source_level_ids):
                    if niveau_id not in missing_count.keys():
                        print(f'****Missing level : {niveau_id}')
                        missing_count[niveau_id] = 1
                    else:
                        missing_count[niveau_id] += 1
                selected_level_ids = []

            if selected_level_ids or selected_domain_id:
                line += 1
                processed_title = next(iter(title or []), '').replace("\n", ".").replace("\xa0", " ")
                processed_description = BeautifulSoup(next(iter(desc or []), ''), "lxml").text.replace("\n",
                                                                                                       ".").replace(
                    "\xa0", " ").replace(">", ".").replace(":", ".").replace(";", ".")
                processed_description = re.sub(r'\.+\s*\.+', '.', processed_description)
                selected_levels_labels = ','.join(
                    map(lambda selected_level: levels_labels[int(levels_index.index(selected_level))],
                        deduplicate(selected_level_ids)))
                file_writer.writerow(
                    [line, processed_title, processed_description, selected_levels_labels,
                     f"[{','.join(deduplicate(selected_level_ids))}]",
                     f"[{','.join(deduplicate(source_level_ids))}]",
                     selected_domain_label, selected_domain_id,
                     f"[{','.join(list(map(take_identifier, deduplicate(source_domain_ids))))}]"])
    # pprint.pprint({k: v for k, v in missing_count.items()})
    # niveaux_ids_global = list(dict.fromkeys(niveaux_ids_global))
    # niveaux_ids_global.sort()
    pprint.pprint(niveaux_count)
