import csv
import pprint
import re

import nltk as nltk
import oaipmh.client
import spacy
from bs4 import BeautifulSoup
from oaipmh.metadata import MetadataRegistry, MetadataReader

from levels_classification import levels_classification
from levels_replacements import levels_replacements
from levels_mappings import levels

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
        'niveauEducatifId': ('textList',
                             "lom:lom/lom:classification[lom:purpose/lom:value='http://data.education.fr/voc/scolomfr/concept/educational_level']/lom:taxonPath/lom:taxon/lom:id/text()"),
        'niveauEducatifLabel': ('textList',
                                "lom:lom/lom:classification[lom:purpose/lom:value='http://data.education.fr/voc/scolomfr/concept/educational_level']/lom:taxonPath/lom:taxon/lom:entry/lom:string/text()"),
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
    niveaux_count = {}
    niveaux_ids_global = []
    missing_count = {}
    uris = []
    with open('gar_level_labeled_data_titles.csv', 'w', encoding='UTF8') as f_label_title:
        with open('gar_level_labeled_data_desc.csv', 'w', encoding='UTF8') as f_label_desc:
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
                niveaux = record[1].getField('niveauEducatifLabel')
                niveaux_ids = record[1].getField('niveauEducatifId')
                niveaux_ids = list(map(lambda x: x.strip('\n').strip(' ').strip('\n').strip(' '), niveaux_ids))
                for niveau_id in niveaux_ids:
                    match = re.search("\s", niveau_id)
                    if match:
                        raise Exception(f'There is a space remaining in {niveau_id}')
                niveaux_ids = [levels_replacements[niveau_id] if niveau_id in levels_replacements.keys() else niveau_id
                               for niveau_id in niveaux_ids]
                for niveau_id in niveaux_ids:
                    if niveau_id and not re.search("scolomfr-voc-022", str(niveau_id)):
                        print(f'>>>>>>> Wrong vocabulary : {niveau_id}')
                niveaux_ids = [niveau_id if niveau_id and re.search("scolomfr-voc-022", niveau_id) else None
                               for niveau_id in niveaux_ids]
                niveaux_ids = list(filter(None, niveaux_ids))
                for niveau_id in niveaux_ids:
                    if not take_identifier(niveau_id) in all_values:
                        raise Exception(f'Value not handled : {niveau_id}')
                niveaux_ids_global += niveaux_ids
                # search in leaf levels
                for index, niveau_id in enumerate(niveaux_ids):
                    niveau_id = take_identifier(niveau_id.lower().strip())
                    key = f"{niveaux_ids[index]} - {niveau_id.strip(' ')}"
                    for lev in levels.keys():
                        if niveau_id == lev:
                            selected_levels.append(lev)
                # search in levels by decreasing depth in levels tree
                depth_array = ['level1', 'level2', 'level3', 'level4']
                current_depth = 3
                # whe dont take level1 (too generic) so strict inequality
                while current_depth > 0 and not selected_levels:
                    for index, niveau_id in enumerate(niveaux_ids):
                        niveau_id = niveau_id.lower().strip()
                        if niveau_id not in levels_classification[depth_array[current_depth]]:
                            continue
                        niveau_id = take_identifier(niveau_id)
                        for lev in levels.keys():
                            if niveau_id in map(lambda str: str.lower().strip(), levels[lev]['scolomfr']):
                                selected_levels.append(lev)
                    current_depth = current_depth - 1
                selected_levels = list(dict.fromkeys(selected_levels))
                if not selected_levels:
                    for index, niveau_id in enumerate(niveaux_ids):
                        print(f'****Manquant : {niveau_id}')
                        if niveau_id not in missing_count.keys():
                            missing_count[niveau_id] = 1
                        else:
                            missing_count[niveau_id] += 1
                if selected_levels:
                    line += 1
                    processed_title = next(iter(title or []), '').replace("\n", ".").replace("\xa0", " ")
                    processed_description = BeautifulSoup(next(iter(desc or []), ''), "lxml").text.replace("\n",
                                                                                                           ".").replace(
                        "\xa0", " ").replace(">", ".").replace(":", ".").replace(";", ".")
                    processed_description = re.sub(r'\.+\s*\.+', '.', processed_description)
                    spacy_doc = nlp(processed_description)
                    selected_levels_indexes = [True if level in selected_levels else False for level in levels_index]
                    selected_levels_labels = ','.join(
                        map(lambda selected_level: levels_labels[int(levels_index.index(selected_level))],
                            selected_levels))
                    label_title_writer.writerow([line, processed_title] + selected_levels_indexes)
                    for sentence in spacy_doc.sents:
                        if len(sentence) < 10:
                            continue
                        desc_line += 1
                        label_desc_writer.writerow([desc_line, sentence] + selected_levels_indexes)
                print(counter, title, selected_levels_labels)
                print(', '.join(niveaux_ids))
                print('---------------------------------')
    # pprint.pprint({k: v for k, v in missing_count.items()})
    # niveaux_ids_global = list(dict.fromkeys(niveaux_ids_global))
    # niveaux_ids_global.sort()
    pprint.pprint(niveaux_count)
