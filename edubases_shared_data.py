import csv
import pprint
import re

import psycopg2 as psycopg2
from dotenv import dotenv_values

from domains_mappings import domains
from levels_mappings import levels
from utils import deduplicate, take_identifier

conn_params = dict(dotenv_values(".env"))

DOM_ENSEIGN_PURPOSE = 'http://data.education.fr/voc/scolomfr/concept/scolomfr-voc-028-num-003'

EDU_LEVEL_PURPOSE = 'http://data.education.fr/voc/scolomfr/concept/educational_level'

levels_index = list(levels.keys())
levels_labels = list(map(lambda hash: hash['label'], levels.values()))

if __name__ == '__main__':
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    cur.execute('SELECT * from teaching_sheets')

    domaines_index = domains.keys()

    count = 0
    total = 0
    line = 0
    disciplines_count = {}
    domaines_count = {}
    missing_count = {}
    old_levels_count = {}
    new_levels_count = {}
    level_1_levels = []
    level_2_levels = []
    missing_legacy_levels = []
    missing_modern_levels = []
    without_domain_resources = 0
    without_level_resources = 0
    without_level_domain_resources = 0

    categories = list(domains.keys())

    with open('edubases_shared_data.csv', 'w', encoding='UTF8') as f_label_title:
        file_writer = csv.writer(f_label_title, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        label_title_header = ['id', 'title', 'desc', 'simple_level_labels', 'simple_level_ids', 'detailed_level_ids',
                              'simple_domain_label', 'simple_domain_id', 'detailed_domains_ids']
        file_writer.writerow(label_title_header)

        for row in cur:
            total += 1
            json_data = row[12]
            title = row[3].replace('\n', ' ').replace(r"\r\n",
                                                      ' ')
            desc = row[4].replace('\n', ' ').replace(r"\r\n",
                                                     ' ')
            uri = row[5]
            selected_level_ids = []
            complete_level_ids = []
            if 'levelTree' in json_data.keys():
                level_found = False
                previous_level = None
                for entry in sorted(json_data['levelTree'], key=lambda e: e['level'], reverse=True):
                    selected_level = None
                    legacy_name = entry['value']
                    level = entry['level']
                    if not level == previous_level and level_found:
                        break
                    previous_level = level
                    for d in levels.keys():
                        if legacy_name in levels[d]['edubases']:
                            level_found = True
                            selected_level = d
                            selected_level_ids.append(selected_level)
                            complete_level_ids.append(selected_level)
                    if selected_level is None and legacy_name not in missing_legacy_levels:
                        missing_legacy_levels.append(legacy_name)
                        print(f'*******Legacy : <{legacy_name}> Missing')
            elif 'classification' in json_data.keys():
                for classification in json_data['classification']:
                    if classification['purpose']['id'] == EDU_LEVEL_PURPOSE:
                        if 'taxonPath' in classification.keys():
                            for tp in classification['taxonPath']:
                                if type(tp) is dict:
                                    selected_level = None
                                    niveau_id = take_identifier(tp['id'].lower().strip())
                                    complete_level_ids.append(niveau_id)
                                    for lev in levels.keys():
                                        if niveau_id in map(lambda str: str.lower().strip(), levels[lev]['scolomfr']):
                                            selected_level = lev
                                            selected_level_ids.append(selected_level)
                                    if selected_level is None and niveau_id not in missing_modern_levels:
                                        missing_modern_levels.append(niveau_id)
                                        print(f'*******Modern : <{niveau_id}> Missing')
            selected_domain_id = ''
            selected_domain_label = ''
            complete_domaine_ids = []
            if 'legacyDiscipline' in json_data.keys():
                legacy_name = json_data['legacyDiscipline']['name']
                for d in domains.keys():
                    if legacy_name in domains[d]['edubases']:
                        selected_domain_id = d
                        complete_domaine_ids.append(d)
                        selected_domain_label = domains[d]['label']
                if not selected_domain_id:
                    print(f'******* Legacy : <{legacy_name}> missing')
            elif 'classification' in json_data.keys():
                for classification in json_data['classification']:
                    if classification['purpose']['id'] == DOM_ENSEIGN_PURPOSE:
                        tp = None
                        if 'taxonPath' in classification.keys():
                            for tp in classification['taxonPath']:
                                tp_label = tp['label']
                                result = re.search(r"\((.+)\s+cycle \d\)", tp_label)
                                if result is not None:
                                    tp_label = result.group(1)
                                for d in domains.keys():
                                    if tp_label.lower().strip() in map(lambda str: str.lower().strip(),
                                                                       domains[d]['scolomfr']):
                                        selected_domain_id = d
                                        selected_domain_label = domains[d]['label']
                                complete_domaine_ids.append(take_identifier(tp['id']))
                            if tp and not selected_domain_id:
                                if not tp['label'] in missing_count.keys():
                                    print(f'******* New : <{tp["label"]}> missing')
                                    missing_count[tp['label']] = 1
                                else:
                                    missing_count[tp['label']] += 1
                            if tp and selected_domain_id:
                                if not tp['label'] in domaines_count.keys():
                                    domaines_count[tp['label']] = 1
                                else:
                                    domaines_count[tp['label']] += 1
            if selected_domain_id:
                if not selected_domain_id in disciplines_count.keys():
                    disciplines_count[selected_domain_id] = 1
                else:
                    disciplines_count[selected_domain_id] += 1
            selected_levels_labels = ','.join(
                map(lambda l: levels_labels[int(levels_index.index(l))],
                    deduplicate(selected_level_ids)))
            if not selected_domain_id:
                without_domain_resources += 1
            if not selected_level_ids:
                without_level_resources += 1
            if not selected_level_ids and not selected_domain_id:
                without_level_domain_resources += 1
            if selected_domain_id or complete_domaine_ids or selected_level_ids or complete_level_ids:
                file_writer.writerow(
                    [line, title, desc, selected_levels_labels,
                     f"[{','.join(deduplicate(selected_level_ids))}]",
                     f"[{','.join(deduplicate(complete_level_ids))}]",
                     selected_domain_label, selected_domain_id,
                     f"[{','.join(deduplicate(complete_domaine_ids))}]"])
            line += 1

    cur.close()
    print(total)
    pprint.pprint(disciplines_count)
    pprint.pprint({k: v for k, v in missing_count.items()})
    pprint.pprint(categories)
    print(f"Sans niveau : {without_level_resources}")
    print(f"Sans domaine : {without_domain_resources}")
    print(f"Sans niveau ni domaine : {without_level_domain_resources}")
