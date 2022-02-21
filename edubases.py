# This is a sample Python script.

# Press Maj+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# Press the green button in the gutter to run the script.
import csv
import pprint

import psycopg2 as psycopg2
from mappings import domains
from db import conn_params

DOM_ENSEIGN_PURPOSE = 'http://data.education.fr/voc/scolomfr/concept/scolomfr-voc-028-num-003'

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

    categories = list(domains.keys())

    # display the PostgreSQL database server version
    with open('edubases_domain_labeled_data.csv', 'w', encoding='UTF8') as f_label:
            label_writer = csv.writer(f_label, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            label_header = ['id', 'title', 'desc', 'label']
            label_writer.writerow(label_header)

            for row in cur:
                total += 1
                json_data = row[12]
                title = row[3].replace('\n', ' ').replace(r"\r\n",
                                                          ' ')  # or json_data['general']['title'][0]['title']
                print(title)
                desc = row[4].replace('\n', ' ').replace(r"\r\n",
                                                         ' ')  # or json_data['general']['description'][0]['title']
                uri = row[5]  # or json_data['general']['identifier'][0]['entry']
                dom = None
                if 'legacyDiscipline' in json_data.keys():
                    legacy_name = json_data['legacyDiscipline']['name']
                    for d in domains.keys():
                        if legacy_name in domains[d]['edubases']:
                            dom = d
                    if not dom:
                        print(f'*******Legacy : <{legacy_name}> Missing')
                elif 'classification' in json_data.keys():
                    for classification in json_data['classification']:
                        if classification['purpose']['id'] == DOM_ENSEIGN_PURPOSE:
                            if 'taxonPath' in classification.keys():
                                for tp in classification['taxonPath']:
                                    for d in domains.keys():
                                        if tp['label'].lower().strip() in map(lambda str: str.lower().strip(), domains[d]['scolomfr']):
                                            dom = d
                                if not dom:
                                    print(f'*******New : <{tp["label"]}> Missing')
                                    if not tp['label'] in missing_count.keys():
                                        missing_count[tp['label']] = 1
                                    else:
                                        missing_count[tp['label']] += 1
                                if dom:
                                    if not tp['label'] in domaines_count.keys():
                                        domaines_count[tp['label']] = 1
                                    else:
                                        domaines_count[tp['label']] += 1
                if dom:
                    if not dom in disciplines_count.keys():
                        disciplines_count[dom] = 1
                    else:
                        disciplines_count[dom] += 1
                if dom:
                    label_writer.writerow([line, title , desc] + [categories.index(dom)])
                line += 1

    # close the communication with the PostgreSQL
    cur.close()
    print(total)
    pprint.pprint(disciplines_count)
    pprint.pprint({k: v for k, v in missing_count.items()})
    pprint.pprint(categories)
