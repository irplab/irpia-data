import csv
import pprint

import nltk
import psycopg2 as psycopg2

from domains_mappings import domains

from dotenv import dotenv_values

conn_params = dict(dotenv_values(".env"))

nltk.download('punkt')

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

    with open('edubases_domain_labeled_data_titles.csv', 'w', encoding='UTF8') as f_label_title:
        with open('edubases_domain_labeled_data_desc.csv', 'w', encoding='UTF8') as f_label_desc:
            label_title_writer = csv.writer(f_label_title, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            label_desc_writer = csv.writer(f_label_desc, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            label_title_header = ['id', 'text', 'label']
            label_title_writer.writerow(label_title_header)
            label_desc_writer.writerow(label_title_header)

            for row in cur:
                total += 1
                json_data = row[12]
                title = row[3].replace('\n', ' ').replace(r"\r\n",
                                                          ' ')
                print(title)
                desc = row[4].replace('\n', ' ').replace(r"\r\n",
                                                         ' ')
                uri = row[5]
                dom = None
                if 'legacyDiscipline' in json_data.keys():
                    legacy_name = json_data['legacyDiscipline']['name']
                    for d in domains.keys():
                        if legacy_name in domains[d]['edubases']:
                            dom = d
                    if not dom:
                        print(f'******* Legacy : <{legacy_name}> Missing')
                elif 'classification' in json_data.keys():
                    for classification in json_data['classification']:
                        if classification['purpose']['id'] == DOM_ENSEIGN_PURPOSE:
                            if 'taxonPath' in classification.keys():
                                for tp in classification['taxonPath']:
                                    for d in domains.keys():
                                        if tp['label'].lower().strip() in map(lambda str: str.lower().strip(),
                                                                              domains[d]['scolomfr']):
                                            dom = d
                                if not dom:
                                    print(f'******* New : <{tp["label"]}> Missing')
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
                    label_title_writer.writerow([line, title] + [categories.index(dom)])
                    sentences = nltk.sent_tokenize(desc, language='french')
                    for sentence in enumerate(sentences):
                        label_desc_writer.writerow([line, sentence[1]] + [categories.index(dom)])
                line += 1

    cur.close()
    print(total)
    pprint.pprint(disciplines_count)
    pprint.pprint({k: v for k, v in missing_count.items()})
    pprint.pprint(categories)
