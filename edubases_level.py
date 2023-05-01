import csv
import pprint

import nltk
import psycopg2 as psycopg2
from domains_mappings import domains

nltk.download('punkt')

from dotenv import dotenv_values

conn_params = dict(dotenv_values(".env"))

EDU_LEVEL_PURPOSE = 'http://data.education.fr/voc/scolomfr/concept/educational_level'

if __name__ == '__main__':
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    cur.execute('SELECT * from teaching_sheets')

    domaines_index = domains.keys()

    count = 0
    total = 0
    line = 0
    old_levels_count = {}
    new_levels_count = {}
    domaines_count = {}
    missing_count = {}
    level_1_levels = []
    level_2_levels = []
    scolomfr_level_ids = []
    scolomfr_level_labels = []
    old_level_classification_count = 0
    new_level_classification_count = 0

    categories = list(domains.keys())

    # display the PostgreSQL database server version
    with open('edubases_level_labeled_data_titles.csv', 'w', encoding='UTF8') as f_label_title:
        with open('edubases_level_labeled_data_desc.csv', 'w', encoding='UTF8') as f_label_desc:
            label_title_writer = csv.writer(f_label_title, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            label_desc_writer = csv.writer(f_label_desc, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            label_title_header = ['id', 'text', 'label']
            label_title_writer.writerow(label_title_header)
            label_desc_writer.writerow(label_title_header)

            for row in cur:
                total += 1
                json_data = row[12]
                title = row[3].replace('\n', ' ').replace(r"\r\n",
                                                          ' ')  # or json_data['general']['title'][0]['title']
                # print(title)
                desc = row[4].replace('\n', ' ').replace(r"\r\n",
                                                         ' ')  # or json_data['general']['description'][0]['title']
                uri = row[5]  # or json_data['general']['identifier'][0]['entry']
                selected_level = None
                if 'levelTree' in json_data.keys():
                    old_level_classification_count += 1
                    for entry in json_data['levelTree']:
                        if entry['level'] == 1:
                            if entry['value'] not in level_1_levels:
                                level_1_levels.append(entry['value'])
                        if entry['level'] == 2:
                            if entry['value'] not in level_2_levels:
                                level_2_levels.append(entry['value'])
                            if not entry['value'] in old_levels_count.keys():
                                old_levels_count[entry['value']] = 1
                            else:
                                old_levels_count[entry['value']] += 1
                    # for d in domains.keys():
                    #     if legacy_name in domains[d]['edubases']:
                    #         selected_domain = d
                    # if not selected_domain:
                    #     print(f'*******Legacy : <{legacy_name}> Missing')
                elif 'classification' in json_data.keys():
                    new_level_classification_count += 1
                    for classification in json_data['classification']:
                        if classification['purpose']['id'] == EDU_LEVEL_PURPOSE:
                            if 'taxonPath' in classification.keys():
                                for tp in classification['taxonPath']:
                                    if type(tp) is dict:
                                        if tp['id'] not in scolomfr_level_ids:
                                            scolomfr_level_ids.append(tp['id'])
                                            scolomfr_level_labels.append(tp['label'])
                                        key = f"{tp['id']} - {tp['label']}"
                                        if not key in new_levels_count.keys():
                                            new_levels_count[key] = 1
                                        else:
                                            new_levels_count[key] += 1
                if selected_level:
                    label_title_writer.writerow([line, title] + [categories.index(selected_level)])
                    sentences = nltk.sent_tokenize(desc, language='french')
                    for sentence in enumerate(sentences):
                        label_desc_writer.writerow([line, sentence[1]] + [categories.index(selected_level)])
                line += 1

    # close the communication with the PostgreSQL
    cur.close()
    level_1_levels.sort()
    level_2_levels.sort()
    scolomfr_level_ids.sort()
    scolomfr_level_labels.sort()
    pprint.pprint(level_1_levels)
    pprint.pprint(level_2_levels)
    print(len(scolomfr_level_ids))
    pprint.pprint(scolomfr_level_ids)
    pprint.pprint(scolomfr_level_labels)
    print("*******************************************")
    pprint.pprint(old_levels_count)
    pprint.pprint(new_levels_count)
    print("*******************************************")
    print(old_level_classification_count)
    print(new_level_classification_count)
