from levels_mappings import levels
from domains_mappings import domains


def write_labels_to_file(labels: list[str], file: str):
    with open(file, 'w') as fp:
        for item in labels:
            # write each item on a new line
            fp.write("%s\n" % item)


write_labels_to_file(levels.keys(), 'level_ids.txt')
write_labels_to_file(map(lambda l: l['label'], levels.values()), 'level_labels.txt')
write_labels_to_file(domains.keys(), 'domain_ids.txt')
write_labels_to_file(map(lambda l: l['label'], domains.values()), 'domains_labels.txt')
