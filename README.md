# IRPIA data preprocessing scripts 

## Entry points

### Generation of training data for classification by teaching domain

- **Edubase**

Create the .env file from .env.example and fill in the database identifiers.

Execute :

```shell
python3 edubases_domain.py
```
This will generate 2 files : edubases_domain_labeled_data_titles.csv and edubases_domain_labeled_data_desc.csv, containing respectively full text titles and descriptions with domain labels. 

Label numbers are given by the order of the dictionary keys in the domains_mappings.py file.

- **GAR OAI access point**

```shell
python3 gar_domain.py
```

This will generate 2 files : gar_domain_labeled_data_titles.csv and gat_domain_labeled_data_desc.csv, containing respectively full text titles and descriptions with domain labels.

Label numbers are given by the order of the dictionary keys in the domains_mappings.py file.
### Generation of training data for classification by teaching level

- **GAR OAI access point**

```shell
python3 gar_level.py
```

This will generate 2 files : gar_level_labeled_data_titles.csv and gar_level_labeled_data_desc.csv, containing respectively full text titles and descriptions with level labels. 

Label numbers are given by the order of the dictionary keys in the levels_mappings.py file.

