import pandas as pd

gar = pd.read_csv("gar_domain_labeled_data_titles.csv")
edu = pd.read_csv("edubases_domain_labeled_data_titles.csv")
gar.label = 0
edu.label = 1
#combine all files in the list
combined_csv = pd.concat([gar, edu ])
#export to csv
combined_csv.to_csv( "combined_csv.csv", index=False, encoding='utf-8-sig')