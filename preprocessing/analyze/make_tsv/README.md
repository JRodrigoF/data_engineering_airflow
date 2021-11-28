## prepare_to_tsv_kym.py

Prepares kym_unique.json memes data for conversion into table structure.


```bash
python prepare_to_tsv_kym.py -f ../../../sourcedata/kym_unique.json > kym_prepared.json
```

## putInTables.py

#creates tsv files in tsv directory
```bash
python putInTables.py -f kym_prepared.json
```
