# Initial cleansing scripts

### remove_duplicates_kym.py

Removes duplicate entries from kym.json. Duplicates are indicated based on 'url' and 'last_update_source' attributes.

**Usage**
```bash
python remove_duplicates_kym.py -f ../../sourcedata/kym.json > ../../sourcedata/kym_unique.json
```
