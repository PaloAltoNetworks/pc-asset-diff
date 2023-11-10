# Setup
Install PCPI Python Integration for Prisma Cloud

```bash
python3 -m pip install pcpi
```

Set up the config json file, 'conf.json'
Time stamps are in unix time and include 3 extra decimal places for mili seconds. An example has been included

```json
{
    "earlier_time":1691478000000,
    "later_time":1697958000000
}
```

# Runtime
```bash
python3 asset_diff.py
```