# ETL Scripts

## Copy project
```shell
git clone https://github.com/pavelmaksimov/etl-scripts etl-scripts
```

## Create enviroments
```shell
cd etl-scripts
python3.7 -m venv venv
source venv/bin/activate
```

## Export from Marilyn to Clickhouse
```shell
cd etl-scripts
source venv/bin/activate
pip install -r requirements.txt

python project/marylin-clickhouse.py --help

python project/marylin-clickhouse.py \ 
  --marilyn-api-root <https://app.mymarilyn.ru> \ 
  --marilyn-token <token> \ 
  --marilyn-account <123> \ 
  --marilyn-project <1050> \ 
  --db-host <localhost> --db-port 9000 \ 
  --db-name <mydb> \ 
  --db-user <default> \ 
  --db-password <pass> \ 
  --start-date $(date -d "7 day ago" '+%Y-%m-%d') \ 
  --end-date $(date -d "today" '+%Y-%m-%d')
```

Logs save to 'etl-mary-clickhouse.log'

# Record for crontab

Will run every day at 00:00
```shell
0 0 * * * etl-scripts/venv/bin/python project/marilyn/marylin-clickhouse.py \ 
  --marilyn-api-root <https://app.mymarilyn.ru> \ 
  --marilyn-token <token> \ 
  --marilyn-account <123> \ 
  --marilyn-project <1050> \ 
  --db-host <localhost> --db-port 9000 \ 
  --db-name <mydb> \ 
  --db-user <default> \ 
  --db-password <pass> \ 
  --start-date $(date -d "7 day ago" '+%Y-%m-%d') \ 
  --end-date $(date -d "today" '+%Y-%m-%d')
```
