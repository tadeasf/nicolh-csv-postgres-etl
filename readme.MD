# ETL pipeline: csv to postgreSQL

Setup pro import csv souborů ze sftp serveru do postgresql databáze via sqlalchemy.

## Jak to funguje?

1. config.py: naloaduje environmental variables z .env souboru v kořenu projektu
2. csv_importer.py: utility skript pro upload nových csvček na sftp server
3. models.py: definuje tabulky v postgresql databázi

### csv_postgres_etl.py

Tenhle skript je naše pipelina. Běží nonstop v pozadí a co hodinu se kouká do /home/tadeas/nicolh/csvs, jestli se ve složce náhodou neobjevil nový csv soubor. Pokud se objevil, naimportuje menší csvčka do paměti a rovnou appenduje do csv_data tabulky v postgresql databázi (nebo ji vytvoří, pokud neexistuje). V případě větších csvs jsem implementoval batch import/export, aby nám nedošla paměť.

### pandas_transformation.py

Tohle je skript, kam můžeš přidávat transformace v pandas. dycky si definuj funkci, naimportuj jí do cvs_postgres_etl a přidej do funkce process_dataframe. Je to lepší dělat takhle, páčto je pak kód easier to maintain a nebude ti bobtnat etl pipelina.

## TODOs

Skript, kterej bude transformovat data v postgresql via sqlalchemy