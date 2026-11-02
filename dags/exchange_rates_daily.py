# ============================================================
# exchange_rates_daily.py
# Airflow version of notebooks/daily_exchange_rates_ETL.ipynb
#
# Pipeline:  create_table ─┐
#            extract ──> transform_and_validate ──> load ──> verify
# ============================================================


# ============================================================
# 1. IMPORTS: borrow tools from other libraries
# ============================================================
from datetime import timedelta      # timedelta = a length of time, e.g. "1 minute"
import logging                      # logging = Python's tool for writing messages into a log

import pandas as pd                 # pandas = tables in Python (same as your notebook); "pd" is its short name
import pendulum                     # pendulum = dates that know their time zone (comes with Airflow)
import requests                     # requests = calls web addresses (the API)
from airflow.sdk import dag, task   # the two Airflow decorators: @dag (pipeline) and @task (step)
from airflow.providers.postgres.hooks.postgres import PostgresHook   # Airflow's helper for talking to PostgreSQL

log = logging.getLogger(__name__)   # create our "pen" for the log; we write with log.info("...")


# ============================================================
# 2. SETTINGS: values used in several places, written once
# ============================================================
URL = "https://api.frankfurter.dev/v2/rates"            # the API address (same as your notebook)
PARAMS = {"base": "EUR", "quotes": "USD,GBP,CHF,CNY"}   # what we ask for: EUR against 4 currencies
POSTGRES_CONN_ID = "exchange_rates_pg"                  # name of the saved connection in Airflow (Admin → Connections);
                                                        # the password lives there, never in this file


# ============================================================
# 3. THE PIPELINE (the DAG)
# ============================================================
@dag(                                                               # decorator: "the function below is a pipeline"
    schedule="0 12 * * *",                                          # cron timer: minute 0, hour 12, every day/month/weekday
    start_date=pendulum.datetime(2026, 11, 1, tz="Europe/Athens"),  # the schedule starts counting from 1 Nov 2026, Athens time
    catchup=False,                                                  # don't go back and run the days already missed
    tags=["exchange_rates"],                                        # a label to filter by on the website
)
def exchange_rates_daily():                                         # the pipeline itself; everything indented below belongs to it

    # --------------------------------------------------------
    # STEP 1: create_table — make sure the table exists
    # --------------------------------------------------------
    @task                                                       # decorator: "the function below is one step"
    def create_table():                                         # define the step (it needs no input)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)  # open the "door" to your database using the saved connection
        hook.run("""
            CREATE TABLE IF NOT EXISTS exchange_rates (
                date  DATE             NOT NULL,
                base  TEXT             NOT NULL,
                quote TEXT             NOT NULL,
                rate  DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (date, base, quote)
            );
        """)                                                    # run this SQL: create the table only if it isn't there yet;
                                                                # PRIMARY KEY = no two rows with the same date+base+quote
        log.info("Table exchange_rates is ready")               # write a message into the log

    # --------------------------------------------------------
    # STEP 2: extract — get the latest rates from the API
    #         (your notebook's EXTRACT cell)
    # --------------------------------------------------------
    @task(retries=3, retry_delay=timedelta(minutes=1))          # a step; if it fails, retry up to 3 times, 1 minute apart
    def extract():                                              # define the step (it needs no input)
        response = requests.get(URL, params=PARAMS, timeout=10) # call the API; give up if no answer within 10 seconds
        response.raise_for_status()                             # if the API answered with an error code, stop (task turns red)
        data = response.json()                                  # turn the JSON answer into a Python list of dictionaries
        log.info("Records received: %s", len(data))             # log how many records came back (%s is replaced by the number)
        log.info("Data: %s", data)                              # log the records themselves
        return data                                             # hand the list to the next step (Airflow stores it as an XCom)

    # --------------------------------------------------------
    # STEP 3: transform_and_validate — clean and check the data
    #         (your notebook's TRANSFORM + VALIDATE cells)
    # --------------------------------------------------------
    @task                                                       # a step
    def transform_and_validate(data):                           # "data" = what extract() returned; Airflow delivers it
        df = pd.DataFrame(data)                                 # list of dictionaries → pandas table
        df["date"] = pd.to_datetime(df["date"])                 # text dates ("2026-11-02") → real dates
        log.info("\n%s", df)                                    # print the table into the log (\n = start on a new line)

        if df.empty:                                            # if the table has no rows at all...
            raise ValueError("Validation failed: the API returned no records")   # ...stop with an error (task turns red)

        if df.isna().sum().sum() != 0:                          # count missing values in the whole table; if not 0...
            raise ValueError("Validation failed: missing values found")          # ...stop with an error

        if (df["rate"] <= 0).any():                             # if any rate is zero or negative...
            raise ValueError("Validation failed: invalid exchange rate")         # ...stop with an error

        if df.duplicated(subset=["date", "base", "quote"]).any():  # if any date+base+quote appears twice...
            raise ValueError("Validation failed: duplicate exchange-rate records")  # ...stop with an error

        log.info("Validation passed")                           # we only get here if every check passed

        df["date"] = df["date"].dt.strftime("%Y-%m-%d")         # real dates → text again, because data passed between
                                                                # steps must be JSON, and JSON can't hold a date
        return df.to_dict(orient="records")                     # pandas table → list of dictionaries, one per row; hand it on

    # --------------------------------------------------------
    # STEP 4: load — upsert the rows into PostgreSQL
    #         (replaces the Spark MERGE; same idea as your prototype)
    # --------------------------------------------------------
    @task                                                       # a step
    def load(rows):                                             # "rows" = what transform_and_validate() returned
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)  # open the door to your database
        sql = """
            INSERT INTO exchange_rates (date, base, quote, rate)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (date, base, quote)
            DO UPDATE SET rate = EXCLUDED.rate;
        """                                                     # the upsert: insert the row; if that date+base+quote already
                                                                # exists, update its rate instead. %s = a value filled in later;
                                                                # EXCLUDED = the row we tried to insert
        conn = hook.get_conn()                                  # open a real connection to the database
        with conn:                                              # transaction: save everything at the end, or nothing on error
            with conn.cursor() as cursor:                       # a cursor sends SQL commands (closes itself at the end)
                for row in rows:                                # go through the rows one by one
                    cursor.execute(sql, (row["date"], row["base"], row["quote"], row["rate"]))  # run the upsert,
                                                                # filling the four %s with this row's values
        conn.close()                                            # close the connection

        log.info("Upserted %s rows", len(rows))                 # log how many rows we wrote
        return sorted({row["date"] for row in rows})            # collect the distinct dates we loaded, sort them,
                                                                # and hand them to verify

    # --------------------------------------------------------
    # STEP 5: verify — read back what we just loaded
    #         (your notebook's VERIFY LOAD cell)
    # --------------------------------------------------------
    @task                                                       # a step
    def verify(dates):                                          # "dates" = what load() returned
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)  # open the door to your database

        total = hook.get_first("SELECT COUNT(*) FROM exchange_rates;")[0]  # run the query, take the first row,
                                                                            # and its first value = the row count
        log.info("Total rows in table: %s", total)              # log the count

        rows = hook.get_records(
            "SELECT date, base, quote, rate FROM exchange_rates "
            "WHERE date = ANY(%s::date[]) ORDER BY date, quote;",
            parameters=(dates,),
        )                                                       # get all rows whose date is in our list of loaded dates
        for row in rows:                                        # go through those rows...
            log.info("%s", row)                                 # ...and write each one into the log

        if len(rows) == 0:                                      # if nothing came back...
            raise ValueError("Verify failed: loaded rows not found in the table")  # ...something is wrong: stop

    # ========================================================
    # 4. THE ORDER OF THE STEPS
    #    Nothing runs here. These lines only tell Airflow
    #    which steps exist and which one comes after which.
    # ========================================================
    table_ready = create_table()                    # step 1 exists
    rows = transform_and_validate(extract())        # step 2, then step 3 (extract's result is passed into step 3)
    loaded_dates = load(rows)                       # then step 4 (step 3's result is passed into step 4)
    table_ready >> loaded_dates                     # ">>" = "must finish before": create_table before load
    verify(loaded_dates)                            # then step 5 (step 4's result is passed into step 5)


# ============================================================
# 5. SWITCH THE PIPELINE ON
#    Calling the outer function once registers it with Airflow.
# ============================================================
exchange_rates_daily()
