import os
import petl
import psycopg2
import requests
import datetime
import json
import decimal

# get data from  configuration file
with open("config.json", "r") as config_file:
    try:
        config = json.load(config_file)
    except Exception as e:
        print("could not read configuration file" + str(e))
        sys.exit()

# read settings from configuration file
startDate = config["startDate"]
url = config["url"]
destServer = config["server"]
destDatabase = config["database"]

# request data from URL
try:
    BOCResponse = requests.get(url + startDate)
except Exception as e:
    print("could not make request:" + str(e))
    sys.exit()

# initialize list of lists for data storage
BOCDates = []
BOCRates = []

# check response status and process BOC JSON object
if BOCResponse.status_code == 200:  # 200 means okay, 404 meas not found
    BOCRaw = json.loads(BOCResponse.text)

    # extract observation data into column arrays
    for row in BOCRaw["observations"]:
        BOCDates.append(datetime.datetime.strptime(row["d"], "%Y-%m-%d"))
        BOCRates.append(decimal.Decimal(row["FXUSDCAD"]["v"]))

    # create petl table from column arrays and rename the columns
    exchangeRates = petl.fromcolumns(
        [BOCDates, BOCRates], header=["date", "rate"]
    )

    # load expense document
    try:
        expenses = petl.io.xlsx.fromxlsx("Expenses.xlsx", sheet="Github")
    except Exception as e:
        print("could not open expenses.xlsx:" + str(e))
        sys.exit()

    # join tables
    expenses = petl.outerjoin(exchangeRates, expenses, key="date")

    # fill down missing values
    expenses = petl.filldown(expenses, "rate")

    # remove dates with no expenses
    expenses = petl.select(expenses, lambda rec: rec.USD != None)

    # add CDN column
    expenses = petl.addfield(
        expenses, "CAD", lambda rec: decimal.Decimal(rec.USD) * rec.rate
    )

    with open("my_connection.json") as f:
        db = json.load(f)
        try:
            conn = psycopg2.connect(**db)
        except Exception as e:
            print("could not connect to server:" + str(e))
            sys.exit()

    create_table_expenses = """
    DROP TABLE IF EXISTS expenses;
    CREATE TABLE expenses
    (
        date timestamp,
        "USD" money,
        rate DECIMAL(6,5),
        "CAD" money
    );
    """

    with conn, conn.cursor() as cur:
        cur.execute(create_table_expenses)

    # populate Expenses database table
    try:
        petl.io.todb(expenses, conn, "expenses")
    except Exception as e:
        print("could not write to database:" + str(e))
