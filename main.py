from typing import Union
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from langchain.llms import OpenAI
from langchain.sql_database import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain.chains import create_sql_query_chain
from langchain.chat_models import ChatOpenAI

import psycopg2
from psycopg2.extras import RealDictCursor
from langchain.llms import OpenAI
from langchain.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain


import environ
env = environ.Env()
environ.Env.read_env()

API_KEY = env('OPENAI_API_KEY')
DATABASE_USER = env('DATABASE_USER')
DATABASE_PASSWORD = env('DATABASE_PASSWORD')
DATABASE_HOST = env('DATABASE_HOST')
DATABASE_PORT = env('DATABASE_PORT')
DATABASE = env('DATABASE')

conn = psycopg2.connect(
    host=DATABASE_HOST,
    port=DATABASE_PORT,
    user=DATABASE_USER,
    password=DATABASE_PASSWORD,
    database=DATABASE
)

cur = conn.cursor(cursor_factory=RealDictCursor)

db = SQLDatabase.from_uri(f"postgresql+psycopg2://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE}")
llm = OpenAI(temperature=0, verbose=True, model_name="gpt-4", openai_api_key=API_KEY)
db_chain = SQLDatabaseChain.from_llm(llm, db, verbose=True, return_intermediate_steps=True)

# Create db chain
QUERY = """
Given an input question, first create a syntactically correct postgresql query to run, then look at the results of the query and return the answer please when you are return the id like prodcut_id,user_id and other stuff also add the name and other properties using the foreign key so end users understand it form the table like name.
Use the following format:

Question: Question here
SQLQuery: SQL Query to run
SQLResult: Result of the SQLQuery
Answer: Final answer here

{question}
"""

def get_prompt(question):
    print(question)
    prompt = question
    try:
        question = QUERY.format(question=prompt)
        print("question", question)
        answer = db_chain(question)

        print("answer", answer)


        records = []
        if (answer["intermediate_steps"] and answer["intermediate_steps"][1]):
            cur.execute(answer["intermediate_steps"][1])
            records = cur.fetchall()

        print(records)
        return {
             "data": {
            "records": records,
            "query" : answer["intermediate_steps"][1] or null
        } 
        }
        # return {
        #     'query': answer["intermediate_steps"][1] or null,
        #     'data': db.run(answer["intermediate_steps"][1]) if answer["intermediate_steps"][1] else null,
        # }
        # return response
    except Exception as e:
        print(e)

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/execute")
def execute():  

#     return {
#     "data": {
#         "records": [
#             {
#                 "user_id": 1,
#                 "username": "user1",
#                 "product_count": 4
#             },
#             {
#                 "user_id": 5,
#                 "username": "user5",
#                 "product_count": 1
#             },
#             {
#                 "user_id": 10,
#                 "username": "user10",
#                 "product_count": 1
#             },
#             {
#                 "user_id": 9,
#                 "username": "user9",
#                 "product_count": 1
#             },
#             {
#                 "user_id": 7,
#                 "username": "user7",
#                 "product_count": 1
#             }
#         ],
#         "query": "SELECT \"users\".\"user_id\", \"users\".\"username\", COUNT(\"sales\".\"product_id\") AS \"product_count\"\nFROM \"sales\"\nJOIN \"users\" ON \"sales\".\"user_id\" = \"users\".\"user_id\"\nGROUP BY \"users\".\"user_id\", \"users\".\"username\"\nORDER BY \"product_count\" DESC\nLIMIT 5;"
#     }
# }
    # return {"data":{"records":[{"user_id":1,"product_count":4}],"query":"SELECT \"user_id\", COUNT(*) as \"product_count\" \nFROM sales \nGROUP BY \"user_id\" \nORDER BY \"product_count\" DESC \nLIMIT 1;"}}

    return {"data":{"records":[{"Year":2023,"Products Sold":7},{"Year":2022,"Products Sold":2},{"Year":2021,"Products Sold":1}],"query":"SELECT EXTRACT(YEAR FROM \"sale_date\") AS \"Year\", COUNT(\"product_id\") AS \"Products Sold\"\nFROM sales\nWHERE \"sale_date\" BETWEEN (CURRENT_DATE - INTERVAL '5 years') AND CURRENT_DATE\nGROUP BY \"Year\"\nORDER BY \"Year\" DESC\nLIMIT 5;"}}
    cur.execute("""SELECT "product_id", EXTRACT(YEAR FROM "sale_date") AS "year", COUNT(*) AS "sales_count"
    FROM sales
    GROUP BY "product_id", "year"
    ORDER BY "sales_count" DESC
    LIMIT 5;""")
    records = cur.fetchall()

    print(records)
    return { "data": {
        "records": records,
        "query" : """SELECT "product_id", EXTRACT(YEAR FROM "sale_date") AS "year", COUNT(*) AS "sales_count"
    FROM sales
    GROUP BY "product_id", "year"
    ORDER BY "sales_count" DESC
    LIMIT 5;"""
    } }

@app.get("/get-query/{question}")
def read_item(question: str, q: Union[str, None] = None):
    return get_prompt(question)
    # return {"question": question, "answers": get_prompt(question)}