#dag -directed acyclic graph

#tasks : 1) fetch amazon data(extract) 2) clean data(transform) 3) create and store data in a table on postgres(load) 
#operators : python operator and postgres operator
#hooks- to allows connection
#dependencies


from airflow import DAG
from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# fetch data from API(extract) and clean data (transform)

def fetch_amazon_data(num_books,ti):
    #base url of amazon api of books
    base_url=f"https://www.amazon.in/s?k=data+engineer+books"

    books =[]
    # To keep track of seen titles
    seen_title = set() 

    page=1

    while len(books) < num_books:
        url = f"{base_url}&page={page}"
        # Send a GET request to the Amazon API
        response = requests.get(url,headers=headers) # type: ignore
        
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the HTML content of the page
            soup=BeautifulSoup(response.content,'html.parser')

            # Find all the book elements on the page
            books_elements=soup.find_all("div",{"class":"s-result-item"})

            # Loop through each book element and extract the title and author
            for book in books_elements:
                title = book.find("span",{"class":"a-text-normal"})
                author = book.find("a",{"class":"a-size-base"})
                price = book.find("span",{"class":"a-price-whole"})
                rating = book.find("span",{"class":"a-icon-alt"})

                if title and author and price and rating:
                    book_title=title.text.strip()

                    # Check if the title has already been seen
                    if book_title not in seen_title:
                        seen_title.add(book_title)
                        books.append({
                            "Title": book_title,
                            "Author": author.text.strip(),
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip()
                        })

                # Increment the page number
                page += 1
            else:
                print("Failed to fetch data from Amazon API")
                break

        # Limit to the requested number of books
        books = books[:num_books]

        # convert the list of dictionaries into a pandas dataframe
        df = pd.DataFrame(books)

        # Remove duplicates based on title column
        df= pd.drop_duplicates(subset=["Title"],inplace=True)
 
        # Push the dataframe to XCom
        ti.xcom_push(key="book_data",value=df.to_dict('records'))

    # 3) create and store data in a table on postgres(load)

    def insert_book_data_to_postgres(ti):
        # Get the book data from XCom
        book_data=ti.xcom_pull(key="book_data",task_ids='fetch_book_data')
        if not book_data:
            raise ValueError("Book data not found in XCom")
        
        # Connect to the PostgreSQL database
        postgres_hook= PostgresHook(postgres_conn_id="books_connection")

        # Insert the book data into the "books" table
        insert_query="""
        INSERT INTO books (title, author, price, rating)
        VALUES (%s, %s, %s, %s)
        """
        for book in book_data:
            postgres_hook.run(insert_query,parameters=(book['Title'],book['Author'],book['Price'],book['Rating']))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag= DAG(
    'fetch_and_store_amazon_books',
    default_args=default_args,
    description='fetch and store amazon books data from API and store in postgres', 
    schedule_interval=timedelta(days=1),
)


#operators : python operator and postgres operator
#hooks - to allows connection to postgres

fetch_amazon_data_task=PythonOperator(
    task_id='fetch_book_data',
    python_callable=fetch_amazon_data,
    op_kwargs=[{"num_books":100}], # Number of books to fetch
    dag=dag,
)

create_table_task=PostgresOperator(
    task_id='create_table',
    postgres_conn_id="books_connection",
    sql="""
    CREATE TABLE IF NOT EXISTS books(
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT.
        price TEXT,
        rating TEXT
    ); 
    """,
    dag=dag,
)

insert_book_data_to_postgres_task=PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_to_postgres, # type: ignore
    dag=dag,
)

# dependencies
fetch_amazon_data_task>>create_table_task>>insert_book_data_to_postgres_task