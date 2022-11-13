from __future__ import annotations
from modules.datalake import Datalake
from newsapi import NewsApiClient
from typing import Dict, List, Tuple
from datetime import date, timedelta
from pandas import DataFrame
from math import ceil
from pipelines.ml_pipeline import execute as ml_execute
from pipelines.analytial_pipeline import execute as analytic_execute
from pyspark import SQLContext, SparkContext, SparkConf
from pyspark.sql import SparkSession

newsapi = NewsApiClient(api_key='dccb66a5c9f84d71bcd6b496a27f44a4')


class NewsAPI:

    @staticmethod
    def get_articles(
            from_date: str = '2022-11-07',
            to_date: str = '2022-11-10'
            ) -> List[Dict]:
        """
        Get articles in the date range [from_date,to_date].

        Parameters:
            from_date (str)     : a string representing the beginning date.
            to_date (str)       : a string representing the ending date.
            query_term (str)

        Returns:
            all_articles (List) : a list of article dictionaries.
        """
        args = dict({
            'sources': 'bbc-news,the-verge',
            # 'domains': 'bbc.co.uk,techcrunch.com',
            'from_param': from_date,
            'to': to_date,
            'language': 'en',
            'sort_by': 'publishedAt',
            'page': 1
            })
        all_articles = newsapi.get_everything(**args)
        return all_articles['articles']

def get_week_start_end(
        particular_date: date = date.today()
        ) -> Tuple[str(date), str(date)]:
    """
    Get the start and end date of the current week.

    Parameters:
        particular_date (date)      : a particular day of the year.

    Returns:
        tuple(start,end) (Tuple)    : a tuple of the start and end of a week.
    """
    today = particular_date
    start = today - timedelta(days=today.weekday())
    end = start + timedelta(days=6)
    return str(start), str(end)


def list_to_dataframe(articles: List[Dict]) -> DataFrame:
    """
    Convert an array of articles to a pandas DataFrame.

    Parameters:
        articles (List[Dict])   : a list of articles.

    Returns:
        df_articles (DataFrame) : a DataFrame of al the articles.
    """
    df_articles = DataFrame.from_dict(articles, orient='columns')
    return df_articles

def week_of_month(dt: date) -> int:
    """
    Returns the week of the month for the specified date.

    Parameters:
        dt (datetime)   : a timestamp

    Returns:
        week (int)      : the week of the month
    """
    first_day = dt.replace(day=1)
    day_of_month = dt.day
    adjusted_day_of_month = day_of_month + first_day.weekday()
    week = int(ceil(adjusted_day_of_month/7.0))
    return week


def main():
    today = date.today()
    week = week_of_month(today)
    if not Datalake.file_ready(week_of_month=week, dt=today):
        start, end = get_week_start_end(particular_date=today)
        articles = NewsAPI.get_articles(from_date=start, to_date=end)
        df_articles = list_to_dataframe(articles=articles)
        Datalake.write_to_datalike(
                articles=df_articles,
                week_of_month=week,
                dt=today
                )
    df_articles = Datalake.read_from_datalake(week_of_month=week, dt=today)
    dict_articles = df_articles.to_dict('records')
    conf = SparkConf()\
           .setAppName("PySpark MySql - via JDBC")\
           .setMaster("local")\
           .set(
                "spark.driver.extraClassPath",
                "mysql-connector-j-8.0.31/mysql-connector-j-8.0.31.jar"
           )
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession
    analytic_execute(spark=spark, articles=dict_articles)
    df_articles = ml_execute(spark=spark, articles=dict_articles)
    Datalake.write_to_datalike(
        articles=df_articles,
        week_of_month=week,
        dt=today
        )
    # newsapi_pipeline = NewsAPI()
    # newsapi_pipeline.start()


if __name__ == "__main__":
    main()
