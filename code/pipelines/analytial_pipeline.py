from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

db = "db"
user = "user"
pw = "password"


def createAuthor(
        spark: SparkSession,
        df_articles: DataFrame
        ) -> None:
    """
    Writes only new authors to the database table Author.

    Parameters:
        spark (SparkSession)    : a spark session.
        df_articles (DataFrame) : a pyspark DataFrame of the current
                                  weeks articles.
    """
    print("adding to db.Author...")
    df_Author = spark.read.format("jdbc")\
        .option("url", f"jdbc:mysql://localhost:3306/{db}") \
        .option("driver", "com.mysql.jdbc.Driver")\
        .option("dbtable", "Author") \
        .option("user", user)\
        .option("password", pw)\
        .load()
    df_Author = df_articles.select("author")\
        .withColumnRenamed("author", "name")\
        .distinct()\
        .join(
            df_Author.select('name'),
            ['name'],
            'anti'
        )
    df_Author.show(1, truncate=False)
    df_Author\
        .write.format("jdbc")\
        .option("url", f"jdbc:mysql://localhost:3306/{db}") \
        .option("driver", "com.mysql.jdbc.Driver")\
        .option("dbtable", "Author") \
        .option("user", user)\
        .option("password", pw)\
        .mode("append")\
        .save()

def createSource(
        spark: SparkSession,
        df_articles: DataFrame
        ) -> None:
    """
    Writes only new sources to the database table Source.

    Parameters:
        spark (SparkSession)    : a spark session.
        df_articles (DataFrame) : a pyspark DataFrame of the current
                                  weeks articles.
    """
    print("adding to db.Source...")
    df_Source = spark.read.format("jdbc")\
        .option("url", f"jdbc:mysql://localhost:3306/{db}") \
        .option("driver", "com.mysql.jdbc.Driver")\
        .option("dbtable", "Source") \
        .option("user", user)\
        .option("password", pw)\
        .load()
    df_Source = df_articles.select("source")\
        .withColumnRenamed('source','name')\
        .select('name')\
        .distinct()\
        .join(
            df_Source.select('name'),
            ['name'],
            'anti'
        )
    df_Source.show(1, truncate=False)
    df_Source\
        .write.format("jdbc")\
        .option("url", f"jdbc:mysql://localhost:3306/{db}") \
        .option("driver", "com.mysql.jdbc.Driver")\
        .option("dbtable", "Source") \
        .option("user", user)\
        .option("password", pw)\
        .mode("append")\
        .save()


def createArticle(
        spark: SparkSession,
        df_articles: DataFrame
        ) -> None:
    """
    Writes only new sources to the database table Source.

    Parameters:
        spark (SparkSession)    : a spark session.
        df_articles (DataFrame) : a pyspark DataFrame of the current
                                  weeks articles.
    """
    print("adding to db.Article...")
    df_Author = spark.read.format("jdbc")\
        .option("url", f"jdbc:mysql://localhost:3306/{db}") \
        .option("driver", "com.mysql.jdbc.Driver")\
        .option("dbtable", "Author") \
        .option("user", user)\
        .option("password", pw)\
        .load()\
        .withColumnRenamed("name", "author")\
        .withColumnRenamed("id", "author_id")\

    df_Source = spark.read.format("jdbc")\
        .option("url", f"jdbc:mysql://localhost:3306/{db}") \
        .option("driver", "com.mysql.jdbc.Driver")\
        .option("dbtable", "Source") \
        .option("user", user)\
        .option("password", pw)\
        .load()\
        .withColumnRenamed('name', 'source')\
        .withColumnRenamed('id', 'source_id')

    df_Article = df_articles.select("title", "description",'publishedAt','source','author')\
        .withColumnRenamed('publishedAt', 'timestamp')\
        .withColumn('timestamp', F.to_timestamp('timestamp'))\
        .join(
            df_Source,
            ['source'],
            'left'
        )\
        .join(
            df_Author,
            ['author'],
            'left'
        )\
        .select('source_id', 'author_id', 'title', 'description', 'timestamp')

    df_Article.show(1, vertical=True, truncate=False)
    df_Article.write.format("jdbc")\
        .option("url", f"jdbc:mysql://localhost:3306/{db}") \
        .option("driver", "com.mysql.jdbc.Driver")\
        .option("dbtable", "Article") \
        .option("user", user)\
        .option("password", pw)\
        .mode("append")\
        .save()

def execute(
        spark: SparkSession,
        articles: List[Dict]
        ) -> None:
    """
    A function to execute the various SQL table writes.

    Parameters:
        spark (SparkSession)    : a spark session.
        df_articles (DataFrame) : a pyspark DataFrame of the current
                                  weeks articles.
    """
    df_articles = spark.createDataFrame(data=articles)
    df_articles = df_articles\
                    .fillna({'author': 'unknown'})\
                    .withColumn("source", F.lower(F.col("source.name")))
    for function in [createAuthor, createSource, createArticle]:
        function(spark=spark, df_articles=df_articles)
