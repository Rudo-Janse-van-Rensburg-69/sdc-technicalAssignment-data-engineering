from transformers import pipeline, AutoTokenizer, DataCollatorForSeq2Seq
from typing import Dict, Tuple, List
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pandas import DataFrame

tokenizer = AutoTokenizer.from_pretrained("t5-small", model_max_length=512)

@F.udf(returnType=T.ArrayType(T.IntegerType()))
def tokenize_description_input_ids(description: str) -> List[int]:
    """
    A user-defined function to tokenize the description column.

    Parameters:
        description (str)   : an article description.

    Returns:
        tokens (List[int])  : an array of tokens.
    """
    return tokenizer(
            description,
            max_length=256,
            truncation=True
            )['input_ids']
@F.udf(returnType=T.ArrayType(T.IntegerType()))
def tokenize_description_attention_mask(description: str) -> List[int]:
    """
    A user-defined function to tokenize the description column.

     Parameters:
        description (str)   : an article description.

    Returns:
        attention mask (List[int])  : an array of bits.

    """
    return tokenizer(
            description,
            max_length=256,
            truncation=True
            )['attention_mask']


@F.udf(returnType=T.ArrayType(T.IntegerType()))
def tokenize_title(title: str) -> List[int]:
    """
    A user-defined function to tokenize the title column.

    Parameters:
        title (str)         : an article description.

    Returns:
        tokens (List[int])  : an array of tokens.
    """
    return tokenizer(
            title,
            max_length=128,
            truncation=True
            )['input_ids']


def execute(
        spark: SparkSession,
        articles: List[Dict]
        ) -> DataFrame:
    """
    A function to execute the machine learning pipeline.

    Parameters:
        spark (SparkSession)    : a spark session.
        articles (List[Dict])   : a list f article dictionaries.

    Returns:
        df_articles             : a DataFrame of articles.
    """
    df_articles = spark.createDataFrame(
                data=articles
                ).select(
                    'title',
                    'description',
                ).withColumn(
                    'input_ids',
                    tokenize_description_input_ids(
                       F.col('description')
                    )
                ).withColumn(
                    'attention_mask',
                     tokenize_description_attention_mask(
                        F.col('description')
                    )
                ).withColumn(
                    'labels',
                    tokenize_title(
                        F.col('title')
                    )
                ).select('title','labels','description','input_ids','attention_mask')
    df_articles.limit(3).show(vertical=True, truncate=False)
    return df_articles.toPandas()
