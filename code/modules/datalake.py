from pandas import DataFrame
from datetime import date
from pathlib import Path
import os


class Datalake:
    """A holder of datalake-related utilities."""

    @staticmethod
    def file_ready(
            week_of_month: int,
            dt: date
            ) -> bool:
        """
        A function to see if the datalake already has the relevant file.

        Parameters:
            dt (datetime)           : A timestamp for when the articles are
                                      from.
            week_of_month (int)     : The week of the month.

        Returns:
            file_exists (bool)      : a boolean determining whether the file
                                      exists or not.
        """
        month_year = dt.strftime("%Y-%m")
        file_exists = os.path.exists(
                f"../datalake/{str(month_year)}/{week_of_month}.gzip"
                )
        return file_exists

    @staticmethod
    def write_to_datalike(
            articles: DataFrame,
            week_of_month: int,
            dt: date
            ) -> None:
        """
        Writes a dataframe to the datalake.

        Parameters:
            dt (datetime)           : A timestamp for when the articles are
                                      from.
            week_of_month (int)     : The week of the month.
            articles (DataFrame)    : A DataFrame containing articles around
                                      a particular timestamp.

        """
        month_year = dt.strftime("%Y-%m")
        path = f"../datalake/{str(month_year)}"
        Path(path).mkdir(
                parents=True,
                exist_ok=True
                )
        articles.to_parquet(
                f"{path}/{week_of_month(dt)}.gzip",
                compression='gzip'
                )
        articles.to_excel(f"{path}/{week_of_month}.xlsx")
