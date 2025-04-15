from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType


def flatten_nested_structs(df: DataFrame, sep: str = "_") -> DataFrame:
   """
   Recursively flatten all nested StructType columns in a Spark DataFrame.

   Args:
       df (DataFrame): Input Spark DataFrame.
       sep (str): Separator for nested field names.

   Returns:
       DataFrame: Flattened DataFrame with no nested StructType columns.
   """

   def _flatten(schema, prefix=""):
       fields = []
       for field in schema.fields:
           col_name = f"{prefix}.{field.name}" if prefix else field.name
           alias_name = f"{prefix}{sep}{field.name}" if prefix else field.name

           if isinstance(field.dataType, StructType):
               fields += _flatten(field.dataType, prefix=col_name)
           else:
               fields.append(col(col_name).alias(alias_name))
       return fields

   return df.select(*_flatten(df.schema))