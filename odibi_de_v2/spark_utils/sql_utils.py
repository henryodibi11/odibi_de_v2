def generate_create_table_sql(df, table_name: str, schema: str = "dbo") -> str:
   """
   Generate a CREATE TABLE statement for a SQL Server database
   based on a Spark DataFrame schema.

   Parameters
   ----------
   df : pyspark.sql.DataFrame
       The DataFrame whose schema will be used.
   table_name : str
       Name of the target table.
   schema : str, default="dbo"
       Schema name to use in SQL Server.

   Returns
   -------
   str : CREATE TABLE statement as a string.
   """

   # Map Spark SQL types to SQL Server types
   type_mapping = {
       "StringType": "VARCHAR(255)",
       "IntegerType": "INT",
       "LongType": "BIGINT",
       "DoubleType": "FLOAT",
       "FloatType": "REAL",
       "BooleanType": "BIT",
       "TimestampType": "DATETIME2",
       "DateType": "DATE",
       "DecimalType": "DECIMAL(18,4)",
       "ShortType": "SMALLINT",
   }

   columns = []
   for field in df.schema.fields:
       spark_type = type(field.dataType).__name__
       sql_type = type_mapping.get(spark_type, "VARCHAR(255)")  # fallback
       col_def = f"[{field.name}] {sql_type}"
       columns.append(col_def)

   cols_sql = ",\n  ".join(columns)
   create_sql = f"CREATE TABLE [{schema}].[{table_name}] (\n  {cols_sql}\n);"
   return create_sql