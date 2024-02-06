import logging

logger = logging.getLogger()
#NDIWEC
import pandas as pd

def read_file(path : str ,type : str ,spark, header = None, delimiter=None, schema=None, multiline=None ):
      try:
          if type.lower() == 'csv':
                if header is None and delimiter is None and schema is None:
                    df = spark.read.csv(path)
                    logger.info(f"{type} file has been read successfully from path {path}")

                elif header is not None and delimiter is not None and schema is None:
                    df = spark.read.option("header", header).\
                                    option("delimiter",delimiter).csv(path)
                    logger.info(f"{type} file has been read successfully")

                elif header is not None and delimiter is not None and schema is not None:
                    df = spark.read.schema(schema).option("header", header). \
                          option("delimiter",delimiter).csv(path)
                    logger.info(f"{type} file has been read successfully")

          elif type.lower() == 'json':
            if multiline is None and schema is None:
                df = spark.read.option("multiline", False).json(path)
                logger.info(f"{type} file has been read successfully")

            elif schema is not None and multiline is not None:
                df = spark.read.schema(schema).option("multiline", multiline).json(path)

          elif type.lower() == 'parquet':
            df = spark.read.parquet(path)
            logger.info(f"{type} file has been read successfully")

          elif type.lower() == 'avro':
              df = spark.read.avro(path)
              logger.info(f"{type} file has been read successfully")

          elif type.lower() == 'textfile':
              df = spark.read.text(path)
              logger.info(f"{type} file has been read successfully")

          elif type.lower =='excel':
              pands_df = pd.read_excel(path)
              df = spark.createDataFrame(pands_df)
              logger.info(f"{type} file has been read successfully")

          else:
              print("Provide correct file type")
              logger.critical("Provide file has no code to read, Please provide new type of create code")

      except:
          df = spark.read.csv(path='defaultpath')
          return df
      finally:
          print("cleaning up code")
      return df





