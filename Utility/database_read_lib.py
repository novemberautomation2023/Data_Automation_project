

def db_read(url,user,password,query,driver,spark):
    df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("query", query) \
            .option("user", user) \
            .option("password", password) \
            .option("driver",driver)\
            .load()
    return df



