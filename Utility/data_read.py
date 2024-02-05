class read_data:
    def __init__(self, url, user, table, password, query, driver, spark):
        self.url = url
        self.user = user
        self.password = password
        self.driver = driver
        self.query = query
        self.table = table
        self.spark = spark

    def db_read_table(self):
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", self.table) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", self.driver) \
            .load()
        return df

    def db_read_query(self):
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.url) \
            .option("query", self.query) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", self.driver) \
            .load()
        return df
