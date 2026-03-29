from pyspark.sql import functions as f


def run_all_validations(df):
    
    def validate_nulls(df,columns=["VendorID", "pickup_datetime"]):

        for col in columns:
            null_count = df.filter(f.col(col).isNull()).count()

            if null_count > 0:
                raise ValueError(f"Column {col} has {null_count} NULL values")

    #    print("Null validation passed")


    def validate_negative_values(df):
        
        if df.filter(f.col("trip_distance") < 0).count() > 0:
            raise ValueError("Negative trip_distance found")

        if df.filter(f.col("fare_amount") < 0).count() > 0:
            raise ValueError("Negative fare_amount found")

    #    print("Numeric validation passed")


    def validate_passenger_count(df):

        invalid = df.filter(f.col("passenger_count") <= 0).count()

        if invalid > 0:
            raise ValueError(f"{invalid} invalid passenger counts detected")

    #    print("Passenger count validation passed")

    print("All validations passed")