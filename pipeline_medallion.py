from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim
from pyspark.ml.feature import StringIndexer, Tokenizer, StopWordsRemover, HashingTF, IDF, VectorAssembler, PCA
from pyspark.ml.clustering import KMeans
import sys
import os

def main(input_path, output_path):
    spark = SparkSession.builder.appName("MedallionProductClustering").getOrCreate()

    # === Bronze Layer: Load raw data ===
    print("Loading raw data (Bronze) from:", input_path)
    df_raw = spark.read.option("header", True).csv(input_path)

    # === Silver Layer: Data cleaning & feature engineering ===
    df_clean = df_raw.withColumn("category_clean", lower(trim(col("category")))) \
                     .withColumn("rating", col("rating").cast("double")) \
                     .na.drop(subset=["category_clean", "rating"])

    indexer = StringIndexer(inputCol="category_clean", outputCol="categoryIndex", handleInvalid="skip")
    df_indexed = indexer.fit(df_clean).transform(df_clean)

    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    df_words = tokenizer.transform(df_indexed)

    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    df_filtered = remover.transform(df_words)

    hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=1000)
    featurized = hashingTF.transform(df_filtered)

    idf = IDF(inputCol="rawFeatures", outputCol="tfidfFeatures")
    idfModel = idf.fit(featurized)
    rescaledData = idfModel.transform(featurized)

    pca = PCA(k=50, inputCol="tfidfFeatures", outputCol="pcaFeatures")
    pcaModel = pca.fit(rescaledData)
    pcaData = pcaModel.transform(rescaledData)

    assembler = VectorAssembler(
        inputCols=["rating", "categoryIndex", "pcaFeatures"],
        outputCol="features"
    )
    featuredData = assembler.transform(pcaData)

    # Simpan Silver data ke Parquet dan CSV
    silver_path_parquet = os.path.join(output_path, "silver")
    silver_path_csv = os.path.join(output_path, "silver_csv")

    print(f"Saving Silver layer data to {silver_path_parquet} (Parquet)")
    featuredData.select("text", "rating", "category_clean", "product_name", "features") \
                .write.mode("overwrite").parquet(silver_path_parquet)

    print(f"Saving Silver layer data to {silver_path_csv} (CSV)")
    featuredData.select("text", "rating", "category_clean", "product_name") \
                .write.mode("overwrite").option("header", True).csv(silver_path_csv)

    # === Gold Layer: Clustering ===
    print("Running KMeans clustering...")
    kmeans = KMeans(k=5, featuresCol="features", predictionCol="cluster")
    model = kmeans.fit(featuredData)

    clustered = model.transform(featuredData)

    # Simpan Gold data ke Parquet dan CSV
    gold_path_parquet = os.path.join(output_path, "gold")
    gold_path_csv = os.path.join(output_path, "gold_csv")

    print(f"Saving Gold layer data to {gold_path_parquet} (Parquet)")
    clustered.select("text", "rating", "category_clean", "product_name", "cluster") \
             .write.mode("overwrite").parquet(gold_path_parquet)

    print(f"Saving Gold layer data to {gold_path_csv} (CSV)")
    clustered.select("text", "rating", "category_clean", "product_name", "cluster") \
             .write.mode("overwrite").option("header", True).csv(gold_path_csv)

    print("Pipeline finished successfully.")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pipeline_medallion.py <input_csv_path> <output_dir>")
        sys.exit(-1)
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)
