from pyspark.sql import SparkSession
import sys

def export_gold_to_csv(gold_parquet_path, output_csv_path):
    spark = SparkSession.builder.appName("ExportGoldToCSV").getOrCreate()

    # Baca data gold Parquet
    df_gold = spark.read.parquet(gold_parquet_path)

    # Simpan ke CSV dengan header
    df_gold.write.mode("overwrite").option("header", True).csv(output_csv_path)

    print(f"Hasil Gold layer berhasil diekspor ke CSV di {output_csv_path}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: export_gold_to_csv.py <gold_parquet_path> <output_csv_path>")
        sys.exit(-1)
    gold_parquet_path = sys.argv[1]
    output_csv_path = sys.argv[2]

    export_gold_to_csv(gold_parquet_path, output_csv_path)
