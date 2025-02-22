import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import csv
import io
import time
# Define the expected field names (header order)
FIELDNAMES = [
    "parent_asin", "rating", "title", "text", "images", "asin", "user_id", 
    "timestamp", "helpful_vote", "verified_purchase", "Category", "seller_id", 
    "sentiment_label", "sentiment_score"
]

class CleanCSV(beam.DoFn):
    FIELDNAMES = [
    "parent_asin", "rating", "title", "text", "images", "asin", "user_id", 
    "timestamp", "helpful_vote", "verified_purchase", "Category", "seller_id", 
    "sentiment_label", "sentiment_score"
]
    def process(self, element):
        # Use csv.DictReader to parse the CSV line with known headers.
        import csv
        import io
        reader = csv.DictReader(io.StringIO(element), fieldnames=FIELDNAMES)
        for row in reader:
            # Clean and convert fields:
            try:
                row['rating'] = float(row['rating'])
            except:
                row['rating'] = None

            row['title'] = row['title'].strip()
            row['text'] = row['text'].strip()
            row['images'] = row['images'].strip()  # Could be processed further if needed
            row['asin'] = row['asin'].strip()
            row['user_id'] = row['user_id'].strip()
            try:
                # Convert timestamp which might be in scientific notation to an integer
                row['timestamp'] = int(float(row['timestamp']))
            except:
                row['timestamp'] = None

            try:
                row['helpful_vote'] = int(row['helpful_vote'])
            except:
                row['helpful_vote'] = 0

            # Convert verified_purchase to boolean (assumes values "TRUE" or "FALSE")
            row['verified_purchase'] = True if row['verified_purchase'].strip().upper() == "TRUE" else False

            row['Category'] = row['Category'].strip()
            row['seller_id'] = row['seller_id'].strip()
            row['sentiment_label'] = row['sentiment_label'].strip().lower()
            try:
                row['sentiment_score'] = float(row['sentiment_score'])
            except:
                row['sentiment_score'] = None

            # Yield the cleaned dictionary
            yield row
import time
# Define the pipeline options
pipeline_options = PipelineOptions(
    runner="DataflowRunner",
    project="spheric-engine-451615-a8",
    temp_location="gs://ai_chatbot_seller_central/Temp",
    region="us-east1",
    job_name="csv-cleaning-pipeline-"+ str(int(time.time()))
)

with beam.Pipeline(options=pipeline_options) as p:
    (p
     | "Read CSV from GCS" >> beam.io.ReadFromText("gs://ai_chatbot_seller_central/new_data_sentiment.csv", skip_header_lines=1)
     | "Clean CSV Data" >> beam.ParDo(CleanCSV())
     | "Write to BigQuery" >> WriteToBigQuery(
            table="spheric-engine-451615-a8:mlops_dataset.cleaned_data",
            schema=(
                "parent_asin:STRING, "
                "rating:FLOAT, "
                "title:STRING, "
                "text:STRING, "
                "images:STRING, "
                "asin:STRING, "
                "user_id:STRING, "
                "timestamp:INTEGER, "
                "helpful_vote:INTEGER, "
                "verified_purchase:BOOLEAN, "
                "Category:STRING, "
                "seller_id:STRING, "
                "sentiment_label:STRING, "
                "sentiment_score:FLOAT"
            ),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
         )
    )

