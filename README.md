### Local dev

```bash
git clone …
bash .docker/run_local.sh
```

```bash
docker run --rm -it \
  --network docker_default \
  -e MC_HOST_local="http://minio:minio123@minio:9000" \
  minio/mc ls -r local
```

```bash
docker-compose -f docker/docker-compose.yml exec spark bash -c '       
cat > /workspace/query_ok.py <<PY                                
from pyspark.sql import SparkSession                               
  
spark = (                           
    SparkSession.builder            
      .appName("query-ok")   
      .config("spark.sql.catalog.local",  "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")                       
      .config("spark.sql.catalog.local.warehouse", "s3a://lakehouse/dev")
      .config("spark.sql.catalog.default", "local")                   
      .config("spark.hadoop.fs.s3a.endpoint",        "http://minio:9000")
      .config("spark.hadoop.fs.s3a.access.key",      "minio")  
      .config("spark.hadoop.fs.s3a.secret.key",      "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access","true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
      .getOrCreate()                                             
)                                                                         

spark.sql("SHOW TABLES IN local.dev_public").show()
spark.read.table("local.dev_public.sample_submission_librarytype").show(5)
spark.stop()
PY

spark-submit \
  --master local[*] \
  --jars /workspace/jars/iceberg-spark-runtime-3.4_2.12-1.5.0.jar \
  /workspace/query_ok.py
'
```

### Deploy

1. Zip repo & upload to S3
2. Create Glue Spark jobs (bulk_offload, retain_cleanup)
3. Pass `--env=prod --config=s3://bucket/conf/offload.yaml`
