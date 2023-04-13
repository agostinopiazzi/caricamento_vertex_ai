FROM europe-west4-docker.pkg.dev/cloud-dataproc/spark/dataproc_2.0:latest

# Change to root temporarily so that it has permissions to create dirs and copy
# files.
USER root

# Add a BigQuery connector jar.
#ENV SPARK_EXTRA_JARS_DIR=/opt/spark/jars/
#ENV SPARK_EXTRA_CLASSPATH='/opt/spark/jars/*'
#RUN mkdir -p "${SPARK_EXTRA_JARS_DIR}" \
#    && chown spark:spark "${SPARK_EXTRA_JARS_DIR}"
#COPY --chown=spark:spark \
#    spark-bigquery-with-dependencies_2.12-0.22.2.jar "${SPARK_EXTRA_JARS_DIR}"

# Install Cloud Storage client Conda package.
#RUN "${CONDA_HOME}/bin/conda" install google-cloud-storage

# Add a custom Python file.
ENV PYTHONPATH=/opt/python/packages
RUN mkdir -p "${PYTHONPATH}"
COPY test_util.py "${PYTHONPATH}"

# Add an init script.
#COPY --chown=spark:spark init-script.sh /opt/init-script.sh

# (Optional) Set user back to `spark`.
USER spark