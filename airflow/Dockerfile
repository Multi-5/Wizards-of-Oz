FROM apache/airflow:2.9.2

# Install Python requirements at image build time to avoid runtime upgrades that can
# break the base Airflow installation. Keep running as the airflow user.
COPY --chown=airflow:root requirements.txt /requirements.txt
USER airflow
# Install packages into the airflow user's local site-packages to avoid running pip as root
RUN pip install --user --no-cache-dir -r /requirements.txt