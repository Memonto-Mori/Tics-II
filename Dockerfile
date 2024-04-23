FROM apache/airflow:2.3.0
RUN pip install markupsafe==2.0.1 \
		&& pip install apache-airflow-providers-odbc \
		&& pip install pyodbc \
		&& pip install apache-airflow-providers-microsoft-mssql \
		&& pip install apache-airflow-providers-microsoft-mssql[odbc] \
		&& pip install apache-airflow-providers-microsoft-azure \
		&& pip install gitpython \
		&& pip install --no-cache-dir pandas sqlalchemy psycopg2-binary
