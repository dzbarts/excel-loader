FROM apache/airflow:2.9.1

# Устанавливаем рантайм-зависимости пакета на этапе сборки.
# Код приезжает через volume mount ./dags → /opt/airflow/dags,
# поэтому изменения в DAG-ах отражаются сразу без пересборки образа.
RUN pip install --no-cache-dir \
    "openpyxl>=3.1,<4.0" \
    "python-dateutil>=2.9,<3.0" \
    "tqdm>=4.66,<5.0"
