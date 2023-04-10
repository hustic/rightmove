from google.cloud import bigquery


class DataAccess:
    def __init__(self, project_id: str, dataset_id: str):
        self.client = bigquery.Client()
        self.dataset = f"{project_id}.{dataset_id}"

    def get_dataframe(self, table_id: str):
        """Get a dataframe from a table"""
        QUERY = f"SELECT * FROM `{self.dataset}.{table_id}`"
        query_job = self.client.query(QUERY)  # API request
        rows = query_job.result()  # Waits for query to finish
        df = rows.to_dataframe()
        return df

    def update_item(self, table_id: str, property_id: str, key: str, value: str):
        """Update a single item in the database"""
        query = f"""
        UPDATE `{self.dataset}.{table_id}`
        SET {key} = {value}

        WHERE property_id = '{property_id}'
        """
        try:
            query_job = self.client.query(query)
            query_job.result()
        except Exception as e:
            print(e)
            return False
        return True
