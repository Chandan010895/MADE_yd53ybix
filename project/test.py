import os
import unittest

class TestDataPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Define paths
        cls.data_dir = 'data'
        cls.csv_path = os.path.join(cls.data_dir, 'jail_deaths.csv')
        cls.db_path = os.path.join(cls.data_dir, 'jail_deaths.db')
        cls.log_file = os.path.join(cls.data_dir, 'pipeline_log.txt')

        # Run the data pipeline
        os.system('python3 pipeline.py')

    def test_csv_file_creation(self):
        """Test if the CSV file is created."""
        self.assertTrue(os.path.isfile(self.csv_path), "CSV file not found. Test failed.")

    def test_db_file_creation(self):
        """Test if the SQLite database file is created."""
        self.assertTrue(os.path.isfile(self.db_path), "SQLite database file not found. Test failed.")

    def test_log_file_creation(self):
        """Test if the log file is created."""
        self.assertTrue(os.path.isfile(self.log_file), "Log file not found. Test failed.")

if __name__ == '__main__':
    unittest.main()