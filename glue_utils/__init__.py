"""
Glue utility functions for Third Opinion ETL jobs.
"""

from .table_utils import check_and_log_table_schema, check_all_tables

__all__ = ['check_and_log_table_schema', 'check_all_tables']
