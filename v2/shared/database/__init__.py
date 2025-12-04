"""
Shared database operations for v2 ETL jobs
"""
from .redshift_operations import write_to_redshift_simple, write_to_redshift_versioned

__all__ = [
    'write_to_redshift_simple',
    'write_to_redshift_versioned'
]

