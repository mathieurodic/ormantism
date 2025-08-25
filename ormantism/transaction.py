import json
import threading
import logging
from contextlib import contextmanager
from .connection import _get_connection


logger = logging.getLogger(__name__)


class TransactionError(Exception):
    """Custom exception for transaction-related errors"""
    pass


class TransactionManager:

    def __init__(self, connection_factory: callable):
        """
        Initialize the transaction manager.
        
        Args:
            connection_factory: A callable that returns a database connection
        """
        self._connection_factory = connection_factory
        self._local = threading.local()
    
    # get connection (built on first call)

    def _get_connection(self):
        """Get or create a connection for the current thread"""
        if not hasattr(self._local, 'connection'):
            self._local.connection = self._connection_factory()
        return self._local.connection
    
    # transaction level

    def _get_transaction_level(self):
        """Get current transaction nesting level"""
        return getattr(self._local, 'transaction_level', 0)
    
    def _set_transaction_level(self, level):
        """Set current transaction nesting level"""
        self._local.transaction_level = level
    
    def _increment_transaction_level(self):
        """Increment transaction nesting level"""
        level = self._get_transaction_level()
        self._set_transaction_level(level + 1)
        return level + 1
    
    def _decrement_transaction_level(self):
        """Decrement transaction nesting level"""
        level = self._get_transaction_level()
        new_level = max(0, level - 1)
        self._set_transaction_level(new_level)
        return new_level
    
    # actual transaction itself

    @contextmanager
    def transaction(self):
        """
        Context manager for database transactions with SAVEPOINT support.
        
        Yields:
            Transaction: Transaction object for executing statements
        """
        connection = self._get_connection()
        
        # Increment nesting level
        new_level = self._increment_transaction_level()
        
        # Create savepoint name for nested transactions
        savepoint_name = f"savepoint_{new_level}" if new_level > 1 else None
        
        transaction_obj = Transaction(connection, self, new_level)
        
        try:
            # Create savepoint for nested transactions
            if savepoint_name:
                logger.debug(f"SAVEPOINT {savepoint_name}")
                connection.execute(f"SAVEPOINT {savepoint_name}")
            
            yield transaction_obj
            
            # Commit or release savepoint based on nesting level
            if savepoint_name:
                logger.debug(f"RELEASE SAVEPOINT {savepoint_name}")
                connection.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            else:
                # For top-level transaction, commit
                logger.debug("COMMIT")
                connection.commit()
            
        except Exception as e:
            # Rollback to savepoint for nested transactions
            if savepoint_name:
                logger.debug(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            else:
                # For top-level transaction, full rollback
                connection.rollback()
            raise
        finally:
            # Decrement nesting level
            self._decrement_transaction_level()


class Transaction:

    def __init__(self, connection, manager, level):
        self._connection = connection
        self._manager = manager
        self._level = level
        self._active = True
    
    def execute(self, sql, parameters=()):
        """
        Execute a statement within this transaction.
        
        Args:
            query: SQL query to execute
            *args: Query parameters
            **kwargs: Additional keyword arguments
            
        Returns:
            Query result
            
        Raises:
            TransactionError: If trying to use a higher-level transaction
        """
        if not self._active:
            raise TransactionError("Transaction is no longer active")
        
        # format parameters
        for p, parameter in enumerate(parameters):
            if isinstance(parameter, (dict, list)):
                parameters[p] = json.dumps(parameter)
        logger.debug(sql)
        logger.debug(parameters)

        # Check if we're trying to use a higher-level transaction
        current_level = self._manager._get_transaction_level()
        if current_level > self._level:
            raise TransactionError(
                f"Cannot use transaction level {self._level} from level {current_level}. "
                "Higher-level transactions cannot be accessed from nested transactions."
            )
        return self._connection.execute(sql, parameters)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._active = False


_transaction_managers: dict[str, TransactionManager] = {}

def transaction(connection_name: str="default"):
    if connection_name not in _transaction_managers:
        def connection_factory_builder(name):
            return lambda : _get_connection(name=name)
        connection_factory = connection_factory_builder(connection_name)
        _transaction_managers[connection_name] = TransactionManager(connection_factory=connection_factory)
    return _transaction_managers[connection_name].transaction()
