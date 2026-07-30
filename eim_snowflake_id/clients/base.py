import logging
import os
from threading import RLock

logger = logging.getLogger(__name__)


class ThreadSafeSingletonMetaclass(type):
    """
    This is a thread-safe implementation of Singleton.
    Is used to prevent race conditions in environments like apache beam or other multithreaded environments where
    class instance is being created from non main thread.

    Source: https://refactoring.guru/design-patterns/singleton/python/example#example-1
    """

    instance = None
    instance_pid = os.getpid()

    _lock: RLock = RLock()
    """
    We now have a lock object that will be used to synchronize threads during
    first access to the Singleton.
    """

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        # Now, imagine that the program has just been launched. Since there's no
        # Singleton instance yet, multiple threads can simultaneously pass the
        # previous conditional and reach this point almost at the same time. The
        # first of them will acquire lock and will proceed further, while the
        # rest will wait here.
        with cls._lock:
            # The first thread to acquire the lock, reaches this conditional,
            # goes inside and creates the Singleton instance. Once it leaves the
            # lock block, a thread that might have been waiting for the lock
            # release may then enter this section. But since the Singleton field
            # is already initialized, the thread won't create a new object.
            if not cls.instance:
                cls.instance = super().__call__(*args, **kwargs)
            elif cls.instance_pid != os.getpid():
                logger.debug(f"{cls.__name__} instance belongs to a different process. Initializing a new instance.")
                cls.instance = None
                cls.instance_pid = None
                cls.instance = super().__call__(*args, **kwargs)
        return cls.instance


class BaseSingleton(metaclass=ThreadSafeSingletonMetaclass):
    @classmethod
    def _bootstrap_instance_args(cls) -> tuple:
        """Override in subclasses to provide args/kwargs for automatic re-initialization"""
        return (), {}

    @classmethod
    def get_instance(cls):
        """Gets the singleton instance of the class, ensuring it is created or reused properly.

        This method ensures that the singleton instance is initialized only once per process. If the
        process ID changes, the existing
        instance will be discarded and a new instance will be created and initialized.

        Returns:
            cls.instance: The singleton instance of the client.

        Raises:
            RuntimeError: If the instance is not initialized or cannot be accessed.
        """
        with cls._lock:
            if cls.instance is None:
                raise RuntimeError(f"{cls.__name__} instance is not initialized")
            elif cls.instance_pid != os.getpid():
                args, kwargs = cls.instance._bootstrap_instance_args()
                cls.close()

                # Create a new instance and initialize it
                cls.instance = cls.__new__(cls, *args, **kwargs)
                cls.instance.__init__(*args, **kwargs)  # Ensure __init__ is called
            return cls.instance

    @classmethod
    def close(cls):
        """Closes allocated resources and removes singleton instance state"""
        with cls._lock:
            if cls.instance:
                try:
                    cls.instance._disconnect()
                except NotImplementedError:
                    raise
                except Exception as error:
                    logger.warning(f"Cannot close connection for {cls=}: {error}")
                cls.instance = None
                cls.instance_pid = None

    def __init_subclass__(cls):
        if not getattr(cls, "_disconnect", None):
            raise RuntimeError(
                f"{cls=} doesn't have required method. Inherited class must implement "
                "_disconnect method to free the allocated resources"
            )
