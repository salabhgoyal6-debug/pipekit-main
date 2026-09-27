from datetime import datetime
import time 

TASK_REGISTRY = {}

class Task:
    def __init__(self, func, retries=0, timeout=None):
        self.func = func
        self.name = func.__name__
        self.retries = retries
        self.timeout = timeout
        self.dependencies = [] 
        TASK_REGISTRY[self.name] = self
        
    def __call__(self, *args, **kwargs):
        attempts = 0
        while True:
            start = time.time()
            try:
                result = self.func(*args, **kwargs)
                elapsed = time.time() - start
                return result
            except Exception as e:
                attempts += 1
                if attempts > self.retries:
                    raise
                wait = 2 ** attempts
                time.sleep(wait)
                    
    def __repr__(self):
        # this control how the object prints
        return f"Task(name={self.name}, retries={self.retries})"
    
    def depends_on(self, *tasks):
        for t in tasks:
            self.dependencies.append(t)
        return self 
    
def task(func=None, *, retries=0, timeout=None):
    
    if func is not None:
        return Task(func)
    else: 
        def wrapper(f):
            return Task(f, retries=retries, timeout=timeout)
        return wrapper
    