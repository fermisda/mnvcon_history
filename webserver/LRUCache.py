import threading

class DummyContext:
    
    def __enter__(self):
        return None
        
    def __exit__(self, exc_type, exc_value, traceback):
        return False

class   LRUCache:

    def __init__(self, capacity, thread_safe = False):
        self.ThreadSafe = thread_safe
        self.Capacity = capacity   
        if self.ThreadSafe:
            self.Lock = threading.RLock()
        else:
            self.Lock = DummyContext()
        self.init() 

    def updateHitRates(self, hit):
        x = 1.0 if hit else 0.0
        self.HitRateAvg10 = 0.01*x + 0.99*self.HitRateAvg10
        self.HitRateAvg100 = 0.001*x + 0.999*self.HitRateAvg100
        if hit:
            self.Hits += 1
        else:
            self.Misses += 1
    
    def __getitem__(self, key):
        with self.Lock:
            if self.Cache.has_key(key):
                x = self.Cache[key]
                self.Keys.remove(key)
                self.Keys.insert(0, key)
                self.updateHitRates(True)
                return x
            else:
                self.updateHitRates(False)
                return None
            
    def __setitem__(self, key, data):
        with self.Lock:
            try:    self.Keys.remove(key)
            except ValueError:  pass
            self.Keys.insert(0, key)
            self.Cache[key] = data
            while len(self.Cache) > self.Capacity:
                k = self.Keys.pop()
                del self.Cache[k]

    def clear(self):
        with self.Lock:
            self.HitRateAvg10 = 0.0    
            self.HitRateAvg100 = 0.0    
            self.Hits = 0L
            self.Misses = 0L
            self.Keys = []
            self.Cache = {}

    init = clear

def cache_wrap(get_method):
    def new_method(self, *args):
        cache = self.Cache
        out = None
        if cache:           out = cache[args]
        if out == None:     out = get_method(self, *args)
        if cache and out != None:
            cache[args] = out
        return out
    return new_method

def add_cache(obj, cache):
    obj.Cache = cache
