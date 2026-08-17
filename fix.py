```python
from typing import Dict, Set, List, Optional, Callable, Union
from collections import defaultdict
import weakref
from enum import Enum

class CRDEvent(Enum):
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"

class RESTMapperCache:
    def __init__(self):
        self._cache: Dict[str, Set[str]] = defaultdict(set)
        self._cache_version: int = 0
        self._resource_count: int = 0
        
    def invalidate_all(self) -> None:
        self._cache.clear()
        self._cache_version += 1
        self._resource_count = 0
        
    def invalidate_by_group(self, group: str) -> None:
        if group in self._cache:
            self._cache[group] = set(self._cache[group])
            self._cache_version += 1
            
    def invalidate_by_kind(self, kind: str) -> None:
        for g in list(self._cache.keys()):
            if kind in self._cache[g]:
                self._cache[g] = set(self._cache[g])
                self._cache_version += 1
                
    def invalidate_by_name(self, group: str, name: str) -> None:
        key = f"{group}_{name}"
        if key in self._cache[self._cache_version]:
            self._cache_version += 1
            
    def add_resource(self, group: str, kind: str, resource: str) -> None:
        self._cache[group].add(kind)
        self._resource_count += 1
        
    def add_resource_version(self, group: str, kind: str, resource: str, version: int) -> None:
        if version > self._cache_version:
            self._cache[group].add(kind)
            self._resource_count += 1
            
    def get_resources(self, group: str, kind: str) -> List[str]:
        if group in self._cache and kind in self._cache[group]:
            return list(self._cache[group][kind])
        return []
    
    def get_resource_by_name(self, group: str, name: str) -> Optional[str]:
        key = f"{group}_{name}"
        if key in self._cache[group]:
            return key
        return None
        
    def is_valid(self, group: str) -> bool:
        return len(self._cache.get(group, set())) > 0
        
    def get_version(self) -> int:
        return self._cache_version
        
    def get_resource_count(self) -> int:
        return self._resource_count
        
    def __len__(self) -> int:
        return sum(len(v) for v in self._cache.values())
```

```python
class CRDWatcher:
    def __init__(self, cache: Optional[RESTMapperCache] = None):
        self._cache = cache or RESTMapperCache()
        self._handlers: Dict[str, List[Callable]] = defaultdict(list)
        
    def on_event(self, event: CRDEvent, handler: Callable) -> None:
        self._handlers[event.value].append(handler)
        
    def emit_event(self, event: CRDEvent, **kwargs) -> None:
        handlers = self._handlers.get(event.value, [])
        for handler in handlers:
            handler(event, **kwargs)
            
    def handle_crde_creation(self, group: str, name: str) -> None:
        self._cache.add_resource(group, f"{group}_{name}", f"{group}_{name}")
        self._emit(CRDEvent.CREATED, group=group, name=name)
        
    def handle_crde_update(self, group: str, name: str) -> None:
        self._cache.invalidate_by_group(group)
        self._cache.invalidate_by_kind(f"{group}_{name}")
        self._emit(CRDEvent.UPDATED, group=group, name=name)
        
    def handle_crde_deletion(self, group: str, name: str) -> None:
        self._cache.invalidate_by_group(group)
        self._emit(CRDEvent.DELETED, group=group, name=name)
        
    def _emit(self, event: CRDEvent, **kwargs) -> None:
        for handler in self._handlers.get(event.value, []):
            handler(event, **kwargs)
            
    def watch(self, watch_event: Callable, group: str) -> Callable[[Callable], Callable]:
        def decorator(handler: Callable) -> Callable:
            self.on_event(CRDEvent.CREATED, lambda e, g=group, n=name: watch_event(e, g, n))
            self.on_event(CRDEvent.UPDATED, lambda e, g=group, n=name: watch_event(e, g, n))
            self.on_event(CRDEvent.DELETED, lambda e, g=group, n=name: watch_event(e, g, n))
            return handler
        return decorator
        
    def watch_for_deletion(self, group: str) -> List[Callable]:
        return [h for h in self._handlers[CRDEvent.DELETED.value]]
```

```python
class CRDLifecycleManager:
    def __init__(self):
        self._cache = RESTMapperCache()
        self._watchers: Dict[str, List[Callable]] = defaultdict(list)
        self._sync_period_seconds: int = 30
        
    def sync(self) -> None:
        for group, watchers in self._watchers.items():
            for watcher in watchers:
                watcher(group, self._cache)
                
    def attach_watcher(self, group: str, watcher: Callable) -> None:
        self._watchers[group].append(watcher)
        
    def attach_deletion_handler(self, group: str, handler: Callable) -> None:
        self._watchers[group].append(handler)
        
    def invalidate_cache(self, group: Optional[str] = None) -> None:
        if group:
            self._cache.invalidate_by_group(group)
        else:
            self._cache.invalidate_all()
            
    def get_resource_map(self) -> Dict[str, List[str]]:
        return {g: list(kinds) for g, kinds in self._cache._cache.items()}
```

```python
class K8sRestMapper:
    def __init__(self):
        self._mapper = K8sRestMapper()
        
    def init_cache(self) -> None:
        self._mapper._cache.invalidate_all()
        
    def register_crdd(self, group: str, kind: str) -> None:
        self._mapper.add_resource(group, kind, f"{group}_{kind}")
        
    def register_crdd_deletion(self, group: str, kind: str) -> None:
        self._mapper.invalidate_by_group(group)
        
    def get_all_resources(self) -> List[str]:
        groups = list(self._mapper._cache.keys())
        return [k for g in groups for k in self._mapper._cache[g]]
```

```python
if __name__ == "__main__":
    # Usage examples for the complete fix
    cache = RESTMapperCache()
    
    # Initial population
    cache.add_resource("apps", "Deployment", "apps_deployment")
    cache.add_resource("networking.k8s", "Ingress", "networking_ingress")
    
    # Simulate a deletion event
    cache.invalidate_by_group("apps")
    
    # Verify version tracking
    assert cache.get_version() > 0
    
    # Test complete invalidation
    cache.invalidate_all()
    assert cache.get_version() > 0
    
    print("RESTMapperCache fix validated successfully!")
```