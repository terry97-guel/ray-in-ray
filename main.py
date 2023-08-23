# %%
import ray


if not ray.is_initialized():
    ray.init()

@ray.remote
def add_one(x):
    return x+1


@ray.remote
class RayAddOneActor():
    def add_one(self, x:list):
        ray_worker = []

        for x_ in x:
            ray_worker.append(add_one.remote(x_))

        return ray.get(ray_worker)


ray_worker = [RayAddOneActor.remote() for _ in range(10)]

res = ray.get([ray_worker[i].add_one.remote(x = [1,2]) for i in range(10)])

ray.shutdown()


# %%

def add_one(x):
    return x+1


class RayAddOneActor():
    def add_one(self, x:list):
        res = []

        for x_ in x:
            res.append(add_one(x_))

        return res


ray_worker = [RayAddOneActor() for _ in range(10)]

res = []
for i in range(10):
    res.append(ray_worker[i].add_one(x = [1,2]))

res

# %%
import ray

def add_one(x):
    return x+1


@ray.remote
class RayAddOneActor():
    def add_one(self, x:list):
        res = []

        for x_ in x:
            res.append(add_one(x_))

        return res

ray_worker = [RayAddOneActor.remote() for _ in range(10)]

res = ray.get([ray_worker[i].add_one.remote(x = [1,2]) for i in range(10)])

# %%
import ray

def add_one(x):
    return x+1

def add_two(x):
    return x+2

@ray.remote
class RayAddOneActor():
    def add_one_(self, x):
        return add_one(x)
    
    def add_two_(self, x):
        return add_two(x)

ray_worker = [RayAddOneActor.remote() for _ in range(10)]

def add_one_and_two(ray_worker:RayAddOneActor, x):
    x = ray_worker.add_one_.remote(x)
    x = ray_worker.add_two_.remote(x)
    return x

ray.get(add_one_and_two(ray_worker[0], x=1))






# %%

import ray

@ray.remote
class internal_worker:
    def __init__(self, id):
        self.id = id
    
    def add_id(self, x):
        return x + self.id

@ray.remote
class worker:
    def __init__(self, id):
        self.internal_worker = [internal_worker.remote(id=i) for i in range(10)]
        self.id = id
    def add_id(self, x):
        x = x + self.id * 10
        x = ray.get([self.internal_worker[i].add_id.remote(x) for i in range(10)])
        return x


ray_worker = [worker.remote(id=i) for i in range(10)]
res = ray.get([ray_worker[i].add_id.remote(x=1) for i in range(10)])

# %%
#Answer is YES!!