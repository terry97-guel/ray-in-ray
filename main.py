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
#Answer is YES!!