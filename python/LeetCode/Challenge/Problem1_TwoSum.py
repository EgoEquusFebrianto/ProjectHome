data = [1, 2, 3, 3, 4, 5]
target = 6

map = dict()
res = None

for i, v in enumerate(data):
    map[v] = i

for i, v in enumerate(data):
    complement = target - v
    if map.get(complement):
        res = [map[complement], i]
        break

if res:
    print(f"Result = {res}.")
else:
    print("Result not found.")