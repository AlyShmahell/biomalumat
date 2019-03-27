from profile import Profiler

def delayed_function():
    array = []
    for i in range(1,100000):
        array.append(i)
    return array

@Profiler
def decorated_function():
    a_list = []
    for i in range(1,100000):
        a_list.append(i)
    return a_list

if __name__ == '__main__':
    delayed_profiler = Profiler(delayed_function)
    print(delayed_profiler)
    result = delayed_profiler()
    print(delayed_profiler)
    print(f"execution result: {result[10]}")
    print(decorated_function)
    result = decorated_function()
    print(decorated_function)
    print(f"execution result: {result[10]}")
    print(decorated_function['profsile'])