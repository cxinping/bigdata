# -*- coding: UTF-8 -*-

import types


def add_recursive(cur_item, cur_compute_result=1):
    if cur_item == 1:
        yield cur_compute_result

    yield add_recursive(cur_item - 1, cur_item + cur_compute_result)


def add_recursive_wapper(generator, item):
    gen = generator(item)
    while isinstance(gen, types.GeneratorType):
        gen = gen.__next__()

    return gen


print(add_recursive_wapper(add_recursive, 10000))


