# -*- coding: utf-8 -*-

l1= [1,2,3,4,5]
l2 = [2,3]
l3 = [x for x in l1 if x not in l2]
print(l3)

travel_city_name = 'aaa '.strip()
travel_city_names = travel_city_name.split(' ')

print(travel_city_name, len(travel_city_names))