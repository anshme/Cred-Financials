from geo_map import GEO_Map

if __name__ == '__main__':
    geo = GEO_Map()

    source_lat = geo.get_lat('10504')
    source_long = geo.get_lat('10504')

    destination_lat = geo.get_lat('10517')
    destination_long = geo.get_long('10517')

    distance = geo.distance(source_lat, source_long, destination_lat, destination_long)
    print(distance)
