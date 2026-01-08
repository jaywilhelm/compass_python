# from tinydb import TinyDB, Query
import pandas as pd
# from flatten_json import flatten
import pickle

# def flatten_dict(d, parent_key='', sep='_'):
#     items = []
#     for k, v in d.items():
#         new_key = parent_key + sep + k if parent_key else k
#         if isinstance(v, dict):
#             items.extend(flatten_dict(v, new_key, sep=sep).items())
#         else:
#             items.append((new_key, v))
#     return dict(items)

    # db = TinyDB('D4_July2025SB_FirstDay.json')
    # alldata = db.all()



dataset = []
file_path = 'March2025_SB_D4' # Replace with the actual path to your .pkl file
with open(f"{file_path}.pkl", 'rb') as f:
    df = pickle.load(f)
    df_sort = df.sort_values(by='vehicle_id')
    df_sort.to_csv(f"{file_path}.csv", index=False)

# for item in alldata:
    #print(item)
    # item = flatten_dict(item)
    # dataset.append(item)

# df = pd.DataFrame(dataset)

# for item in df_sort:
#     print(item['road_matched_point'])

    #{'vehicle_type': 2, 'timestamp': {'seconds': 1759883694, 'nanos': 524000000}, 
    # 'road_matched_point': {'lat': 39.6729127586207, 'lon': -82.63482931034483}, 
    # 'speed_kmh': 125.0999984741211, 'osm_way_id': 1055346482, 'vehicle_id': 'LD7D1hhoBXZJ7NtTC1GwTV+Bvr2sNflmO7H7iymtLTM=', 
    # 'trip_id': 'a6ced2a2-b832-4c30-b869-1ad94d0f5532', 'raw_point': {'lat': 39.672931724137925, 'lon': -82.63482655172413}, 
    # 'transport_type': 1, 'acceleration': {'x': 0.0041, 'y': 0.0245, 'z': 0.0}, 'gyro': {'roll': 0.0, 'pitch': 0.0, 'yaw': 0.0}, 
    # 'iri': 0.0, 'near_miss': {'timestamp': {'seconds': 0, 'nanos': 0}, 'type': 0}, 'bearing': 98.0, 'point_id': 'YzA4OWExNjgxMmJkNGUwM2E5YmM4YWNjZDRiNDhmZjM='}