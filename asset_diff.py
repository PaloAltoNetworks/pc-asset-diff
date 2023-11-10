import json
import datetime
import hashlib
import csv
from pcpi import session_loader
session_managers = session_loader.load_config('creds.json')
session = session_managers[0].create_cspm_session()

def extract_asset_details(asset):
    cloud_type = asset.get('cloudType')
    asset_id = asset.get('id')
    asset_name = asset.get('name')
    region_name = asset.get('regionName')
    asset_csv = [cloud_type,region_name,asset_id,asset_name]
    asset_hash = hashlib.md5(f'{cloud_type}{region_name}{asset_id}{asset_name}'.encode()).hexdigest()
    # asset_id = asset_id

    return asset_id, asset_csv, asset_hash
    # return asset_id, asset_csv

def get_assets(target_time):
    assets_csv_dict = {}
    asset_ids_list = []

    asset_hashes = []

    payload = {
        "filters": [
            {
                "name": "scan.status",
                "operator": "=",
                "value": "all"
            }
        ],
        "timeRange": {
            "type": "absolute",
            "value": {
                "startTime": target_time,
                "endTime": target_time
            }
        },
        "limit": 10000
    }

    #Paginate Assets
    while True:
        res = session.request('POST','resource/scan_info', json=payload)
        if res.json()['resources']:
            for asset in res.json()['resources']:
                asset_id, asset_csv, asset_hash = extract_asset_details(asset)
                # asset_id, asset_csv = extract_asset_details(asset)

                if asset_hash not in asset_hashes:
                    assets_csv_dict[asset_id] = asset_csv
                    asset_ids_list.append(asset_id)
                    asset_hashes.append(asset_hash)

        if 'nextPageToken' in res.json():
            nextPageToken = res.json()['nextPageToken']
            payload['pageToken'] = nextPageToken
        else:
            break

    print()
    print('Done gathering time range: ', end='')
    print(datetime.datetime.utcfromtimestamp(int(target_time)/1000).strftime('%Y-%m-%d:%H:%M:%S'))
    print()

    return assets_csv_dict, asset_ids_list


def generate_summary(time_1, time_2):
    #Summary section
    payload = {
        "groupBy":[
            "cloud.type"
        ],
        "filters":[],
        "timeRange":{
            "type":"absolute",
            "value":{
                "startTime":time_1,
                "endTime":  time_1
            }
        }
    }
    res = session.request('POST','v2/inventory', json=payload)
    total_resources_1 = res.json()['summary']['totalResources']


    payload = {
        "groupBy":[
            "cloud.type"
        ],
        "filters":[],
        "timeRange":{
            "type":"absolute",
            "value":{
                "startTime":time_2,
                "endTime":  time_2
            }
        }
    }
    res = session.request('POST','v2/inventory', json=payload)
    total_resources_2 = res.json()['summary']['totalResources']

    print()
    print(datetime.datetime.utcfromtimestamp(int(time_1)/1000).strftime('%Y-%m-%d:%H:%M:%S'),total_resources_1)
    print(datetime.datetime.utcfromtimestamp(int(time_2)/1000).strftime('%Y-%m-%d:%H:%M:%S'),total_resources_2)
    print("Difference:", total_resources_2-total_resources_1)


if __name__ == '__main__':
    conf_data = {}
    with open('conf.json', 'r') as infile:
        conf_data = json.load(infile)

    earlier_time = conf_data['earlier_time']
    later_time = conf_data['later_time']

    earlier_asset_csv_dict, earlier_asset_ids_list = get_assets(earlier_time)
    later_asset_csv_dict, later_asset_ids_list = get_assets(later_time) 


    new_asset_count = 0
    deleted_asset_count = 0
    print('Processing New Assets')
    #CSV of new assets
    with open('new_assets.csv','w') as outfile:
        with open('unique_later_assets.csv', 'w') as outfile_2:
            csv_writer = csv.writer(outfile)
            csv_writer_2 = csv.writer(outfile_2)
            #write headers
            outfile.write('cloud_type,region_name,asset_id,asset_name\n')
            outfile_2.write('cloud_type,region_name,asset_id,asset_name\n')
            count = 0
            
            for later_asset_id in later_asset_ids_list:
                count += 1
                total = len(later_asset_ids_list)
                print( round((count/total)*100, 2),'%', end='\r')

                csv_writer_2.writerow(later_asset_csv_dict[later_asset_id])

                #If the asset from the later dates is not in the earlier dates, its a new asset
                if later_asset_id not in earlier_asset_ids_list:
                    new_asset_count += 1
                    csv_writer.writerow(later_asset_csv_dict[later_asset_id])

    print('Done Processing New Assets')
    print()
    print('Processing Deleted Assets')

    #CSV of deleted assets
    with open('deleted_assets.csv','w') as outfile:
        with open('unique_earlier_assets.csv', 'w') as outfile_2:
            csv_writer = csv.writer(outfile)
            csv_writer_2 = csv.writer(outfile_2)
            #write headers
            outfile.write('cloud_type,region_name,asset_id,asset_name\n')
            outfile_2.write('cloud_type,region_name,asset_id,asset_name\n')
            count = 0
            for earlier_asset_id in earlier_asset_ids_list:
                count += 1
                total = len(earlier_asset_ids_list)
                print( round((count/total)*100, 2),'%', end='\r')

                csv_writer_2.writerow(earlier_asset_csv_dict[earlier_asset_id])

                #If the asset from the later dates is not in the earlier dates, its a new asset
                if earlier_asset_id not in later_asset_ids_list:
                    deleted_asset_count += 1
                    csv_writer.writerow(earlier_asset_csv_dict[earlier_asset_id])
    
    print('Done Processing Deleted Assets')

    print()

    print('New Assets:',new_asset_count)
    print('Deleted Assets:', deleted_asset_count)


    # generate_summary(earlier_time, later_time)
