import csv
import json
from pymongo import MongoClient
import sys


def get_connection(cn_dict):
    mongo_uri = "mongodb://{user}:{passwd}@{hostsandports}/{database}"
    if cn_dict.get('authSource'):
        mongo_uri += '?authSource={authSource}'
    mongo_uri = mongo_uri.format(**cn_dict)
    mongoclient = MongoClient(mongo_uri)
    return mongoclient


def single_ingest(database, from_topic, to_topic):
    if database.topics.find({'topic_name': from_topic}).count() != 1:
        raise ValueError("The topic name: {} not found in database".format(from_topic))

    from_id = database.topics.find({'topic_name': from_topic})[0]['_id']

    if database.topics.find({'topic_name': to_topic}).count() > 1:
        raise ValueError("The to topic {} is mapped to more than one id".format(to_topic))
    elif database.topics.find({'topic_name': to_topic}).count() == 0:
        # Cool all we need to do is change the topic name in the database
        result = database.topics.replace_one({'topic_name': from_topic}, {'topic_name': to_topic})
        print('from {} Changed: {} records'.format(from_topic, result.modified_count))
    else:
        # Now we do a query and map all of the from_id onto the to_id
        to_id = database.topics.find({'topic_name': to_topic})[0]['_id']

        results = database.data.update_many({'topic_id': from_id}, {'$set': {'topic_id': to_id}})
        print('from {} Changed: {} records'.format(from_topic, results.modified_count))
        result = database.topics.delete_one({"_id": from_id})
        print('Removed from topic {}'.format(result.deleted_count))


def replace_topic_data(cn_dict, replace_file):
    cn = get_connection(cn_dict)
    with open(replace_file, 'r', newline='') as fin:
        reader = csv.reader(fin, delimiter=',', quotechar='"')
        for row in reader:
            try:
                single_ingest(cn.get_default_database(), row[0], row[1])
            except ValueError as e:
                print(e.args[0])
    cn.close()


if __name__ == '__main__':
    import json
    db_config_file = sys.argv[1]
    with open(db_config_file, newline="") as fp:
        db_config = json.load(fp)

    topic_replace_file = sys.argv[2]
    replace_topic_data(db_config, topic_replace_file)

