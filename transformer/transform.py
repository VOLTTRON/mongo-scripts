import csv
from pymongo import MongoClient
import re
import sys

topic_list = None

def get_connection(cn_dict):
    mongo_uri = "mongodb://{user}:{passwd}@{hostsandports}/{database}"
    if cn_dict.get('authSource'):
        mongo_uri += '?authSource={authSource}'
    mongo_uri = mongo_uri.format(**cn_dict)
    mongoclient = MongoClient(mongo_uri)
    return mongoclient


def print_topic_list(cn_dict):
    for topic_name in build_topic_list(cn_dict):
        print(topic_name)


def build_topic_list(cn_dict=None):
    global topic_list

    if cn_dict is None and topic_list is None:
        raise ValueError('Topic list not initialized.')

    if cn_dict is None:
        return topic_list

    cn = get_connection(cn_dict)
    cursor = cn.get_default_database().topics.find().sort("topic_name")
    data = []
    for row in cursor:
        data.append(row['topic_name'])
    cursor.close()
    cn.close()
    topic_list = data
    return topic_list


def build_topic_replace(regex_file):

    REPLACE = "%REPLACE%"
    topic_list = build_topic_list()
    replace_list = []
    with open(regex_file, 'r', newline='') as fin:
        for line in regex_file:
            reader = csv.reader(fin, delimiter=',', quotechar='"')
            try:
                for row in reader:
                    if reader.line_num == 1:
                        continue
                    matcher = re.compile(row[0])
                    for topic in topic_list:
                        matched = matcher.match(topic)
                        if matched:
                            matched_string = topic[matched.start(): matched.end()]
                            if row[1].startswith(REPLACE):
                                args = row[1][len(REPLACE)+1: -1].split(',')
                                replacement = (topic, topic.replace(args[0], args[1]))
                            else:
                                replacement = (topic, row[1].replace('%MATCH%', matched_string))
                            replace_list.append(replacement)
                    #print(row)
            except csv.Error as e:
                sys.exit('file {}, line {}: {}'.format(regex_file, reader.line_num, e))
    count_of_from = [row[0] for row in replace_list]
    set_of_from = set(count_of_from)
    if len(count_of_from) != len(set_of_from):
        raise ValueError("Overlapping set between different lines in input expressions.")
    return replace_list


def print_replace_topics(regex_file):
    for out1, out2 in build_topic_replace(regex_file):
        data = '"{}","{}"'.format(out1, out2)
        print(data)

if __name__ == '__main__':
    import json
    db_config_file = sys.argv[1]
    with open(db_config_file, newline="") as fp:
        db_config = json.load(fp)

    build_topic_list(db_config)
    print_replace_topics('reg-input.txt')


