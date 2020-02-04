import requests
import json


def elastic_push(alert_table_df, elastic_index_name, write_mode='append', discovery_mode=False):
    # logger.info("Begin pushing to get elastic")
    # start_time = time()

    # logger.info('elastic_index_name : {}'.format(elastic_index_name))

    elastic_ip = '123.123.123.123'
    elastic_port = '8888'
    e_user = "USER"
    e_pass = "PASSWORD"

    if discovery_mode:
        alert_table_df.write.mode(write_mode).format("org.elasticsearch.spark.sql") \
            .option("es.resource", elastic_index_name) \
            .option("es.nodes", elastic_ip) \
            .option("es.port", elastic_port) \
            .option("es.net.http.auth.user", e_user) \
            .option("es.net.http.auth.pass", e_pass) \
            .option("es.nodes.discovery", "true") \
            .save()
    else:
        elastic_ip = "123.123.123.123,321.321.321.321"
        # TODO: not sure if this elastic_ip can be used for discovery mode
        alert_table_df.write.mode(write_mode).format("org.elasticsearch.spark.sql") \
            .option("es.resource", elastic_index_name) \
            .option("es.nodes", elastic_ip) \
            .option("es.port", elastic_port) \
            .option("es.net.http.auth.user", e_user) \
            .option("es.net.http.auth.pass", e_pass) \
            .option('es.nodes.wan.only', 'true') \
            .save()

    # logger.info("Time Taken to elastic_push : {}".format(time() - start_time))


def get_date_property(colname):
    """
    as per fomat below and more if you need
    E.g. "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
    There are KNOWN ISSUES in KQL (kibana query language)
    """
    return {colname: {
    "type":   "date",
    "format": "yyyy-MM-dd HH:mm:ss||epoch_millis"}}


def get_geo_property(colname):
    """
    geo_point = 'lat, lon'
    """
    return {colname: {"type": "geo_point"}}


def init_elastic_mapping(index_name, date_cols=[], geo_cols=[]):
    # fix params
    headers = {'Content-Type': 'application/json'}
    params = (('pretty', ''),)
    elastic_ip = '123.123.123.123'
    elastic_port = '8888'
    e_user = "USER"
    e_pass = "PASSWORD"

    properties = {}
    [properties.update(get_date_property(col)) for col in date_cols]
    [properties.update(get_geo_property(col)) for col in geo_cols]

    data = {
      "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 3
      },
      "mappings": {
        "properties": properties
      }
    }

    url = 'http://{elastic_ip}:{elastic_port}/{index_name}?pretty'.format(elastic_ip=elastic_ip, elastic_port=elastic_port, index_name=index_name)
    payload = json.dumps(data)

    session = requests.Session()
    session.auth = (e_user, e_pass)
    response = session.put(url, headers=headers, params=params, data=payload)
    return response


def init_elastic():
    date_cols = ['date_col_1', 'date_col_2',]
    geo_cols = ['geo_col_1']
    respond = init_elastic_mapping(module_name, date_cols=date_cols, geo_cols=geo_cols)
    return respond


if __name__ == '__main__':
    module_name = 'blah_blah_blah'
    init_elastic()
