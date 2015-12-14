from charms.reactive import when, when_not, set_state, remove_state
from charms.hadoop import get_hadoop_base
from jujubigdata.handlers import HDFS
from jujubigdata import utils
from charmhelpers.core import hookenv, unitdata


@when('hadoop.installed')
@when_not('namenode.started')
def configure_namenode():
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.configure_namenode()
    hdfs.format_namenode()
    hdfs.start_namenode()
    hdfs.create_hdfs_dirs()
    hadoop.open_ports('namenode')
    set_state('namenode.started')


@when('namenode.started')
@when_not('datanode.connected')
def blocked():
    hookenv.status_set('blocked', 'Waiting for relation to DataNodes')


@when('namenode.started', 'datanode.connected')
def send_info(datanode):
    hadoop = get_hadoop_base()
    hdfs_port = hadoop.dist_config.port('namenode')
    webhdfs_port = hadoop.dist_config.port('nn_webapp_http')
    datanode.send_spec(hadoop.spec())
    datanode.send_ports(hdfs_port, webhdfs_port)
    datanode.send_ssh_key(utils.get_ssh_key('ubuntu'))


@when('namenode.started', 'datanode.connected')
@when_not('datanode.available')
def waiting(datanode):
    hookenv.status_set('waiting', 'Waiting for DataNodes')


@when('namenode.started', 'datanode.available')
def configure_datanodes(datanode):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    nodes = datanode.nodes()

    slaves = [node['hostname'] for node in nodes]
    unitdata.kv().set('namenode.slaves', slaves)
    hdfs.register_slaves(slaves)

    utils.update_kv_hosts({node['ip']: node['hostname'] for node in nodes})
    utils.manage_etc_hosts()

    hookenv.status_set('active', 'Ready ({count} DataNode{s})'.format(
        count=len(slaves),
        s='s' if len(slaves) > 1 else '',
    ))
    set_state('namenode.ready')


@when('namenode.started', 'datanode.leaving')
def unregister_datanode(datanode):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    nodes_leaving = datanode.nodes()  # only returns nodes in "leaving" state

    slaves = unitdata.kv().get('namenode.slaves')
    slaves_leaving = [node['hostname'] for node in nodes_leaving]
    hookenv.log('Slaves leaving: {}'.format(slaves_leaving))

    slaves_remaining = list(set(slaves) ^ set(slaves_leaving))
    unitdata.kv().set('namenode.slaves', slaves_remaining)
    hdfs.register_slaves(slaves_remaining)

    utils.remove_kv_hosts({node['ip']: node['hostname'] for node in nodes_leaving})
    utils.manage_etc_hosts()

    if not slaves_remaining:
        hookenv.status_set('blocked', 'Waiting for relation to DataNodes')
        remove_state('namenode.ready')


@when('namenode.ready', 'hub.available')
def register_hdfs(hub):
    hadoop = get_hadoop_base()
    hub.register_service(name='hdfs', data={
        'hdfs_port': hadoop.dist_config.port('namenode'),
        'webhdfs_port': hadoop.dist_config.port('nn_webapp_http'),
        'hadoop_version': hadoop.dist_config.hadoop_version,
    })


@when('hub.available')
@when_not('namenode.ready')
def unregister_hdfs(hub):
    hub.unregister_service(name='hdfs')
