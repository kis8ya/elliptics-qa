import openstack
import copy

session = openstack.Session()

flavors = {None: 0}
for f in session.get_flavors_list():
    flavors[f['name']] = f['ram']

def _get_flavor_name(flavor_id):
    flavor_list = session.get_flavors_list()
    for flavor in flavor_list:
        if flavor['id'] == flavor_id:
            return flavor['name']
    else:
        return None

def _satisfied(instance_name, flavor_name):
    instance_info = session.get_instance_info(instance_name)
    if instance_info is None:
        return False
    else:
        current_flavor_name = _get_flavor_name(instance_info['flavor']['id'])
        return flavors[current_flavor_name] >= flavors[flavor_name]

def create(instances_cfg):
    instances = []
    for instance_cfg in instances_cfg['servers']:
        instances_names = openstack.utils.get_instances_names_from_conf(instance_cfg)
        instances += instances_names
        for instance_name in instances_names:
            #TODO: temporary fix for rebuil bug
            if False and _satisfied(instance_name, instance_cfg["flavor_name"]):
                session.rebuild_instance(instance_name)
            else:
                icfg = copy.deepcopy(instance_cfg)
                icfg["name"] = instance_name
                icfg["max_count"] = icfg["min_count"] = 1
                icfg = {"servers": [icfg]}

                session.delete_instance(instance_name)
                session.create_instances(icfg, check=False)

    return openstack.utils.check_availability(session, instances)

def delete(instances_cfg):
    session.delete_instances(instances_cfg)

def _flavors_order(f):
    """ Ordering function for instance flavor
    (ordering by RAM)
    """
    return flavors[f]

def get_instances_cfg(instances_params, base_names):
    """ Prepares instances config for future usage
    """
    clients_conf = _get_cfg(base_names['client'],
                            instances_params["clients"]["flavor"],
                            instances_params["clients"]["count"],
                            instances_params["clients"]["image"])
    servers_conf = _get_cfg(base_names['server'],
                            instances_params["servers"]["flavor"],
                            instances_params["servers"]["count"],
                            instances_params["servers"]["image"])
    if servers_conf["max_count"] == 1:
        servers_conf["name"] += "-1"
    if clients_conf["max_count"] == 1:
        clients_conf["name"] += "-1"

    return {"servers": [clients_conf, servers_conf]}

def _get_cfg(name, flavor, count, image):
    return {
        "name": name,
        "image_name": image,
        "key_name": "",
        "flavor_name": flavor,
        "max_count": count,
        "min_count": count,
        "networks_label_list": [
            "SEARCHOPENSTACKVMNETS"
            ]
        }

