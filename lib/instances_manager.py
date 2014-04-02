import openstack

session = openstack.Session()

flavors = {None: 0}
for f in session.get_flavors_list():
    flavors[f['name']] = f['ram']

def create(instances_cfg):
#    session.create_instances(instances_cfg)
    session.rebuild_instances(instances_cfg)

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

