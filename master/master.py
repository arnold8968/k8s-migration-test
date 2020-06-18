# kubernetes config part
from kubernetes import client, config, watch
import yaml
import pandas as pd
import os
import time
import random
config.load_incluster_config()
v1 = client.CoreV1Api()
master_log = open("/data/master.log","w")
distribution = open("/data/distribution.csv","w")
distribution.write("time,job,orginal_node,to_node\n")
# UDP connection part
from socket import *
master_log.write("before connect\n")
master_log.flush()
hosts = ''
port = 5678
addr = (hosts, port)
uss = socket(AF_INET, SOCK_DGRAM)
uss.bind(addr)
used_pod = []
used_mig2 = []

# read all templates data at begining
def read_templates():
    file_names = os.listdir('/data/templates/')
    dict = {}
    for file_name in file_names:
        yaml_dic = yaml.load(open("/data/templates/"+file_name, 'r'))
        name = yaml_dic["metadata"]["name"]
        dict[name] = yaml_dic
    return dict



# initial node information
def init_node_information():
    dic = {}
    record = {}
    node_list = v1.list_node().items
    for node in node_list:
        if "node-0" not in node.metadata.name:
            dic[node.metadata.name] = [0,0,0]
            record[node.metadata.name] = 0
    return dic,record


#This is a global variables
templates = read_templates()
#This is a global variable
node_info,record = init_node_information()
#This is a global variable
new_bal_pods = []
#this is a global variable
init_time = time.time()
used_pods = []


def get_new_templates(node,job,str = "-migrated",templates = templates):
    yaml_dic = templates[job]
    yaml_dic["spec"]["nodeName"] = node
    yaml_dic["metadata"]["name"] = job+str
    return yaml_dic,job+str

# read logs
def read_logs(job):
    data = pd.read_csv("/data/"+job+".csv")
    return data.values[-1][1]

# check whether a jobs is still running
def check_running(job):
    pod = v1.read_namespaced_pod(job,"default")
    if pod.status.phase == "Running":
        return True
    else:
        return False

# whether whether epochs shown in logs is greater than workers' request epochs
def check_save(job,epoch):
    return read_logs(job)>=epoch

# bind pod to a specified node
def scheduler(name, node, namespace='default'):
    """
    Bind a pod to a node
    :param name: pod name
    :param node: node name
    :param namespace: kubernetes namespace
    :return:
    """

    target = client.V1ObjectReference(kind = 'Node', api_version = 'v1', name = node)
    meta = client.V1ObjectMeta(name = name)
    body = client.V1Binding(target = target, metadata = meta)
    try:
        client.CoreV1Api().create_namespaced_binding(namespace=namespace, body=body)
    except :
        # PRINT SOMETHING or PASS
        pass

#check whether node empty
def nodeisempty(node):
    all_pods = v1.list_namespaced_pod("default").items
    for pod in all_pods:
        if pod.spec.node_name == node and pod.status.phase in ["Running","Pending"]:
            return False
    return True

def nodes_available():
    ready_nodes = []
    for n in v1.list_node().items:
        ready_nodes.append(n.metadata.name)
        for status in n.status.conditions:
            if status.type == 'Ready':
                if status.status == "False":
                    master_log.log.write("Reason:"+status.type + "\n")
                    ready_nodes.remove(n.metadata.name)
                    #logging.warning(n.metadata.name+" is not available")
                    break
            else:
                if status.status == "True":
                    master_log.write("Reason:" + status.type + "\n")
                    ready_nodes.remove(n.metadata.name)
                    break
    for node in ready_nodes:
        if "node-0" in node:
            ready_nodes.remove(node)
    return ready_nodes

def send_to_job(job,epoch):
    f = open("/data/"+job+".info","w")
    f.close()

# get node_pod length
def get_node_pod_length(node_dic = node_info):
    length_dic = {}
    for node in list(node_dic.keys()):
        length_dic[node] = 0
    pod_list = v1.list_namespaced_pod(namespace="default").items
    for pod in pod_list:
        for node in list(node_dic.keys()):
            if pod.spec.node_name == node and pod.status.phase == "Running":
                length_dic[node] += 1
    return length_dic


# Calculate scores
def calculate_scores(incoming_node_name,node_information = node_info):
    length_dic = get_node_pod_length(node_information)
    scores = {}
    master_log.write("For incoming node {}\n".format(incoming_node_name.split(".")[0]))
    for node in node_information:
        node_penalty = node_information[node][0]+node_information[node][1]+node_information[node][2]
        master_log.write("node {}, PJ {}, WJ {}, MJ {}\n".format(node.split(".")[0],node_information[node][0],
                                                           node_information[node][1],node_information[node][2]))
        #if node == incoming_node_name:
            #node_penalty -= 1
        job_weight_score = 2*node_information[node][0]+1.5*node_information[node][1]+node_information[node][2]
        master_log.write("node {} , weight without add job numer {}\n".format(node.split(".")[0],job_weight_score))
        if node == incoming_node_name:
            scores[node] = (1-0)*node_penalty*job_weight_score
        else:
            scores[node] = (node_penalty+1)*(job_weight_score+1)
        master_log.write("node {}, total score {}\n".format(node.split(".")[0],scores[node]))
    #scores[incoming_node_name] -= 1
    ranked_scores = sorted(scores.items(), key=lambda x: x[1], reverse=False)
    return ranked_scores,scores

def update(data):
    data = data.split("<")
    node_name,lp,lw,lm = data
    node_info[node_name] = [int(lp),int(lw),int(lm)]
    record[node_name] += int(lp)
    return node_name

def update_migrated(data):
    data = data.split("<")
    node_name, job,epoch,lp,lw, lm = data
    node_info[node_name] = [int(lp),int(lw), int(lm)]
    epoch = int(epoch)
    print(str(node_info))
    return node_name,job,epoch

def migration(incoming_node_name,job,epoch):

    #node_selector
    ready_node = nodes_available()
    if len(ready_node) == 0:
        master_log.write(str(node_info))
        master_log.write(job + ":" + "no nodes available\n")
        master_log.flush()
        return 0

    # rank nodes
    ranks,node_dic = calculate_scores(incoming_node_name)
    # choose best nodes
    best_scores = ranks[0][1]
    ready_node = nodes_available()
    candicate_nodes = {}
    for node in ready_node:
        if node_dic[node] == best_scores:
            candicate_nodes[node] = float(v1.read_node(node).status.allocatable["cpu"])
    if len(candicate_nodes.keys()) == 0:
        master_log.write(str(node_info))
        distribution.write("{},{},{},\n".format(str(time.time()),job,incoming_node_name))
        distribution.flush()
        master_log.write(job+":"+incoming_node_name.split(".")[0]+"is now the best node\n")
        master_log.flush()
        used_pods.append(job)
        return 1
    elif incoming_node_name in candicate_nodes.keys():
        distribution.write("{},{},{},\n".format(str(time.time()),job,incoming_node_name))
        distribution.flush()
        master_log.write(job + ":" + incoming_node_name.split(".")[0] + "is now the best node\n")
        master_log.flush()
        used_pods.append(job)
        return 1
    new_ranks = sorted(candicate_nodes.items(), key=lambda x: x[1])
    node = ranks[0][0]
    master_log.write(str(ranks))
    master_log.write(str(new_ranks))
    
	# translate 
    send_to_job(job, epoch)
    while True:
        if check_running(job) == False:
            return 1
        if os.path.exists("/data/"+job):
            break
        time.sleep(1)
    pod_manifest,new_job_name = get_new_templates(node,job)
    #master_log.write(str(pod_manifest)+new_job_name)
    """
    if job in new_bal_pods:
    """
    resp = v1.create_namespaced_pod(body=pod_manifest,namespace='default')
    res = v1.delete_namespaced_pod(job, "default")
    master_log.write("before migrate containers distribution is {}\n".format(str(get_node_pod_length)))
    master_log.write("{} has be migrated from {} to {}\n".format(job,incoming_node_name.split(".")[0],node.split(".")[0]))
    master_log.flush()
    distribution.write("{},{},{},{}\n".format(str(time.time()),job,incoming_node_name,node))
    distribution.flush()
    node_info[incoming_node_name][2] -= 1
    node_info[node][2] += 1
    #wait for new pod status is pending
    """
    while True:
        try:
            resp = v1.read_namespaced_pod(name=new_job_name,
                                                namespace='default')
        except :
            pass
        if resp.status.phase == 'Pending':
            break
        time.sleep(1)
    """
    time.sleep(1)
    #scheduling
    #scheduler(new_job_name,node)
    return 2

def get_candidates(node_dic = node_info,used = used_pods):
    in_nodes,out_nodes = [],[]
    for node in node_dic:
        if node_dic[node][0]+node_dic[node][1]+node_dic[node][2] == 0:
            in_nodes.append(node)
        if node_dic[node][0]+node_dic[node][1]+node_dic[node][2] >= 2:
            out_nodes.append(node)
    if len(in_nodes) == 0:
        return 1
    if len(out_nodes) == 0:
        return 1
    can  = {}
    for node in out_nodes:
        can[node] = []
    used.reverse()
    for pod in used:
        if pod not in used_mig2:
            if check_running(pod) == True:
                pod_api = v1.read_namespaced_pod(pod,"default")
                if pod_api.spec.node_name in out_nodes:
                    can[pod_api.spec.node_name].append(pod)
    if len(can.keys()) == 0:
        return 1
    return in_nodes,can

def get_epochs(job):
    df = pd.read_csv("/data/"+job+".csv")
    columns = df.columns.tolist()
    epoch = df[columns[1]].tolist()[-1]
    try:
        epoch = df[columns[1]].tolist()[-1]
        epoch = int(epoch)
    except :
        epoch = df[columns[1]].tolist()[-2]
        epoch = int(epoch)
    return epoch

def mig_2(job,incoming_node_name,node,epoch):
    send_to_job(job, epoch)
    while True:
        if check_running(job) == False:
            return 1
        if os.path.exists("/data/" + job):
            break
        time.sleep(1)
    pod_manifest, new_job_name = get_new_templates(node, job,"-migrated2")
    # master_log.write(str(pod_manifest)+new_job_name)
    resp = v1.create_namespaced_pod(body=pod_manifest, namespace='default')
    res = v1.delete_namespaced_pod(job, "default")
    used_mig2.append(job)
    master_log.write("before migrate containers distribution is {}\n".format(str(get_node_pod_length)))
    master_log.write(
        "{} has be migrated from {} to {}\n".format(job, incoming_node_name.split(".")[0], node.split(".")[0]))
    master_log.flush()
    distribution.write("{},{},{},{}\n".format(str(time.time()), job, incoming_node_name, node))
    distribution.flush()
    node_info[incoming_node_name][2] -= 1
    node_info[node][2] += 1
    return 0

def bal_action(in_nodes,can):
    for income_node in can:
        for job in can[income_node]:
            if len(in_nodes) == 0:
                break
            epoch = get_epochs(job)
            to_node = in_nodes[0]
            if nodeisempty(to_node) == True:
                if mig_2(job,income_node,to_node,epoch) == 0:
                    in_nodes.pop(0)
            else:
                in_nodes.pop(0)

def main():
    while True:
        nodes_len = len(nodes_available())
        candidate = []
        while nodes_len > 0:
            data, addc = uss.recvfrom(1024)
            if not data: continue
            data = data.decode()
            UorM,data = data.split(">")
            if UorM == "U":
                #master_log.write("Ulogic\n")
                node_name = update(data)
                nodes_len -= 1
                #uss.sendto("Update".encode(), addc)
                #master_log.write(str(node_info) + "\n")
                #master_log.flush()
                #master_log.write("containers now distribution is {}\n".format(str(get_node_pod_length())))
                #master_log.flush()
            else:
                master_log.write("Mlogic on time:{}\n".format(time.time()-init_time))
                incoming_node_name,job,epoch = update_migrated(data)
                candidate.append((incoming_node_name,job,epoch))
        if len(candidate) == 0:
            res = get_candidates()
            if res != 1:
                bal_action(res[0], res[1])
        else:
            for pair in candidate:
                result = migration(pair[0], pair[1], pair[2])
if __name__ == "__main__":
    main()